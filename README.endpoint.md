# The endpoint interface

Endpoint interface is a series of SQL commands that exporting query results from segments.

A C example client program using this interface: `src/test/examples/test_parallel_cursor.c`

## Run your query and export to endpoints

The user starts a transaction and declares a parallel cursor for a SELECT query, we introduce the "parallel" keyword here, to distinguish it from the "common" cursors.

```
gpadmin=# BEGIN;
BEGIN
gpadmin=# DECLARE c1 PARALLEL CURSOR FOR SELECT * FROM t1;
DECLARE PARALLEL CURSOR
```

After the parallel cursor is declared, the user could query its details from pg\_cursors. The "is\_parallel" column is to indicate if this cursor is a parallel one.

```
gpadmin=# SELECT * FROM pg_cursors;
 name |                    statement                     | is_holdable | is_binary | is_scrollable | is_parallel |         creation_time
------+--------------------------------------------------+-------------+-----------+---------------+-------------+-------------------------------
 c1   | DECLARE c1 PARALLEL CURSOR FOR SELECT * FROM t1; | f           | f         | f             | t           | 2018-12-26 15:14:21.873567+08
(1 row)
```

Once the parallel cursor is declared, the user could `EXECUTE` it and `RETRIEVE` result data from endpoints in parallel. Here we introduce a new term "endpoint", endpoint behaviors like a store, the client could retrieve results from it. The endpoints could be on master or segments, depending on the query of the parallel cursor.

Get the endpoints information. There are three common kinds of status for an endpoint:

- INIT: after `DECLARE PARALLEL CURSOR`
- READY: after `EXECUTE PARALLEL CURSOR`
- RETRIEVING: a client is retrieving data from endpoints

```
gpadmin=# SELECT * FROM gp_endpoints;
    token     | cursorname | sessionid | hostname | port  | dbid | userid | status
--------------+------------+-----------+----------+-------+------+--------+--------
 tk1466020311 | c1         |        65 | s0       | 40000 |    2 |     10 | INIT
 tk1466020311 | c1         |        65 | s0       | 40001 |    3 |     10 | INIT
 tk1466020311 | c1         |        65 | s0       | 40002 |    4 |     10 | INIT
(3 rows)
```

To start the query, user runs `EXECUTE PARALLEL CURSOR`, the QD which receives this command will start the query and send results to endpoints, it won’t return until all data have been retrieved from endpoints.

## Retrieve the query result from endpoints

Then the user could open another connection to check the status of endpoints, status would become "READY", which means the results are ready on endpoints for retrieving now.

```
gpadmin=# SELECT * FROM gp_endpoints;
    token     | cursorname | sessionid | hostname | port  | dbid | userid | status
--------------+------------+-----------+----------+-------+------+--------+--------
 tk1466020311 | c1         |        65 | s0       | 40000 |    2 |     10 | READY
 tk1466020311 | c1         |        65 | s0       | 40001 |    3 |     10 | READY
 tk1466020311 | c1         |        65 | s0       | 40002 |    4 |     10 | READY
(3 rows)
```

Now user could connect to each segment via "retrieve" mode to retrieve results. On retrieve mode, the only valid operation is `RETRIEVE`, which is similar to the `FETCH` operation.

```
$ PGOPTIONS='-c gp_session_role=retrieve' psql -p 40000 gpadmin
Password:
psql (9.4beta1)
Type "help" for help.

gpadmin=# RETRIEVE 100 FROM tk1466020311;
 c1
----
  4
(1 row)

gpadmin=# \q
$ PGOPTIONS='-c gp_session_role=retrieve' psql -p 40001 gpadmin
Password:
psql (9.4beta1)
Type "help" for help.

gpadmin=# RETRIEVE 100 FROM tk1466020311;
 c1
----
  2
  5
(2 rows)

gpadmin=# \q
$ PGOPTIONS='-c gp_session_role=retrieve' psql -p 40002 gpadmin
Password:
psql (9.4beta1)
Type "help" for help.

gpadmin=# RETRIEVE 100 FROM tk1466020311;
 c1
----
  1
  3
(2 rows)
```

`100` here is the number of tuples for a single retrieve operation, the user could use `ALL` keyword to retrieve all results.

After all data from endpoints are retrieved, the "EXECUTE PARALLEL CURSOR" command will return.
```
gpadmin=# EXECUTE PARALLEL CURSOR c1;
EXECUTE PARALLEL CURSOR
```

If the query of the parallel cursor is an aggregate function, like `sum()` or `count()`, there will be only one endpoint on QD since the result has been aggregated on QD.

```
gpadmin=# BEGIN;
BEGIN
gpadmin=# DECLARE c2 PARALLEL CURSOR FOR SELECT count(*) FROM t1;
DECLARE CURSOR
gpadmin=# EXECUTE PARALLEL CURSOR c2;
```
```
gpadmin=# SELECT * FROM gp_endpoints;
    token     | cursorname | sessionid | hostname | port  | dbid | userid | status
--------------+------------+-----------+----------+-------+------+--------+--------
 tk1048939729 | c2         |        65 | s0       | 15432 |    1 |     10 | INIT
(1 row)
```
```
$ PGOPTIONS='-c gp_session_role=retrieve' psql -p 5432 gpadmin
Password:
psql (9.4beta1)
Type "help" for help.

gpadmin=# RETRIEVE 100 FROM tk1048939729;
 count
-------
     5
(1 row)
```
