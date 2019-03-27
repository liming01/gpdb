-- ===================================================================
-- create FDW objects
-- ===================================================================

DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;
DROP DATABASE IF EXISTS contrib_regression2;
CREATE DATABASE contrib_regression2;

\c contrib_regression2
CREATE EXTENSION postgres_fdw;

CREATE SERVER testserver1 FOREIGN DATA WRAPPER postgres_fdw 
	OPTIONS (host 'localhost', dbname 'testdb', mpp_execute 'all segments');
 -- CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
 -- OPTIONS (dbname 'contrib_regression', mpp_execute 'all segments');

CREATE USER MAPPING FOR CURRENT_USER SERVER testserver1;
-- OPTIONS (user 'linw', password '');
-- CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

\c testdb;
-- ===================================================================
-- create objects used through FDW server
-- ===================================================================
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');

CREATE TABLE t1 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 user_enum,
	CONSTRAINT t1_pkey PRIMARY KEY (c1)
);

CREATE TABLE t2 (
	c1 int NOT NULL,
	c2 text,
	CONSTRAINT t2_pkey PRIMARY KEY (c1)
) DISTRIBUTED BY (c1);

INSERT INTO t1
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'::user_enum
	FROM generate_series(1, 1000) id;
INSERT INTO t2
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;


-- ===================================================================
-- create foreign tables
-- ===================================================================
\c contrib_regression2

CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');

CREATE FOREIGN TABLE ft1 (
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum
) SERVER testserver1 OPTIONS(schema_name 'public', table_name 't1');
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

CREATE FOREIGN TABLE ft2 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 user_enum
) SERVER testserver1 OPTIONS(schema_name 'public', table_name 't1');
ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

ALTER FOREIGN TABLE ft2 OPTIONS (schema_name 'public', table_name 't1');

CREATE FOREIGN TABLE _ft2 (
	c1 int NOT NULL,
	c2 text
) SERVER testserver1 OPTIONS(schema_name 'public', table_name 't2');

-- test directly dispatch
SELECT * FROM _ft2 WHERE c1 = 10;

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  

ALTER SERVER testserver1 OPTIONS (SET dbname 'no such database');

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail

ALTER SERVER testserver1 OPTIONS (SET dbname 'testdb');

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work

EXPLAIN (COSTS false) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- whole-row reference
EXPLAIN (VERBOSE, COSTS false) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
-- EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1'; -- should work, but failed, report as a bug 
-- SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
-- EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE; -- should work, but failed, report as a bug 
-- SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;   -- should work, but failed, report as a bug 
-- EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;    -- should work, but failed, report as a bug 
-- aggregate
SELECT COUNT(*) FROM ft1 t1;
-- join two tables
SELECT t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- subquery
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
-- SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1; -- should work, but failed, report as a bug 
-- used in CTE
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
-- SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1; -- should work, but failed, report as a bug 
-- user-defined operator/function
CREATE FUNCTION postgres_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==
);
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2;
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE 1 = c1!;           -- OpExpr(r)
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- ArrayRef
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
EXPLAIN (VERBOSE, COSTS false) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote

-- parameterized remote path
EXPLAIN (VERBOSE, COSTS false)
  SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;
-- SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2; -- should work, but failed, report as a bug 

EXPLAIN (VERBOSE, COSTS false)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);

-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
-- SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5)); -- should work, but failed, report as a bug
-- SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5)); -- should work, but failed, report as a bug

-- simple join
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
EXPLAIN (VERBOSE, COSTS false) EXECUTE st1(1, 2);
-- EXECUTE st1(1, 1);      -- should work, but failed, report as a bug
-- EXECUTE st1(101, 101);  -- should work, but failed, report as a bug

-- subquery using stable function (can't be sent to remote)
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS false) EXECUTE st2(10, 20);
EXECUTE st2(10, 20);
EXECUTE st2(101, 121);

-- subquery using immutable function (can be sent to remote)
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
EXPLAIN (VERBOSE, COSTS false) EXECUTE st3(10, 20);
EXECUTE st3(10, 20);
EXECUTE st3(20, 30);

-- custom plan should be chosen initially
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);

-- once we try it enough times, should switch to generic plan
EXPLAIN (VERBOSE, COSTS false) EXECUTE st4(1);

-- value of $1 should not be sent to remote
PREPARE st5(user_enum,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXPLAIN (VERBOSE, COSTS false) EXECUTE st5('foo', 1);
EXECUTE st5('foo', 1);

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;


-- System columns, except ctid, should not be sent to remote
EXPLAIN (VERBOSE, COSTS false)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1 WHERE c2 = 1;
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass LIMIT 1  WHERE c2 = 1;
EXPLAIN (VERBOSE, COSTS false)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1 WHERE c2 = 1;
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1 WHERE c2 = 1;
EXPLAIN (VERBOSE, COSTS false)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
EXPLAIN (VERBOSE, COSTS false)
SELECT ctid, * FROM ft1 t1 LIMIT 1 WHERE c2 = 1;
SELECT ctid, * FROM ft1 t1 LIMIT 1 WHERE c2 = 1;

-- ===================================================================
-- used in pl/pgsql function
-- ===================================================================
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
-- SELECT f_test(100); -- should work but failed, report as a bug
DROP FUNCTION f_test(int);

-- ===================================================================
-- conversion error
-- ===================================================================
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
SELECT * FROM ft1 WHERE c1 = 1;  -- ERROR
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE user_enum;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
FETCH c;
SAVEPOINT s;
-- SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR TODO: not an expected error, another command is already in progress
ROLLBACK TO s;
FETCH c;
-- SELECT * FROM ft1 ORDER BY c1 LIMIT 1;  -- should work but failed, report as a bug
COMMIT;
