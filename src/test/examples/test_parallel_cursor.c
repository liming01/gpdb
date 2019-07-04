/*
 * src/test/examples/test_parallel_cursor.c
 *
 *
 * test_parallel_cursor.c
 *		this program only support gpdb with the parallel cursor feature. It shows how to
 * use LIBPQ to make a connect to gpdb master node and create & execute a parallel cursor,
 * and how to make multiple retrieve mode connections to all endpoints of the parallel cursor
 * (i.e. gpdb all segment nodes in this sample) and parallelly retrieve the results of these
 * endpoints.
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "libpq-fe.h"

static void
finish_conn_nicely(PGconn *master_conn, PGconn *endpoint_conns[], size_t endpoint_conns_num)
{
	if (master_conn)
		PQfinish(master_conn);

	for (int i = 0; i < endpoint_conns_num; i++)
	{
		if (endpoint_conns[i])
			PQfinish(endpoint_conns[i]);
	}

	free(endpoint_conns);
	endpoint_conns_num = 0;
}

static void
check_prepare_conn(PGconn *conn, const char *dbName)
{
	PGresult   *res;

	/* check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database \"%s\" failed: %s",
				dbName, PQerrorMessage(conn));
		exit(1);
	}

	/* Set always-secure search path, so malicous users can't take control. */
	res = PQexec(conn,
				 "SELECT pg_catalog.set_config('search_path', '', false)");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SET failed: %s", PQerrorMessage(conn));
		PQclear(res);
		exit(1);
	}
	PQclear(res);
}
static int
exec_sql_without_resultset(PGconn *conn, const char *sql)
{
	PGresult   *res1;

	printf("\nExec SQL: %s\n", sql);
	res1 = PQexec(conn, sql);
	if (PQresultStatus(res1) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "execute sql failed: \"%s\"\nfailed %s", sql, PQerrorMessage(conn));
		PQclear(res1);
		return 1;
	}

	/*
	 * make sure to PQclear() a PGresult whenever it is no longer needed to
	 * avoid memory leaks
	 */
	PQclear(res1);
	return 0;
}
static int
exec_sql_with_resultset(PGconn *conn, const char *sql)
{
	PGresult   *res1;
	int			nFields;
	int			i;

	printf("\nExec SQL: %s\n", sql);
	res1 = PQexec(conn, sql);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Query didn't return tuples properly: \"%s\"\nfailed %s", sql, PQerrorMessage(conn));
		PQclear(res1);
		return 1;
	}

	/* first, print out the attribute names */
	nFields = PQnfields(res1);
	for (i = 0; i < nFields; i++)
		printf("%-15s", PQfname(res1, i));
	printf("\n---------\n");

	/* next, print out the instances */
	for (i = 0; i < PQntuples(res1); i++)
	{
		for (int j = 0; j < nFields; j++)
			printf("%-15s", PQgetvalue(res1, i, j));
		printf("\n");
	}

	PQclear(res1);
	return 0;
}

/* this function is run by the second thread*/
static void *
exec_parallel_cursor_threadfunc(void *master_conn)
{
	PGconn	   *conn = (PGconn *) master_conn;

	/* execute parallel cursor and it will wait for finish retrieving. */
	if (exec_sql_without_resultset(conn, "EXECUTE PARALLEL CURSOR myportal;") != 0)
		exit(1);
	return NULL;
}
int
main(int argc, char **argv)
{
	char	   *pghost,
			   *pgport,
			   *pgoptions,
			   *pgoptions_retrieve_mode,
			   *pgtty;
	char	   *dbName;
	int			i;

	PGconn	   *master_conn,
			  **endpoint_conns = NULL;
	size_t		endpoint_conns_num = 0;
	char	   *token = NULL;

	/*
	 * PGresult   *res1, *res2;
	 */
	PGresult   *res1;

	if (argc != 2)
	{
		fprintf(stderr, "usage: %s dbName\n", argv[0]);
		fprintf(stderr, "      show how to use parallel cursor to parallelly retrieve data from multiple endpoints.\n");
		exit(1);
	}
	dbName = argv[1];

	/*
	 * begin, by setting the parameters for a backend connection if the
	 * parameters are null, then the system will try to use reasonable
	 * defaults by looking up environment variables or, failing that, using
	 * hardwired constants
	 */
	pghost = NULL;				/* host name of the backend */
	pgport = NULL;				/* port of the backend */
	pgoptions = NULL;			/* special options to start up the backend
								 * server */
	pgoptions_retrieve_mode = "-c gp_session_role=retrieve";	/* specify this
																 * connection is in the
																 * retrieve mode */
	pgtty = NULL;				/* debugging tty for the backend */

	/* make a connection to the database */
	master_conn = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName);
	check_prepare_conn(master_conn, dbName);

	/* do some preparation for test */
	if (exec_sql_without_resultset(master_conn, "DROP TABLE IF EXISTS public.tab_parallel_cursor;") != 0)
		goto LABEL_ERR;
	if (exec_sql_without_resultset(master_conn, "CREATE TABLE public.tab_parallel_cursor AS SELECT id FROM pg_catalog.generate_series(1,100) id;") != 0)
		goto LABEL_ERR;

	/*
	 * start a transaction block because parallel cursor only support WITHOUT
	 * HOLD option
	 */
	if (exec_sql_without_resultset(master_conn, "BEGIN;") != 0)
		goto LABEL_ERR;

	/* declare parallel cursor for this table */
	if (exec_sql_without_resultset(master_conn, "DECLARE myportal PARALLEL CURSOR FOR select * from public.tab_parallel_cursor;") != 0)
		goto LABEL_ERR;

	/*
	 * get the endpoints info of this parallel cursor
	 */
	const char *sql1 = "select hostname,port,token,status from pg_catalog.gp_endpoints where cursorname='myportal';";

	printf("\nExec SQL: %s\n", sql1);
	res1 = PQexec(master_conn, sql1);
	if (PQresultStatus(res1) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "select gp_endpoints view didn't return tuples properly\n");
		PQclear(res1);
		goto LABEL_ERR;
	}
	/* firstly check that the endpoint info rows > 0 */
	int			ntup = PQntuples(res1);

	if (ntup <= 0)
	{
		fprintf(stderr, "select gp_endpoints view doesn't return rows\n");
		goto LABEL_ERR;
	}

	endpoint_conns = malloc(ntup * sizeof(PGconn *));
	endpoint_conns_num = ntup;

	/*
	 * create retrieve mode connection to endpoints according to the endpoints
	 * info fetched
	 */
	for (i = 0; i < ntup; i++)
	{
		if (i == 0)
		{
			/*
			 * currently all endpoints of one parallel cursor has the same
			 * token, so just set it at the first time
			 */
			token = strdup(PQgetvalue(res1, i, 2));
		}

		endpoint_conns[i] = PQsetdbLogin(PQgetvalue(res1, i, 0), PQgetvalue(res1, i, 1),
									  pgoptions_retrieve_mode, pgtty, dbName,
										 NULL, PQgetvalue(res1, i, 2));
		check_prepare_conn(endpoint_conns[i], dbName);
	}
	PQclear(res1);


	pthread_t	thread1;

	/*
	 * create a second thread to execute parallel cursor because it will
	 * waiting until all data finished retrieved
	 */
	if (pthread_create(&thread1, NULL, exec_parallel_cursor_threadfunc, master_conn))
	{
		fprintf(stderr, "Error creating thread of \"execute the parallel cursor\"\n");
		goto LABEL_ERR;
	}

	/*
	 * call it to suspend the main thread, so that the thread of "execute the
	 * parallel cursor" will run fistly
	 */
	usleep(1);

	/*
	 * Waiting for the status to becomes 'READY', and then retrieve the result
	 * of the endpoints This section can be execute parallely on different
	 * host or in different threads/processes on the same host. For
	 * simplicity, here just use loop in one process.
	 */
	for (i = 0; i < endpoint_conns_num; i++)
	{
		char		sql[256];

		printf("\n------ Begin retrieving data from Endpoint %d# ------\n", i);

LABEL_RETRY:
		/* check that this endpoint is ready to be retrieved. */
		snprintf(sql, sizeof(sql), "SELECT 1 FROM GP_ENDPOINTS_STATUS_INFO() WHERE token='%s' and status='READY';", token);
		printf("\nExec SQL: %s\n", sql);
		res1 = PQexec(endpoint_conns[i], sql);
		if (PQresultStatus(res1) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "select GP_ENDPOINTS_STATUS_INFO view didn't return tuples properly\n");
			PQclear(res1);
			goto LABEL_ERR;
		}
		/* retry until the result set rows > 0 */
		int			ntup = PQntuples(res1);

		if (ntup == 0)
		{
			fprintf(stderr, "select gp_endpoints view doesn't return rows\n");
			PQclear(res1);
			goto LABEL_RETRY;
		}

		snprintf(sql, sizeof(sql), "RETRIEVE ALL FROM %s", token);
		exec_sql_with_resultset(endpoint_conns[i], sql);
		printf("\n------ End retrieving data from Endpoint %d# ------.\n", i);
	}

	/* wait for the second thread to finish "execute parallel cursor" */
	if (pthread_join(thread1, NULL))
	{
		fprintf(stderr, "Error joining thread of \"execute the parallel cursor\"\n");
		goto LABEL_ERR;
	}
	/* close the cursor */
	if (exec_sql_without_resultset(master_conn, "CLOSE myportal;") != 0)
		goto LABEL_ERR;

	/* end the transaction */
	if (exec_sql_without_resultset(master_conn, "END;") != 0)
		goto LABEL_ERR;

	/* close the connections to the database and cleanup */
	finish_conn_nicely(master_conn, endpoint_conns, endpoint_conns_num);

/*	 fclose(debug); */
	return 0;

LABEL_ERR:
	finish_conn_nicely(master_conn, endpoint_conns, endpoint_conns_num);
	if (token)
		free(token);
	return 1;
}
