#include "postgres.h"

#include "postgres_fdw.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbendpoint.h"

static void
get_session_id(PGconn *conn, int *session_id)
{
	PGresult   *res;

	res = pgfdw_exec_query(conn, "show gp_session_id");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pgfdw_report_error(ERROR, res, conn, true, "show gp_session_id");
	}

	*session_id = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
}

void
wait_endpoints_ready(ForeignServer *server,
					 UserMapping *user,
					 int32 token)
{
	StringInfoData buf;
	PGconn	   *conn;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT status FROM gp_endpoints WHERE token = %d", token);

	conn = ConnectPgServer(server, user);

	while (true)
	{
		bool		all_endpoints_ready = true;
		PGresult   *res;

		CHECK_FOR_INTERRUPTS();

		res = pgfdw_exec_query(conn, buf.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			pgfdw_report_error(ERROR, res, conn, true, buf.data);

		if (PQntuples(res) == 0)
			pgfdw_report_error(ERROR, res, conn, true, buf.data);

		for (int row = 0; row < PQntuples(res); ++row)
		{
			if (strcmp(PQgetvalue(res, row, 0), "READY") != 0)
			{
				all_endpoints_ready = false;
				break;
			}
		}

		PQclear(res);

		if (all_endpoints_ready)
			break;
	}

	PQfinish(conn);
}

void
get_endpoints_info(PGconn *conn,
				   int cursor_number,
				   int session_id,
				   List **fdw_private,
				   int32 *token)
{
	StringInfoData sql_buf;
	PGresult   *res;
	List	   *endpoints_list;
	char	   *foreign_username = NULL;

	initStringInfo(&sql_buf);
	appendStringInfo(&sql_buf,
					 "SELECT hostname, port, dbid, token, pg_get_userbyid(userid) FROM gp_endpoints "
					 "WHERE sessionid=%d AND cursorname = 'c%d'",
					 session_id, cursor_number);

	*token = InvalidToken;
	endpoints_list = NIL;

	res = pgfdw_exec_query(conn, sql_buf.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		pgfdw_report_error(ERROR, res, conn, true, sql_buf.data);

	if (PQntuples(res) == 0)
		pgfdw_report_error(ERROR, res, conn, true, sql_buf.data);

	for (int row = 0; row < PQntuples(res); ++row)
	{
		char	   *host;
		char	   *port;
		char	   *dbid;
		List	   *endpoint = NIL;

		if (PQnfields(res) != 5)
			pgfdw_report_error(ERROR, res, conn, true, sql_buf.data);

		host = pstrdup(PQgetvalue(res, row, 0));
		port = pstrdup(PQgetvalue(res, row, 1));
		dbid = pstrdup(PQgetvalue(res, row, 2));
		endpoint = list_make3(makeString(host), makeString(port), makeString(dbid));

		endpoints_list = lappend(endpoints_list, endpoint);

		if (*token == InvalidToken)
			*token = atoi(PQgetvalue(res, row, 3));
		else if (*token != atoi(PQgetvalue(res, row, 3)))
			pgfdw_report_error(ERROR, res, conn, true, sql_buf.data);

		if (row == 0)
			foreign_username = pstrdup(PQgetvalue(res, row, 4));
	}

	/* The order should be same as enum FdwScanPrivateIndex definition */
	*fdw_private = lappend(*fdw_private, endpoints_list);
	*fdw_private = lappend(*fdw_private, list_make1_int(*token));
	*fdw_private = lappend(*fdw_private, list_make1(makeString(foreign_username)));

	PQclear(res);
}

void
create_parallel_cursor(ForeignScanState *node)
{
	StringInfoData buf;
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	initStringInfo(&buf);
	appendStringInfo(&buf, "DECLARE c%u PARALLEL CURSOR FOR\n%s",
					 fsstate->cursor_number, fsstate->query);

	create_cursor_helper(node, buf.data);
}

void
execute_parallel_cursor(ForeignScanState *node)
{
	StringInfoData buf;
	PGconn	   *conn;
	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	conn = fsstate->conn;

	initStringInfo(&buf);
	appendStringInfo(&buf, "EXECUTE PARALLEL CURSOR c%u", fsstate->cursor_number);

	/* We don't want to block main thread, so we don't use PQexec for results. */
	if (!PQsendQuery(conn, buf.data))
		pgfdw_report_error(ERROR, NULL, conn, false, buf.data);
}

void
create_and_execute_parallel_cursor(ForeignScanState *node)
{
	int			session_id;
	int32		token;
	ForeignScan *foreign_scan;

	PgFdwScanState *fsstate = (PgFdwScanState *) node->fdw_state;

	foreign_scan = (ForeignScan *) node->ss.ps.plan;

	/* get session id for fetching endpoints info of parallel cursor */
	get_session_id(fsstate->conn, &session_id);
	create_parallel_cursor(node);
	get_endpoints_info(fsstate->conn, fsstate->cursor_number, session_id,
					   &(foreign_scan->fdw_private), &token);
	execute_parallel_cursor(node);
	fsstate->token = token;
}
