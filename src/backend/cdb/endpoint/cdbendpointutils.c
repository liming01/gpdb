/*
 * cdbendpointutils.c
 *
 * Utility functions for querying tokens and endpoints.
 *
 * Copyright (c) 2019 - Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbendpointutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cdb/cdbendpoint.h"
#include "cdbendpointinternal.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "utils/builtins.h"

#define GP_ENDPOINTS_INFO_ATTRNUM       9

/*
 * EndpointStatus, EndpointsInfo and EndpointsStatusInfo structures are used
 * in UDFs(gp_endpoints_info, gp_endpoints_status_info) that show endpoint and
 * token information.
 */
typedef struct
{
	char name[ENDPOINT_NAME_LEN];
	char cursor_name[NAMEDATALEN];
	int8 token[ENDPOINT_TOKEN_LEN];
	int dbid;
	enum AttachStatus attach_status;
	pid_t sender_pid;
	Oid user_id;
	int session_id;
} EndpointStatus;

typedef struct
{
	int curTokenIdx;              /* current index in shared token list. */
	CdbComponentDatabases *cdbs;
	int currIdx;                   /* current index of node (master + segment) id */
	EndpointStatus *status;
	int status_num;
} EndpointsInfo;

typedef struct
{
	int endpoints_num;            /* number of EndpointDesc in the list */
	int current_idx;              /* current index of EndpointDesc in the list */
} EndpointsStatusInfo;

/* Used in UDFs */
static char *status_enum_to_string(enum AttachStatus status);
static enum AttachStatus status_string_to_enum(char *status);
static char *endpoint_status_enum_to_string(EndpointStatus *ep_status);

struct EndpointControl EndpointCtl = {                   /* Endpoint ctrl */
	PRCER_NONE, {0}, -1
};

/*
 * Convert the string tk0123456789 to int 0123456789 and save it into
 * the given token pointer.
 */
void
ParseToken(int8 *token /*out*/, const char *tokenStr)
{
	static const char *fmt = "Invalid token \"%s\"";
	if (tokenStr[0] == 't' && tokenStr[1] == 'k' &&
			strlen(tokenStr) == ENDPOINT_TOKEN_STR_LEN)
	{
		PG_TRY();
		{
			hex_decode(tokenStr + 2, ENDPOINT_TOKEN_LEN * 2, (char*)token);
		}
		PG_CATCH(); {
			/* Create a general message on purpose for security concerns. */
			elog(ERROR, fmt, tokenStr);
		}
		PG_END_TRY();
	} else {
		elog(ERROR, fmt, tokenStr);
	}
}

/*
 * Generate a string tk0123456789 from int 0123456789
 *
 * Note: need to pfree() the result
 */
char *
PrintToken(const int8 *token)
{
	Insist(is_endpoint_token_valid(token));
	const size_t len = ENDPOINT_TOKEN_STR_LEN + 1; /* 2('tk') + HEX string length + 1('\0') */
	char *res = palloc(len);
	res[0] = 't';
	res[1] = 'k';
	hex_encode((const char*)token, ENDPOINT_TOKEN_LEN, res + 2);
	res[len - 1] = 0;

	return res;
}

/*
 * Set the role of endpoint, sender or receiver
 */
void
SetParallelCursorExecRole(enum ParallelRetrCursorExecRole role)
{
	if (EndpointCtl.Gp_prce_role != PRCER_NONE)
		elog(ERROR, "endpoint role %s is already set",
			 endpoint_role_to_string(EndpointCtl.Gp_prce_role));

	elog(DEBUG3, "CDB_ENDPOINT: set endpoint role to %s", endpoint_role_to_string(role));

	EndpointCtl.Gp_prce_role = role;
}

/*
 * Clear the role of endpoint
 */
void
ClearParallelCursorExecRole(void)
{
	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint role %s", endpoint_role_to_string(EndpointCtl.Gp_prce_role));

	EndpointCtl.Gp_prce_role = PRCER_NONE;
	memset(EndpointCtl.cursor_name, '\0', NAMEDATALEN);
}

/*
 * Return the value of static variable Gp_prce_role
 */
enum ParallelRetrCursorExecRole GetParallelCursorExecRole(void)
{
	return EndpointCtl.Gp_prce_role;
}

const char *
endpoint_role_to_string(enum ParallelRetrCursorExecRole role)
{
	switch (role)
	{
		case PRCER_SENDER:
			return "[END POINT SENDER]";

		case PRCER_RECEIVER:
			return "[END POINT RECEIVER]";

		case PRCER_NONE:
			return "[END POINT NONE]";

		default:
			elog(ERROR, "unknown end point role %d", role);
			return NULL;
	}
}

bool
is_endpoint_token_valid(const int8 *token) {
	Assert(token);
	for (int i = 0; i < ENDPOINT_TOKEN_LEN; ++i)
	{
		if (token[i]) return true;
	}
	return false;
}

void
invalidate_endpoint_name(char *endpointName /*out*/)
{
	Assert(endpointName);
	memset(endpointName, '\0', ENDPOINT_NAME_LEN);
}

/*
 * Returns true if the two given endpoint tokens are equal.
 */
bool
token_equals(const int8* token1, const int8* token2)
{
	Assert(token1);
	Assert(token2);
	/* memcmp should be good enough. Timing attack would not be a concern here. */
	return memcmp(token1, token2, ENDPOINT_TOKEN_LEN) == 0;
}

bool
endpoint_name_equals(const char *name1, const char *name2)
{
	return strncmp(name1, name2, ENDPOINT_NAME_LEN) == 0;
}

/*
 * Create a magic number from a given token for DSM TOC usage.
 * The same tokens will eventually generate the same magic number.
 */
uint64
create_magic_num_for_endpoint(const EndpointDesc *desc)
{
	uint64 magic = 0;
	Assert(desc);
	for (int i = 0; i < 8; i++)
	{
		magic |= ((uint64)desc->name[i]) << i * 8;
	}
	return magic;
}

/*
 * On QD, display all the endpoints information in shared memory
 */
Datum
gp_endpoints_info(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "gp_endpoints_info() only can be called on query dispatcher");

	bool allSessions = PG_GETARG_BOOL(0);
	FuncCallContext *funcctx;
	EndpointsInfo *mystatus;
	MemoryContext oldcontext;
	Datum values[GP_ENDPOINTS_INFO_ATTRNUM];
	bool nulls[GP_ENDPOINTS_INFO_ATTRNUM] = {true};
	HeapTuple tuple;
	int res_number = 0;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */


		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc tupdesc = CreateTemplateTupleDesc(GP_ENDPOINTS_INFO_ATTRNUM, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "token",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "cursorname",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sessionid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "hostname",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "port",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "dbid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "userid",
						   OIDOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "status",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "endpointname",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (EndpointsInfo *) palloc0(sizeof(EndpointsInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->curTokenIdx = 0;
		mystatus->cdbs = cdbcomponent_getCdbComponents();
		mystatus->currIdx= 0;
		mystatus->status = NULL;
		mystatus->status_num = 0;

		CdbPgResults cdb_pgresults = {NULL, 0};

		CdbDispatchCommand("SELECT endpointname,cursorname,token,dbid,status,senderpid,userid,sessionid FROM pg_catalog.gp_endpoints_status_info()",
						   DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR, &cdb_pgresults);

		if (cdb_pgresults.numResults == 0)
		{
			elog(ERROR, "gp_endpoints_info didn't get back any data from the segDBs");
		}
		for (int i = 0; i < cdb_pgresults.numResults; i++)
		{
			if (PQresultStatus(cdb_pgresults.pg_results[i]) != PGRES_TUPLES_OK)
			{
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				elog(ERROR, "gp_endpoints_info(): resultStatus is not tuples_Ok");
			}
			res_number += PQntuples(cdb_pgresults.pg_results[i]);
		}

		if (res_number > 0)
		{
			mystatus->status = (EndpointStatus *) palloc0(sizeof(EndpointStatus) * res_number);
			mystatus->status_num = res_number;
			int idx = 0;

			for (int i = 0; i < cdb_pgresults.numResults; i++)
			{
				struct pg_result *result = cdb_pgresults.pg_results[i];

				for (int j = 0; j < PQntuples(result); j++)
				{
					strncpy(mystatus->status[idx].name, PQgetvalue(result, j, 0), ENDPOINT_NAME_LEN);
					strncpy(mystatus->status[idx].cursor_name, PQgetvalue(result, j, 1), NAMEDATALEN);
					ParseToken(mystatus->status[idx].token, PQgetvalue(result, j, 2));
					mystatus->status[idx].dbid = atoi(PQgetvalue(result, j, 3));
					mystatus->status[idx].attach_status = status_string_to_enum(PQgetvalue(result, j, 4));
					mystatus->status[idx].sender_pid = atoi(PQgetvalue(result, j, 5));
					mystatus->status[idx].user_id = atoi(PQgetvalue(result, j, 6));
					mystatus->status[idx].session_id = atoi(PQgetvalue(result, j, 7));
					idx++;
				}
			}
		}

		/* get end-point status on master */
		LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
		int cnt = 0;

		for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
		{
			const EndpointDesc* entry = get_endpointdesc_by_index(i);

			if (!entry->empty)
				cnt++;
		}
		if (cnt != 0)
		{
			mystatus->status_num += cnt;
			if (mystatus->status)
			{
				mystatus->status = (EndpointStatus *) repalloc(mystatus->status,
															   sizeof(EndpointStatus) * mystatus->status_num);
			}
			else
			{
				mystatus->status = (EndpointStatus *) palloc(
					sizeof(EndpointStatus) * mystatus->status_num);
			}
			int idx = 0;

			for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
			{
				const EndpointDesc* entry = get_endpointdesc_by_index(i);

				if (!entry->empty)
				{
					EndpointStatus* status = &mystatus->status[mystatus->status_num - cnt + idx];
					strncpy(mystatus->status[idx].name, entry->name, ENDPOINT_NAME_LEN);
					strncpy(mystatus->status[idx].cursor_name, entry->cursor_name, NAMEDATALEN);
					memcpy(status->token, get_token_by_session_id(entry->session_id), ENDPOINT_TOKEN_LEN);
					status->dbid = contentid_get_dbid(MASTER_CONTENT_ID, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY, false);
					status->attach_status = entry->attach_status;
					status->sender_pid = entry->sender_pid;
					status->user_id = entry->user_id;
					status->session_id = entry->session_id;
					idx++;
				}
			}
		}
		LWLockRelease(ParallelCursorEndpointLock);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;

	while (mystatus->currIdx < mystatus->status_num)
	{
		Datum result;
		EndpointStatus *qe_status = &mystatus->status[mystatus->currIdx++];
		Assert(qe_status);

		if (!allSessions && qe_status->session_id != gp_session_id)
		{
			continue;
		}

		GpSegConfigEntry *segCnfInfo = dbid_get_dbinfo(qe_status->dbid);
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		char *token = PrintToken(qe_status->token);
		values[0]   = CStringGetTextDatum(token);
		pfree(token);
		nulls[0] = false;
		values[1] = CStringGetTextDatum(qe_status->cursor_name);
		nulls[1]  = false;
		values[2] = Int32GetDatum(qe_status->session_id);
		nulls[2]  = false;
		values[3] = CStringGetTextDatum(segCnfInfo->hostname);
		nulls[3]  = false;
		values[4] = Int32GetDatum(segCnfInfo->port);
		nulls[4]  = false;
		values[5] = Int32GetDatum(segCnfInfo->dbid);
		nulls[5]  = false;
		values[6] = ObjectIdGetDatum(qe_status->user_id);
		nulls[6]  = false;

		/*
		 * find out the status of end-point
		 */
		values[7] =
			CStringGetTextDatum(endpoint_status_enum_to_string(qe_status));
		nulls[7] = false;

		if (qe_status)
		{
			values[8] = CStringGetTextDatum(qe_status->name);
			nulls[8]  = false;
		}
		else
			nulls[8] = true;

		tuple  = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	SRF_RETURN_DONE(funcctx);
}

/*
 * Display the status of all valid EndpointDesc of current
 * backend in shared memory
 */
Datum
gp_endpoints_status_info(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	EndpointsStatusInfo *mystatus;
	MemoryContext oldcontext;
	Datum values[10];
	bool nulls[10] = {true};
	HeapTuple tuple;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc tupdesc = CreateTemplateTupleDesc(10, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "token",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "databaseid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "senderpid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "receiverpid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "dbid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "sessionid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "userid",
						   OIDOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "endpointname",
						   TEXTOID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "cursorname",
						   TEXTOID, -1, 0);


		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (EndpointsStatusInfo *) palloc0(sizeof(EndpointsStatusInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->endpoints_num = MAX_ENDPOINT_SIZE;
		mystatus->current_idx = 0;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	while (mystatus->current_idx < mystatus->endpoints_num)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum result;

		const EndpointDesc* entry = get_endpointdesc_by_index(mystatus->current_idx);

		if (!entry->empty && (superuser() || entry->user_id == GetUserId()))
		{
			char *status = NULL;
			char *token = PrintToken(get_token_by_session_id(entry->session_id));

			values[0] = CStringGetTextDatum(token);
			nulls[0] = false;
			values[1] = Int32GetDatum(entry->database_id);
			nulls[1] = false;
			values[2] = Int32GetDatum(entry->sender_pid);
			nulls[2] = false;
			values[3] = Int32GetDatum(entry->receiver_pid);
			nulls[3] = false;
			status = status_enum_to_string(entry->attach_status);
			values[4] = CStringGetTextDatum(status);
			nulls[4] = false;
			values[5] = Int32GetDatum(GpIdentity.dbid);
			nulls[5] = false;
			values[6] = Int32GetDatum(entry->session_id);
			nulls[6] = false;
			values[7] = ObjectIdGetDatum(entry->user_id);
			nulls[7] = false;
			values[8] = CStringGetTextDatum(entry->name);
			nulls[8] = false;
			values[9] = CStringGetTextDatum(entry->cursor_name);
			nulls[9] = false;
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			mystatus->current_idx++;
			LWLockRelease(ParallelCursorEndpointLock);
			pfree(token);
			SRF_RETURN_NEXT(funcctx, result);
		}
		mystatus->current_idx++;
	}
	LWLockRelease(ParallelCursorEndpointLock);
	SRF_RETURN_DONE(funcctx);
}

char *
status_enum_to_string(enum AttachStatus status)
{
	char *result = NULL;

	switch (status)
	{
		case Status_NotAttached:
			result = GP_ENDPOINT_STATUS_INIT;
			break;
		case Status_Prepared:
			result = GP_ENDPOINT_STATUS_READY;
			break;
		case Status_Attached:
			result = GP_ENDPOINT_STATUS_RETRIEVING;
			break;
		case Status_Finished:
			result = GP_ENDPOINT_STATUS_FINISH;
			break;
		default:
			elog(ERROR, "unknown end point status %d", status);
			break;
	}
	return result;
}

enum AttachStatus
status_string_to_enum(char *status)
{
	Assert(status);
	if (strcmp(status, GP_ENDPOINT_STATUS_INIT) == 0)
	{
		return Status_NotAttached;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_READY) == 0)
	{
		return Status_Prepared;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_RETRIEVING) == 0)
	{
		return Status_Attached;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_FINISH) == 0)
	{
		return Status_Finished;
	}
	else
	{
		elog(ERROR, "unknown end point status %s", status);
		return Status_NotAttached;
	}
}

char *
endpoint_status_enum_to_string(EndpointStatus *ep_status)
{
	if (ep_status != NULL)
	{
		return status_enum_to_string(ep_status->attach_status);
	} else
	{
		/* called on QD, if endpoint status is null, and token info is not release*/
		return GP_ENDPOINT_STATUS_RELEASED;
	}
}
