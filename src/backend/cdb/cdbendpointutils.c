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
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbdispatchresult.h"
#include "libpq-fe.h"
#include "utils/builtins.h"

#define GP_ENDPOINTS_INFO_ATTRNUM       8
#define BITS_PER_BITMAPWORD             32
#define WORDNUM(x)                      ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)                       ((x) % BITS_PER_BITMAPWORD)

/*
 * EndpointStatus, EndpointsInfo and EndpointsStatusInfo structures are used
 * in UDFs(gp_endpoints_info, gp_endpoints_status_info) that show endpoint and
 * token information.
 */
typedef struct
{
	int8 token[ENDPOINT_TOKEN_LEN];
	int dbid;
	enum AttachStatus attach_status;
	pid_t sender_pid;
} EndpointStatus;

typedef struct
{
	int curTokenIdx;              /* current index in shared token list. */
	GpSegConfigEntry *seg_db_list;
	int segment_num;              /* number of segments */
	int curSegIdx;                /* current index of segment id */
	EndpointStatus *status;
	int status_num;
} EndpointsInfo;

typedef struct
{
	int endpoints_num;            /* number of EndpointDesc in the list */
	int current_idx;              /* current index of EndpointDesc in the list */
} EndpointsStatusInfo;

/*
 * Used for bigmap
 */
static const uint8 rightmost_one_pos[256] = {
	0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
};

extern bool compare_token(const int8 *token1, const int8 *token2);
extern uint64 create_magic_num_from_token(const int8 *token);

static EndpointStatus *
find_endpoint_status(EndpointStatus *status_array, int number,
					 const int8 *token, int dbid)
{
	for (int i = 0; i < number; i++)
	{
		if (compare_token(status_array[i].token, token)
			&& status_array[i].dbid == dbid)
		{
			return &status_array[i];
		}
	}
	return NULL;
}

static char *status_enum_to_string(enum AttachStatus status)
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

static enum AttachStatus status_string_to_enum(char *status)
{
	Assert(status);
	if (strcmp(status, GP_ENDPOINT_STATUS_INIT) == 0)
	{
		return Status_NotAttached;
	} else if (strcmp(status, GP_ENDPOINT_STATUS_READY) == 0)
	{
		return Status_Prepared;
	} else if (strcmp(status, GP_ENDPOINT_STATUS_RETRIEVING) == 0)
	{
		return Status_Attached;
	} else if (strcmp(status, GP_ENDPOINT_STATUS_FINISH) == 0)
	{
		return Status_Finished;
	} else
	{
		elog(ERROR, "unknown end point status %s", status);
		return Status_NotAttached;
	}
}

static char *endpoint_status_enum_to_string(EndpointStatus *ep_status)
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

/*
 * Return true if this end-point exists on QD.
 */
bool endpoint_on_qd(ParaCursorToken token)
{
	return (token->endpoint_cnt == 1) && (dbid_has_token(token, MASTER_DBID));
}

/*
 * End-points with same token can exist in some or all segments.
 * This function is to determine if the end-point exists in the segment(dbid).
 */
bool dbid_has_token(ParaCursorToken token, int16 dbid)
{
	if (token->all_seg)
		return true;

	return dbid_in_bitmap(token->dbIds, dbid);
}

/*
 * If the dbid is in this bitmap.
 */
bool dbid_in_bitmap(int32 *bitmap, int16 dbid)
{
	if (dbid < 0 || dbid >= sizeof(int32) * 8 * MAX_NWORDS)
		elog(ERROR, "invalid dbid");
	if (bitmap == NULL)
		elog(ERROR, "invalid dbid bitmap");

	if ((bitmap[WORDNUM(dbid)] & ((uint32) 1 << BITNUM(dbid))) != 0)
		return true;
	return false;
}

/*
 * Add a dbid into bitmap.
 */
void add_dbid_into_bitmap(int32 *bitmap, int16 dbid)
{
	if (dbid < 0 || dbid >= sizeof(int32) * 8 * MAX_NWORDS)
		elog(ERROR, "invalid dbid");
	if (bitmap == NULL)
		elog(ERROR, "invalid dbid bitmap");

	bitmap[WORDNUM(dbid)] |= ((uint32) 1 << BITNUM(dbid));
}

/*
 * Get the next dbid from bitmap.
 *	The typical pattern is to iterate the dbid bitmap
 *
 *		x = -1;
 *		while ((x = get_next_dbid_from_bitmap(bitmap, x)) >= 0)
 *			process member x;
 *	This implementation is copied from bitmapset.c
 */
int get_next_dbid_from_bitmap(int32 *bitmap, int prevbit)
{
	int wordnum;
	uint32 mask;

	if (bitmap == NULL)
		elog(ERROR, "invalid dbid bitmap");

	prevbit++;
	mask = (~(uint32) 0) << BITNUM(prevbit);
	for (wordnum = WORDNUM(prevbit); wordnum < MAX_NWORDS; wordnum++)
	{
		uint32 w = bitmap[wordnum];

		/* ignore bits before prevbit */
		w &= mask;

		if (w != 0)
		{
			int result;

			result = wordnum * BITS_PER_BITMAPWORD;
			while ((w & 255) == 0)
			{
				w >>= 8;
				result += 8;
			}
			result += rightmost_one_pos[w & 255];
			return result;
		}

		/* in subsequent words, consider all bits */
		mask = (~(bitmapword) 0);
	}
	return -2;
}

EndpointDesc * find_endpoint_by_token(const int8 *token)
{
	EndpointDesc *res = NULL;

	LWLockAcquire(EndpointsLWLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!SharedEndpoints[i].empty &&
				compare_token(SharedEndpoints[i].token, token))
		{

			res = &SharedEndpoints[i];
			break;
		}
	}
	LWLockRelease(EndpointsLWLock);
	return res;
}

/*
 * On QD, display all the endpoints information in shared memory
 */
Datum
gp_endpoints_info(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "gp_endpoints_info() only can be called on query dispatcher");

	bool is_all = PG_GETARG_BOOL(0);
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

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (EndpointsInfo *) palloc0(sizeof(EndpointsInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->curTokenIdx = 0;
		mystatus->seg_db_list = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID)->cdbs->segment_db_info->config;
		mystatus->segment_num = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID)->cdbs->total_segment_dbs;
		mystatus->curSegIdx = 0;
		mystatus->status = NULL;
		mystatus->status_num = 0;

		CdbPgResults cdb_pgresults = {NULL, 0};

		CdbDispatchCommand("SELECT token,dbid,status,senderpid FROM pg_catalog.gp_endpoints_status_info()",
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
					ParseToken(mystatus->status[idx].token, PQgetvalue(result, j, 0));
					mystatus->status[idx].dbid = atoi(PQgetvalue(result, j, 1));
					mystatus->status[idx].attach_status = status_string_to_enum(PQgetvalue(result, j, 2));
					mystatus->status[idx].sender_pid = atoi(PQgetvalue(result, j, 3));
					idx++;
				}
			}
		}

		/* get end-point status on master */
		LWLockAcquire(EndpointsLWLock, LW_SHARED);
		int cnt = 0;

		for (int i = 0; i < MAX_ENDPOINT_SIZE && SharedEndpoints != NULL; i++)
		{
			Endpoint entry = &SharedEndpoints[i];

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
			} else
			{
				mystatus->status = (EndpointStatus *) palloc(
					sizeof(EndpointStatus) * mystatus->status_num);
			}
			int idx = 0;

			for (int i = 0; i < MAX_ENDPOINT_SIZE && SharedEndpoints != NULL; i++)
			{
				Endpoint entry = &SharedEndpoints[i];

				if (!entry->empty)
				{
					memcpy(mystatus->status[mystatus->status_num - cnt + idx].token,
							entry->token, ENDPOINT_TOKEN_LEN);
					mystatus->status[mystatus->status_num - cnt + idx].dbid = MASTER_DBID;
					mystatus->status[mystatus->status_num - cnt + idx].attach_status = entry->attach_status;
					mystatus->status[mystatus->status_num - cnt + idx].sender_pid = entry->sender_pid;
					idx++;
				}
			}
		}
		LWLockRelease(EndpointsLWLock);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;
	/*
	 * build detailed token information
	 */
	LWLockAcquire(TokensLWLock, LW_SHARED);
	while (mystatus->curTokenIdx < MAX_ENDPOINT_SIZE && SharedTokens != NULL)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum result;
		GpSegConfigEntry *dbinfo;

		ParaCursorToken entry = &SharedTokens[mystatus->curTokenIdx];

		if (IsEndpointTokenValid(entry->token)
			&& (superuser() || entry->user_id == GetUserId()))
		{
			if (endpoint_on_qd(entry))
			{
				if (gp_session_id == entry->session_id || is_all)
				{
					/* one end-point on master */
					dbinfo = dbid_get_dbinfo(MASTER_DBID);

					char *token = PrintToken(entry->token);

					values[0] = CStringGetTextDatum(token);
					nulls[0] = false;
					values[1] = CStringGetTextDatum(entry->cursor_name);
					nulls[1] = false;
					values[2] = Int32GetDatum(entry->session_id);
					nulls[2] = false;
					values[3] = CStringGetTextDatum(dbinfo->hostname);
					nulls[3] = false;
					values[4] = Int32GetDatum(dbinfo->port);
					nulls[4] = false;
					values[5] = Int32GetDatum(MASTER_DBID);
					nulls[5] = false;
					values[6] = ObjectIdGetDatum(entry->user_id);
					nulls[6] = false;

					/*
					 * find out the status of end-point
					 */
					EndpointStatus *ep_status = find_endpoint_status(mystatus->status, mystatus->status_num,
																	 entry->token, MASTER_DBID);
					values[7] = CStringGetTextDatum(endpoint_status_enum_to_string(ep_status));
					nulls[7] = false;

					mystatus->curTokenIdx++;
					tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
					result = HeapTupleGetDatum(tuple);
					LWLockRelease(TokensLWLock);
					SRF_RETURN_NEXT(funcctx, result);
					pfree(token);
				} else
				{
					mystatus->curTokenIdx++;
				}
			} else
			{
				/* end-points on segments */
				while ((mystatus->curSegIdx < mystatus->segment_num) &&
					   ((mystatus->seg_db_list[mystatus->curSegIdx].role != 'p') ||
						!dbid_has_token(entry, mystatus->seg_db_list[mystatus->curSegIdx].dbid)))
				{
					mystatus->curSegIdx++;
				}

				if (mystatus->curSegIdx == mystatus->segment_num)
				{
					/* go to the next token */
					mystatus->curTokenIdx++;
					mystatus->curSegIdx = 0;
				} else if (mystatus->seg_db_list[mystatus->curSegIdx].role == 'p'
						   && mystatus->curSegIdx < mystatus->segment_num)
				{
					if (gp_session_id == entry->session_id || is_all)
					{
						/* get a primary segment and return this token and segment */
						char *token = PrintToken(entry->token);

						values[0] = CStringGetTextDatum(token);
						nulls[0] = false;
						values[1] = CStringGetTextDatum(entry->cursor_name);
						nulls[1] = false;
						values[2] = Int32GetDatum(entry->session_id);
						nulls[2] = false;
						values[3] = CStringGetTextDatum(mystatus->seg_db_list[mystatus->curSegIdx].hostname);
						nulls[3] = false;
						values[4] = Int32GetDatum(mystatus->seg_db_list[mystatus->curSegIdx].port);
						nulls[4] = false;
						values[5] = Int32GetDatum(mystatus->seg_db_list[mystatus->curSegIdx].dbid);
						nulls[5] = false;
						values[6] = ObjectIdGetDatum(entry->user_id);
						nulls[6] = false;

						/*
						 * find out the status of end-point
						 */
						EndpointStatus *qe_status = find_endpoint_status(mystatus->status,
																		 mystatus->status_num,
																		 entry->token,
																		 mystatus->seg_db_list[mystatus->curSegIdx].dbid);
						values[7] = CStringGetTextDatum(endpoint_status_enum_to_string(qe_status));
						nulls[7] = false;

						mystatus->curSegIdx++;
						if (mystatus->curSegIdx == mystatus->segment_num)
						{
							mystatus->curTokenIdx++;
							mystatus->curSegIdx = 0;
						}

						tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
						result = HeapTupleGetDatum(tuple);
						LWLockRelease(TokensLWLock);
						SRF_RETURN_NEXT(funcctx, result);
						pfree(token);
					} else
					{
						mystatus->curSegIdx++;
						if (mystatus->curSegIdx == mystatus->segment_num)
						{
							mystatus->curTokenIdx++;
							mystatus->curSegIdx = 0;
						}
					}
				}
			}
		} else
		{
			mystatus->curTokenIdx++;
		}
	}
	LWLockRelease(TokensLWLock);
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
	Datum values[8];
	bool nulls[8] = {true};
	HeapTuple tuple;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc tupdesc = CreateTemplateTupleDesc(8, false);

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


		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (EndpointsStatusInfo *) palloc0(sizeof(EndpointsStatusInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->endpoints_num = MAX_ENDPOINT_SIZE;
		mystatus->current_idx = 0;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	// FIXME: WHY????
	mystatus = funcctx->user_fctx;

	LWLockAcquire(EndpointsLWLock, LW_SHARED);
	while (mystatus->current_idx < mystatus->endpoints_num && SharedEndpoints != NULL)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum result;

		Endpoint entry = &SharedEndpoints[mystatus->current_idx];

		if (!entry->empty && (superuser() || entry->user_id == GetUserId()))
		{
			char *status = NULL;
			char *token = PrintToken(entry->token);

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
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			mystatus->current_idx++;
			LWLockRelease(EndpointsLWLock);
			SRF_RETURN_NEXT(funcctx, result);
			pfree(token);
		}
		mystatus->current_idx++;
	}
	LWLockRelease(EndpointsLWLock);
	SRF_RETURN_DONE(funcctx);
}

/*
 * Create a magic number from a given token for DSM TOC usage.
 * The same tokens will eventually generate the same magic number.
 */
uint64 create_magic_num_from_token(const int8 *token)
{
	uint64 magic = 0;
	Assert(token);
	for (int i = 0; i < 8; i++)
	{
		magic |= ((uint64)token[i]) << i * 8;
	}
	return magic;
}

/*
 * Returns true if the two given endpoint tokens are equal.
 */
bool
compare_token(const int8* token1, const int8* token2)
{
	Assert(token1);
	Assert(token2);
	/* memcmp should be good enough. Timing attack would not be a concern here. */
	return memcmp(token1, token2, ENDPOINT_TOKEN_LEN) == 0;
}

void
InvalidateEndpointToken(int8* token /*out*/)
{
	Assert(token);
	memset(token, 0, ENDPOINT_TOKEN_LEN);
}

bool
IsEndpointTokenValid(const int8* token) {
	Assert(token);
	for (int i = 0; i < ENDPOINT_TOKEN_LEN; ++i) {
		if (token[i]) return true;
	}
	return false;
}

