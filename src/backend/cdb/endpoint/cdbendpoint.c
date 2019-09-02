/*
 * cdbendpoint.c
 *
 * When define and execute a PARALLEL RETRIEVE CURSOR, the results are written to endpoints.
 *
 * Endpoint may exist on master or segments, depends on the query of the PARALLEL RETRIEVE CURSOR:
 * (1) An endpoint is on QD only if the query of the parallel
 *     cursor needs to be finally gathered by the master. e.g.
 *     > CREATE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 ORDER BY C1;
 * (2) The endpoints are on specific segments node if the direct dispatch happens. e.g.
 *     > CREATE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 WHERE C1=1 OR C1=2;
 * (3) The endpoints are on all segments node. e.g.
 *     > CREATE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1;
 *
 * When QE or QD write results to endpoint, it will replace normal dest receiver with
 * TQueueDestReceiver, so that the query results will be write into a shared
 * message queue. Then the retrieve backend use TupleQueueReader to read
 * query results from the shared message queue(cdbendpointretrieve.c).
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbendpoint.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cdb/cdbendpoint.h"
#include "access/xact.h"
#include "access/tupdesc.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"

#define WAIT_RECEIVE_TIMEOUT            50
#define ENDPOINT_TUPLE_QUEUE_SIZE       65536  /* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */
#define InvalidSession                  (-1)

#define SHMEM_PARALLEL_CURSOR_ENTRIES   "SharedMemoryParallelCursorTokens"
#define SHMEM_ENDPOINTS_ENTRIES         "SharedMemoryEndpointDescEntries"

ParallelCursorTokenDesc *SharedTokens = NULL;            /* Point to ParallelCursorTokenDesc entries in shared memory */
EndpointDesc *SharedEndpoints = NULL;                    /* Point to EndpointDesc entries in shared memory */

#ifdef FAULT_INJECTOR
static int8 dummyToken[ENDPOINT_TOKEN_LEN] = {0xef};
#endif

static MsgQueueStatusEntry *currentMQEntry = NULL;       /* Current message queue entry */
static EndpointDesc *my_shared_endpoint = NULL;          /* Current EndpointDesc entry */

/* Endpoint and PARALLEL RETRIEVE CURSOR token helper function */
static void init_shared_endpoints(void *address);
static void free_endpoint(EndpointDesc *endpointDesc);
static void free_endpoint_by_cursor_name(const char *cursor_name);
static void wait_for_init_by_cursor_name(const char *cursor_name);
static void init_shared_tokens(void *address);
static void reset_shared_token(ParallelCursorTokenDesc *desc);
static void parallel_cursor_exit_callback(int code, Datum arg);
static bool remove_parallel_cursor(const char  *cursor_name, bool *on_qd, List **seg_list);

/* sender which is an endpoint */
static void set_sender_pid(void);
static void create_and_connect_mq(TupleDesc tupleDesc);
static void wait_receiver(void);
static void sender_finish(void);
static void sender_close(void);
static void unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc);
static void signal_receiver_abort(volatile EndpointDesc *endPointDesc);
static void endpoint_abort(void);
static void register_endpoint_xact_callbacks(void);
static void sender_xact_abort_callback(XactEvent ev, void *vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);

/* utility */
static int16 dbid_to_contentid(CdbComponentDatabases *dbs, int16 dbid);
static void check_end_point_allocated(void);
static void set_attach_status(enum AttachStatus status);
static void generate_token(int8 *token);
extern bool token_equals(const int8 *token1, const int8 *token2);
extern uint64 create_magic_num_from_token(const int8 *token);
extern void generate_endpoint_name(char *name, const char *cursor_name, int32 session_id, int32 segindex);
extern EndpointDesc * find_endpoint_by_cursor_name(const char *name);

/*
 * Endpoint_ShmemSize - Calculate the shared memory size for PARALLEL RETRIEVE CURSOR execute.
 *
 * The size contains LWLocks and EndpointSharedCTX.
 */
Size
EndpointShmemSize(void)
{
	Size size;
	size = mul_size(sizeof(LWLockPadded), 2);
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		size += MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(ParallelCursorTokenDesc)));
	}
	size += MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc)));
	return size;
}

/*
 * Endpoint_CTX_ShmemInit - Init shared memory structure for PARALLEL RETRIEVE CURSOR execute.
 */
void
EndpointCTXShmemInit(void)
{
	bool is_shmem_ready;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		SharedTokens = (ParallelCursorTokenDesc *) ShmemInitStruct(
			SHMEM_PARALLEL_CURSOR_ENTRIES,
			MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(ParallelCursorTokenDesc))),
			&is_shmem_ready);
		Assert(is_shmem_ready || !IsUnderPostmaster);
		if (!is_shmem_ready)
		{
			init_shared_tokens(SharedTokens);
		}
		before_shmem_exit(parallel_cursor_exit_callback, (Datum) 0);
	}
	SharedEndpoints = (EndpointDesc *) ShmemInitStruct(
		SHMEM_ENDPOINTS_ENTRIES,
		MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc))),
		&is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);
	if (!is_shmem_ready)
	{
		init_shared_endpoints(SharedEndpoints);
	}
}

/*
 * Init EndpointDesc entriesS.
 */
static void
init_shared_endpoints(void *address)
{
	Endpoint endpoints = (Endpoint) address;

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		endpoints[i].database_id = InvalidOid;
		endpoints[i].sender_pid = InvalidPid;
		endpoints[i].receiver_pid = InvalidPid;
		InvalidateEndpointToken(endpoints[i].token);
		endpoints[i].handle = DSM_HANDLE_INVALID;
		endpoints[i].session_id = InvalidSession;
		endpoints[i].user_id = InvalidOid;
		endpoints[i].attach_status = Status_NotAttached;
		endpoints[i].empty = true;
		InitSharedLatch(&endpoints[i].ack_done);
	}
}

/*
 * Init ParallelCursorTokenDesc entries.
 */
static void
init_shared_tokens(void *address)
{
	ParaCursorToken tokens = (ParaCursorToken) address;
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		reset_shared_token(&tokens[i]);
	}
}

/*
 * On QD, if the process exit, the ParallelCursorTokenDesc entries that allocated
 * in this QD need to be removed.
 *
 * This function registered in on_shmem_exit. Cause when proc exit,
 * the call stack is:
 * shmem_exit()
 * --> before_shmem_exit
 *     --> parallel_cursor_exit_callback
 *         --> reset_shared_token all tokens, no need to free endpoints since
 *         endpoint's callback will free the exist EndpointDesc slot
 *         for current session.
 *     --> ... (other before shmem callback if exists)
 *     --> ... (other callbacks)
 *     --> ShutdownPostgres (the last before shmem callback)
 *         --> AbortOutOfAnyTransaction
 *             --> ...
 *             --> CleanupTransaction
 *                 --> AtCleanup_Portals
 *                     --> PortalDrop
 *                         --> DestoryParallelCursor
 *                         	   --> remove_parallel_cursor
 */
static void
parallel_cursor_exit_callback(int code, Datum arg)
{
	elog(DEBUG3, "CDB_ENDPOINT: PARALLEL RETRIEVE CURSOR exit callback.");

	LWLockAcquire(ParallelCursorTokenLock, LW_EXCLUSIVE);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].session_id == gp_session_id)
		{
			reset_shared_token(&SharedTokens[i]);
		}
	}
	LWLockRelease(ParallelCursorTokenLock);
}

/*
 * remove_parallel_cursor - remove PARALLEL RETRIEVE CURSOR from shared memory
 */
bool
remove_parallel_cursor(const char *cursor_name, bool *on_qd, List **seg_list)
{
	Assert(cursor_name && cursor_name[0]);
	bool found = false;

	LWLockAcquire(ParallelCursorTokenLock, LW_EXCLUSIVE);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (strncmp(SharedTokens[i].cursor_name, cursor_name, NAMEDATALEN) == 0 &&
				SharedTokens[i].session_id == gp_session_id)
		{
			found = true;
			if (on_qd != NULL && endpoint_on_qd(&SharedTokens[i]))
			{
				*on_qd = true;
			}
			else
			{
				if (seg_list != NULL && SharedTokens[i].endPointExecPosition != ENDPOINT_ON_ALL_QE)
				{
					int16 x = -1;
					CdbComponentDatabases *cdbs = cdbcomponent_getCdbComponents();
					while ((x = get_next_dbid_from_bitmap(SharedTokens[i].dbIds, x)) >= 0)
					{
						*seg_list = lappend_int(*seg_list, dbid_to_contentid(cdbs, x));
					}
					Assert((*seg_list)->length == SharedTokens[i].endpoint_cnt);
				}
			}

			elog(DEBUG3, "CDB_ENDPOINT: <RemoveParallelCursorToken> removed: %s"
				", session id: %d from shared memory",
				 cursor_name, SharedTokens[i].session_id);

			reset_shared_token(&SharedTokens[i]);
			break;
		}
	}
	LWLockRelease(ParallelCursorTokenLock);
	return found;
}

/*
 * Clear the given ParallelCursorTokenDesc entry.
 * Needs to be called with exclusive lock on ParallelCursorTokenLock.
 */
void
reset_shared_token(ParallelCursorTokenDesc *desc)
{
	InvalidateEndpointToken(desc->token);
	memset(desc->cursor_name, 0, NAMEDATALEN);
	desc->session_id = InvalidSession;
	desc->user_id = InvalidOid;
	desc->endpoint_cnt = 0;
	desc->endPointExecPosition = ENDPOINT_POS_INVALID;
	memset(desc->dbIds, 0, sizeof(int32) * MAX_NWORDS);
}

/*
 * generate_token - Generate an unique int64 token into the given address.
 */
void
generate_token(int8 *token)
{
#ifdef HAVE_STRONG_RANDOM
	REGENERATE:
	if (!pg_strong_random(token, ENDPOINT_TOKEN_LEN))
	{
		elog(ERROR, "Failed to generate a new random token.");
	}
#else
#error A strong random number source is needed.
#endif
	if (!IsEndpointTokenValid(token))
	{
		goto REGENERATE;
	}
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token_equals(token, SharedTokens[i].token))
		{
			goto REGENERATE;
		}
	}
}

/*
 * GetParallelCursorEndpointPosition - get PARALLEL RETRIEVE CURSOR endpoint allocate position
 *
 * If already focused and flow is CdbLocusType_SingleQE, CdbLocusType_Entry,
 * we assume the endpoint should be existed on QD. Else, on QEs.
 */
enum EndPointExecPosition
GetParallelCursorEndpointPosition(const struct Plan *planTree)
{
	if (planTree->flow->flotype == FLOW_SINGLETON &&
		planTree->flow->locustype != CdbLocusType_SegmentGeneral)
	{
		return ENDPOINT_ON_QD;
	}
	else
	{
		if (planTree->flow->flotype == FLOW_SINGLETON)
		{
			/*
			 * In this case, the plan is for replicated table.
			 * locustype must be CdbLocusType_SegmentGeneral.
			 */
			Assert(planTree->flow->locustype == CdbLocusType_SegmentGeneral);
			return ENDPOINT_ON_SINGLE_QE;
		}
		else if (planTree->directDispatch.isDirectDispatch &&
				 planTree->directDispatch.contentIds != NULL)
		{
			/*
			 * Direct dispatch to some segments, so end-points only exist
			 * on these segments
			 */
			return ENDPOINT_ON_SOME_QE;
		}
		else
		{
			return ENDPOINT_ON_ALL_QE;
		}
	}
}

/*
 * ChooseEndpointContentIDForParallelCursor - choose endpoints position base on plan.
 *
 * Base on different condition, we need figure out which
 * segments that endpoints should be allocated in.
 */
List *
ChooseEndpointContentIDForParallelCursor(const struct Plan *planTree,
										 enum EndPointExecPosition *position)
{
	List *cids = NIL;
	*position = GetParallelCursorEndpointPosition(planTree);
	switch (*position)
	{
		case ENDPOINT_ON_QD:
		{
			cids = list_make1_int(MASTER_CONTENT_ID);
			break;
		}
		case ENDPOINT_ON_SINGLE_QE:
		{
			cids = list_make1_int(gp_session_id % planTree->flow->numsegments);
			break;
		}
		case ENDPOINT_ON_SOME_QE:
		{
			ListCell *cell;
			foreach(cell, planTree->directDispatch.contentIds)
			{
				int contentid = lfirst_int(cell);
				cids = lappend_int(cids, contentid);
			}
			break;
		}
		case ENDPOINT_ON_ALL_QE:
		default:
			break;
	}
	return cids;
}

/*
 * AddParallelCursorToken - allocate PARALLEL RETRIEVE CURSOR token into shared memory.
 * Memory the information of PARALLEL RETRIEVE CURSOR tokens on all or which segments,
 * while DECLARE PARALLEL RETRIEVE CURSOR
 * The newly alloced token will be returned by argument 'token' pointer.
 *
 * The UDF gp_endpoints_info() queries the information.
 */
void
AddParallelCursorToken(int8 *token /*out*/, const char *name, int session_id, Oid user_id,
					   enum EndPointExecPosition endPointExecPosition, List *seg_list)
{
	int i;

	Assert(token);
	Assert(name != NULL);
	Assert(session_id != InvalidSession);

	LWLockAcquire(ParallelCursorTokenLock, LW_EXCLUSIVE);
	generate_token(token);

#ifdef FAULT_INJECTOR
	/* inject fault to set end-point shared memory slot full. */
	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		const char *FJ_CURSOR = "FAULT_INJECTION_CURSOR";

		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (!IsEndpointTokenValid(SharedTokens[i].token))
			{
				/* pretend to set a valid token */
				snprintf(SharedTokens[i].cursor_name, NAMEDATALEN, "%s", FJ_CURSOR);
				SharedTokens[i].session_id = session_id;
				memcpy(SharedTokens[i].token, dummyToken, ENDPOINT_TOKEN_LEN);
				SharedTokens[i].user_id = user_id;
				SharedTokens[i].endPointExecPosition = endPointExecPosition;
			}
		}
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (token_equals(SharedTokens[i].token, dummyToken))
			{
				memset(SharedTokens[i].cursor_name, '\0', NAMEDATALEN);
				InvalidateEndpointToken(SharedTokens[i].token);
			}
		}
	}
#endif

	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!IsEndpointTokenValid(SharedTokens[i].token))
		{
			snprintf(SharedTokens[i].cursor_name, NAMEDATALEN, "%s", name);
			SharedTokens[i].session_id = session_id;
			memcpy(SharedTokens[i].token, token, ENDPOINT_TOKEN_LEN);
			SharedTokens[i].user_id = user_id;
			SharedTokens[i].endPointExecPosition = endPointExecPosition;
			if (seg_list != NIL)
			{
				ListCell *l;

				foreach(l, seg_list)
				{
					int16 contentid = lfirst_int(l);

					add_dbid_into_bitmap(SharedTokens[i].dbIds,
										 contentid_get_dbid(contentid,
															GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
															false));
					SharedTokens[i].endpoint_cnt++;
				}
			}

			char *token_str = PrintToken(token);
			elog(DEBUG3, "CDB_ENDPOINT: added a new token: '%s'"
						 ", session id: %d, cursor name: %s, into shared memory",
				 token_str, session_id, SharedTokens[i].cursor_name);
			pfree(token_str);
			break;
		}
	}

	LWLockRelease(ParallelCursorTokenLock);

	/* no empty entry to save this token */
	if (i == MAX_ENDPOINT_SIZE)
	{
		elog(ERROR, "can't add a new token %s into shared memory", PrintToken(token));
	}
}

void
WaitEndpointReady(const struct Plan *planTree, const char *cursorName)
{
	char cmd[255];
	List *cids;
	enum EndPointExecPosition endPointExecPosition;

	cids = ChooseEndpointContentIDForParallelCursor(
		planTree, &endPointExecPosition);

	if (endPointExecPosition == ENDPOINT_ON_QD)
	{
		DirectFunctionCall3(gp_operate_endpoints_token, CharGetDatum('w'),
							CStringGetDatum(""), CStringGetDatum(cursorName));
	}
	else
	{
		snprintf(cmd, 255, "select __gp_operate_endpoints_token('w', '', '%s')", cursorName);
		if (endPointExecPosition == ENDPOINT_ON_ALL_QE)
		{
			/* Push token to all segments */
			CdbDispatchCommand(cmd, DF_CANCEL_ON_ERROR, NULL);
		}
		else
		{
			CdbDispatchCommandToSegments(cmd, DF_CANCEL_ON_ERROR, cids, NULL);
		}
	}
}

/*
 * Check if the given token is created by the current user.
 * Returns true if the given token is created by the current user.
 */
bool
CheckParallelCursorPrivilege(const int8 *token)
{
	bool result = false;

	Assert(IsUnderPostmaster);
	LWLockAcquire(ParallelCursorTokenLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token_equals(SharedTokens[i].token, token))
		{
			result = SharedTokens[i].user_id == GetUserId();
			break;
		}
	}
	LWLockRelease(ParallelCursorTokenLock);
	return result;
}

/*
 * DestoryParallelCursor - Remove the target token information from parallel
 * cursor token shared memory. We need clean the token from shared memory
 * when cursor close and exception happens.
 */
void
DestroyParallelCursor(const char *cursorName)
{
	bool found;
	bool on_qd = false;
	List *seg_list = NIL;

	found = remove_parallel_cursor(cursorName, &on_qd, &seg_list);

	/*
	 * During abort progress, the Endpoints'(on QE/QD) xact abort will
	 * clean endpoint info. So there's no need dispatch the free endpoint
	 * UDF cmd to Endpoints(on QE/QD).
	 *
	 * Also, if dispatch free endpoint UDF cmd to Endpoints during in
	 * abort progress, it'll trigger "signal 6: Abort trap" exception.
	 */
	if (found && !IsAbortInProgress())
	{
		/* free end-point */
		if (on_qd)
		{
			free_endpoint_by_cursor_name(cursorName);
		} else
		{
			size_t cmd_len = 255;
			char cmd[cmd_len];
			snprintf(cmd, cmd_len, "select __gp_operate_endpoints_token('f', '', '%s')",
					cursorName);
			if (seg_list != NIL)
			{
				/* dispatch to some segments. */
				CdbDispatchCommandToSegments(cmd, DF_CANCEL_ON_ERROR, seg_list, NULL);
			}
			else
			{
				/* dispatch to all segments. */
				CdbDispatchCommand(cmd, DF_CANCEL_ON_ERROR, NULL);
			}
		}
	}
}

/*
 * Obtain the content-id of a segment by given dbid
 */
static int16
dbid_to_contentid(CdbComponentDatabases *cdbs, int16 dbid)
{
	/* Can only run on a master node. */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "dbid_to_contentid() should only execute on execution segments");

	for (int i = 0; i < cdbs->total_entry_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdi = &cdbs->entry_db_info[i];
		if (cdi->config->dbid == dbid)
		{
			return cdi->config->segindex;
		}
	}

	for (int i = 0; i < cdbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdi = &cdbs->segment_db_info[i];
		if (cdi->config->dbid == dbid)
		{
			return cdi->config->segindex;
		}
	}

	elog(ERROR, "CDB_ENDPOINT: No content id for current dbid %d", dbid);
	return -2; // Should not reach this line.
}

/*
 * CreateTQDestReceiverForEndpoint - Create the dest receiver of PARALLEL RETRIEVE CURSOR
 *
 * Also create shared memory message queue here. Alloc local currentMQEntry to track
 * endpoint info.
 * Create TupleQueueDestReceiver base on the message queue to pass tuples to retriever.
 */
DestReceiver *
CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc, const char* cursorName)
{
	// Register callback to deal with proc exit.
	register_endpoint_xact_callbacks();
	AllocEndpointOfToken(cursorName);
	set_sender_pid();
	check_end_point_allocated();

	currentMQEntry = MemoryContextAllocZero(TopMemoryContext, sizeof(MsgQueueStatusEntry));
	memcpy(currentMQEntry->endpoint_name, my_shared_endpoint->name, ENDPOINT_NAME_LEN);
	currentMQEntry->mq_seg = NULL;
	currentMQEntry->mq_handle = NULL;
	currentMQEntry->retrieve_status = RETRIEVE_STATUS_INIT;
	currentMQEntry->retrieve_ts = NULL;
	currentMQEntry->tq_reader = NULL;
	create_and_connect_mq(tupleDesc);
	return CreateTupleQueueDestReceiver(currentMQEntry->mq_handle);
}

/*
 * DestroyTQDestReceiverForEndpoint - destroy TupleQueueDestReceiver
 *
 * If the queue is large enough for tuples to send, must wait for a receiver
 * to attach the message queue before endpoint detaches the message queue.
 * Cause if the queue gets detached before receiver attaches, the queue
 * will never be attached by a receiver.
 *
 * Should also clean all other endpoint info here.
 */
void
DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest)
{
	/* wait for receiver to retrieve the first row */
	wait_receiver();
	/* tqueueShutdownReceiver() will call shm_mq_detach(), so need to
	 * call it before sender_close()*/
	(*endpointDest->rShutdown)(endpointDest);
	(*endpointDest->rDestroy)(endpointDest);
	sender_close();

	sender_finish();
	unset_endpoint_sender_pid(my_shared_endpoint);
	set_attach_status(Status_Finished);
	ClearGpToken();
	ClearParallelCursorExecRole();
}

/*
 * AllocEndpointOfToken should be called before set_sender_pid.
 * Since current QD/QE has been specified as endpoint, when start
 * to send data to message queue, we set current proc id as sender id.
 * Also set other info for the allocated EndpointDesc entry.
 */
static void
set_sender_pid(void)
{
	int i;
	int found_idx = -1;

	Assert(SharedEndpoints);

	elog(DEBUG3, "CDB_ENDPOINT: set sender pid");
	if (EndpointCtl.Gp_prce_role != PRCER_SENDER)
		elog(ERROR, "%s could not allocate endpoint slot",
			 EndpointRoleToString(EndpointCtl.Gp_prce_role));

	if (my_shared_endpoint && IsEndpointTokenValid(my_shared_endpoint->token))
		elog(ERROR, "endpoint is already allocated");

	CheckTokenValid();

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	/*
     * Presume that for any token, only one PARALLEL RETRIEVE CURSOR is activated at
     * that time.
     */
	/* find the slot with the same token */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token_equals(SharedEndpoints[i].token, EndpointCtl.Gp_token))
		{
			found_idx = i;
			break;
		}
	}

	if (found_idx != -1)
	{
		SharedEndpoints[i].sender_pid = MyProcPid;
		OwnLatch(&SharedEndpoints[i].ack_done);
	}

	my_shared_endpoint = &SharedEndpoints[i];

	LWLockRelease(ParallelCursorEndpointLock);

	if (!my_shared_endpoint)
		elog(ERROR, "failed to allocate endpoint");
}

/*
 * AllocEndpointOfToken - Allocate an EndpointDesc entry in shared memroy.
 *
 * Find a free slot in DSM and set token and other info.
 */
void
AllocEndpointOfToken(const char *cursorName)
{
	int i;
	int found_idx = -1;

	if (!IsEndpointTokenValid(EndpointCtl.Gp_token))
		elog(ERROR, "allocate endpoint of invalid token ID");
	Assert(SharedEndpoints);
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */
	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (!IsEndpointTokenValid(SharedEndpoints[i].token))
			{
				/* pretend to set a valid token */
				SharedEndpoints[i].database_id = MyDatabaseId;
				memcpy(SharedEndpoints[i].token, dummyToken, ENDPOINT_TOKEN_LEN);
				SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
				SharedEndpoints[i].session_id = gp_session_id;
				SharedEndpoints[i].user_id = GetUserId();
				SharedEndpoints[i].sender_pid = InvalidPid;
				SharedEndpoints[i].receiver_pid = InvalidPid;
				SharedEndpoints[i].attach_status = Status_NotAttached;
				SharedEndpoints[i].empty = false;
			}
		}
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (token_equals(SharedEndpoints[i].token, dummyToken))
			{
				InvalidateEndpointToken(SharedEndpoints[i].token);
				SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
				SharedEndpoints[i].empty = true;
			}
		}
	}
#endif

	/*
	 * Presume that for any token, only one PARALLEL RETRIEVE CURSOR is activated at
	 * that time.
	 */
	/* find the slot with the same token */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token_equals(SharedEndpoints[i].token, EndpointCtl.Gp_token))
		{
			found_idx = i;
			break;
		}
	}

	/* find a new slot */
	for (i = 0; i < MAX_ENDPOINT_SIZE && found_idx == -1; ++i)
	{
		if (SharedEndpoints[i].empty)
		{
			found_idx = i;
			break;
		}
	}

	if (found_idx != -1)
	{
		generate_endpoint_name(SharedEndpoints[i].name, cursorName, gp_session_id, GpIdentity.segindex);
		snprintf(EndpointCtl.cursor_name, NAMEDATALEN, "%s", cursorName);
		SharedEndpoints[i].database_id = MyDatabaseId;
		memcpy(SharedEndpoints[i].token, EndpointCtl.Gp_token, ENDPOINT_TOKEN_LEN);
		SharedEndpoints[i].session_id = gp_session_id;
		SharedEndpoints[i].user_id = GetUserId();
		SharedEndpoints[i].sender_pid = InvalidPid;
		SharedEndpoints[i].receiver_pid = InvalidPid;
		SharedEndpoints[i].attach_status = Status_NotAttached;
		SharedEndpoints[i].empty = false;
	}

	LWLockRelease(ParallelCursorEndpointLock);

	if (found_idx == -1)
		elog(ERROR, "failed to allocate endpoint");
}

/*
 * Waits until the QE is ready -- the dest receiver will be ready after this
 * function returns successfully.
 */
void
wait_for_init_by_cursor_name(const char *cursor_name)
{
	int wr = 0;
	EndpointDesc *endpointDesc = find_endpoint_by_cursor_name(cursor_name);
//	if (!endpointDesc) {
//		elog(ERROR, "Endpoint doesn't exist");
//	}
}

/*
 *  Find and free the EndpointDesc entry by the cursor name.
 */
void
free_endpoint_by_cursor_name(const char *cursor_name)
{
	EndpointDesc *endpointDesc = find_endpoint_by_cursor_name(cursor_name);

	// FIXME: Log error instead?
	if (endpointDesc) {
		free_endpoint(endpointDesc);
	}
}

/*
 * Free the given EndpointDesc entry.
 */
void
free_endpoint(EndpointDesc *endpointDesc)
{
	if (!endpointDesc && !endpointDesc->empty)
		elog(ERROR, "not an valid endpoint");

	elog(DEBUG3, "CDB_ENDPOINTS: Free endpoint '%s'.", endpointDesc->name);

	unset_endpoint_sender_pid(endpointDesc);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	endpointDesc->database_id = InvalidOid;
	InvalidateEndpointToken(endpointDesc->token);
	endpointDesc->handle = DSM_HANDLE_INVALID;
	endpointDesc->session_id = InvalidSession;
	endpointDesc->user_id = InvalidOid;
	endpointDesc->empty = true;
	LWLockRelease(ParallelCursorEndpointLock);
}

/**
 * Check the PARALLEL RETRIEVE CURSOR execution status, if get error, then rethrow the error
 *
 * @param isWait:  support 2 modes: WAIT/NOWAIT
 * @return true if the PARALLEL RETRIEVE CURSOR Execution Finished
 */
bool
CheckParallelCursorErrors(QueryDesc *queryDesc, bool isWait)
{
	EState	   *estate;
	bool       isParallelRetrCursorFinished = false;

	/* caller must have switched into per-query memory context already */
	estate = queryDesc->estate;
	/*
	 * If QD, wait for QEs to finish and check their results.
	 */
	if (estate->dispatcherState && estate->dispatcherState->primaryResults)
	{
		ErrorData *qeError = NULL;

		CdbDispatcherState *ds = estate->dispatcherState;
		DispatchWaitMode waitMode = isWait ? DISPATCH_WAIT_NONE : DISPATCH_WAIT_CHECK;
		cdbdisp_checkDispatchResult(ds, waitMode);
		cdbdisp_getDispatchResults(ds, &qeError);
		if (qeError)
		{
			estate->dispatcherState = NULL;
			cdbdisp_destroyDispatcherState(ds);
			ReThrowError(qeError);
		}
		isParallelRetrCursorFinished = cdbdisp_isDispatchFinished(ds);
	}
	return isParallelRetrCursorFinished;
}

void
HandleEndpointFinish(void)
{

	if (my_shared_endpoint && EndpointCtl.Gp_prce_role == PRCER_SENDER)
	{
		LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
		if (my_shared_endpoint->attach_status == Status_Prepared ||
			my_shared_endpoint->attach_status == Status_Attached)
		{
			SetLatch(&MyProc->procLatch);
		}
		LWLockRelease(ParallelCursorEndpointLock);
	}
}

/*
 * When endpoint init, create and setup the shared memory message queue.
 *
 * Create a dsm which contains a TOC(table of content). It has 3 parts:
 * 1. Tuple's TupleDesc length.
 * 2. Tuple's TupleDesc.
 * 3. Shared memory message queue.
 */
static void
create_and_connect_mq(TupleDesc tupleDesc)
{
	CheckTokenValid();
	Assert(currentMQEntry);
	if (currentMQEntry->mq_handle != NULL)
		return;

	elog(DEBUG3, "CDB_ENDPOINTS: create and setup the shared memory message queue.");
	dsm_segment *dsm_seg;
	shm_toc *toc;
	shm_mq *mq;
	shm_toc_estimator toc_est;
	Size toc_size;
	int tupdesc_len;
	char *tupdesc_ser;
	char *tdlen_space;
	char *tupdesc_space;
	TupleDescNode *node = makeNode(TupleDescNode);

	/* Serialize TupleDesc */
	node->natts = tupleDesc->natts;
	node->tuple = tupleDesc;
	tupdesc_ser = serializeNode((Node *) node, &tupdesc_len, NULL /* uncompressed_size */ );

	/*
	 * Calculate dsm size, size = toc meta + toc_nentry(3) * entry size + tuple desc
	 * length size + tuple desc size + queue size.
	*/
	shm_toc_initialize_estimator(&toc_est);
	shm_toc_estimate_chunk(&toc_est, sizeof(tupdesc_len));
	shm_toc_estimate_chunk(&toc_est, tupdesc_len);
	shm_toc_estimate_keys(&toc_est, 2);

	shm_toc_estimate_chunk(&toc_est, ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_estimate_keys(&toc_est, 1);
	toc_size = shm_toc_estimate(&toc_est);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	dsm_seg = dsm_create(toc_size);
	if (dsm_seg == NULL)
	{
		LWLockRelease(ParallelCursorEndpointLock);
		sender_close();
		elog(ERROR, "failed to create shared message queue for send tuples.");
	}
	my_shared_endpoint->handle = dsm_segment_handle(dsm_seg);
	LWLockRelease(ParallelCursorEndpointLock);
	dsm_pin_mapping(dsm_seg);

	toc = shm_toc_create(create_magic_num_from_token(my_shared_endpoint->token), dsm_segment_address(dsm_seg),
						 toc_size);

	tdlen_space = shm_toc_allocate(toc, sizeof(tupdesc_len));
	memcpy(tdlen_space, &tupdesc_len, sizeof(tupdesc_len));
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC_LEN, tdlen_space);

	tupdesc_space = shm_toc_allocate(toc, tupdesc_len);
	memcpy(tupdesc_space, tupdesc_ser, tupdesc_len);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC, tupdesc_space);

	mq = shm_mq_create(shm_toc_allocate(toc, ENDPOINT_TUPLE_QUEUE_SIZE), ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_QUEUE, mq);
	shm_mq_set_sender(mq, MyProc);
	currentMQEntry->mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
	set_attach_status(Status_Prepared);
	currentMQEntry->mq_seg = dsm_seg;
}

/*
 * wait_receiver - wait receiver to retrieve at least once from the
 * shared memory message queue.
 *
 * If the queue only attached by the sender and the queue is large enough
 * for all tuples, sender should wait receiver. Cause if sender detached
 * from the queue, the queue will be not available for receiver.
 */
static void
wait_receiver(void)
{
	elog(DEBUG3, "CDB_ENDPOINTS: wait receiver.");
	while (true)
	{
		int wr;

		CHECK_FOR_INTERRUPTS();

		if (QueryFinishPending)
			break;

		/* Check the QD dispatcher connection is lost */
		unsigned char firstchar;
		int r;

		pq_startmsgread();
		r = pq_getbyte_if_available(&firstchar);
		if (r < 0)
		{
			elog(ERROR, "unexpected EOF on query dispatcher connection");
		}
		else if (r > 0)
		{
			elog(ERROR, "query dispatcher should get nothing until QE backend finished processing");
		}
		else
		{
			/* no data available without blocking */
			pq_endmsgread();
			/* continue processing as normal case */
		}

		elog(DEBUG5, "CDB_ENDPOINT: sender wait latch in wait_receiver()");
		wr = WaitLatch(&my_shared_endpoint->ack_done,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_RECEIVE_TIMEOUT);
		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			sender_close();
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT:sender reset latch in wait_receiver()");
		ResetLatch(&my_shared_endpoint->ack_done);
		break;
	}
}


/*
 * Once sender finish send tuples, and receiver already retrieve some tuples,
 * Still need to wait receiver retrieve all data from the queue.
 */
static void
sender_finish(void)
{
	elog(DEBUG3, "CDB_ENDPOINTS: sender finish");
	/* wait for receiver to finish retrieving */
	wait_receiver();
}

/*
 * Endpoint is finish the job. So detach from the shared memory
 * message queue. Clean current endpoint local info.
 */
static void
sender_close(void)
{
	elog(DEBUG3, "CDB_ENDPOINT: Sender message queue detaching.");
	// If error happened, currentMQEntry could be none.
	if (currentMQEntry != NULL && currentMQEntry->mq_seg != NULL)
	{
		dsm_detach(currentMQEntry->mq_seg);
		if (currentMQEntry->retrieve_ts != NULL)
			ExecDropSingleTupleTableSlot(currentMQEntry->retrieve_ts);
		pfree(currentMQEntry);
		currentMQEntry = NULL;
	}
}

/*
 * Unset endpoint sender pid.
 *
 * Clean the EndpointDesc entry sender pid when endpoint finish it's job.
 * Also consider to clean receiver state.
 */
static void
unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc)
{
	if (!endPointDesc || (endPointDesc && endPointDesc->empty))
		return;

	/*
	 * Since the receiver is not in the session, sender has the duty to cancel
	 * it
	 */
	signal_receiver_abort(endPointDesc);

	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint sender pid.");
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	/*
	 * Only the endpoint QE/QD execute this unset sender pid function.
	 * The sender pid in Endpoint entry must be MyProcPid or InvalidPid.
	 * Note the "gp_operate_endpoints_token" UDF dispatch comment.
	 */
	Assert(MyProcPid == endPointDesc->sender_pid || endPointDesc->sender_pid == InvalidPid);
	if (MyProcPid == endPointDesc->sender_pid)
	{
		endPointDesc->sender_pid = InvalidPid;
		ResetLatch(&endPointDesc->ack_done);
		DisownLatch(&endPointDesc->ack_done);
	}

	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * If endpoint exit with error, let retrieve role know exception happened.
 * Called by endpoint.
 */
static void
signal_receiver_abort(volatile EndpointDesc *endPointDesc)
{
	pid_t receiver_pid;
	bool is_attached;

	if (!endPointDesc || (endPointDesc && endPointDesc->empty))
		return;

	elog(DEBUG3, "CDB_ENDPOINT: signal the receiver to abort.");
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	receiver_pid = endPointDesc->receiver_pid;
	is_attached = endPointDesc->attach_status == Status_Attached;
	if (receiver_pid != InvalidPid && is_attached && receiver_pid != MyProcPid)
	{
		pg_signal_backend(receiver_pid, SIGINT, "Signal the receiver to abort.");
	}

	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Clean up EndpointDesc entry for specify token.
 *
 * The sender should only have one in EndpointCtl.TokensInXact list.
 */
static void endpoint_abort(void)
{
	free_endpoint_by_cursor_name(EndpointCtl.cursor_name);
	my_shared_endpoint = NULL;
	ClearGpToken();
	ClearParallelCursorExecRole();
}

/*
 * Register callback for endpoint/sender to deal with xact abort.
 */
static void
register_endpoint_xact_callbacks(void)
{
	static bool is_registered = false;

	if (!is_registered)
	{
		// Register callback to deal with proc endpoint xact abort.
		RegisterSubXactCallback(sender_subxact_callback, NULL);
		RegisterXactCallback(sender_xact_abort_callback, NULL);
		is_registered = true;
	}
}

/*
 * If endpoint/sender on xact abort, we need to do sender clean jobs.
 */
void
sender_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT)
	{
		if (Gp_role == GP_ROLE_RETRIEVE || Gp_role == GP_ROLE_UTILITY)
		{
			return;
		}
		elog(DEBUG3, "CDB_ENDPOINT: sender xact abort callback");
		endpoint_abort();
		/*
		 * During xact abort, should make sure the endpoint_cleanup called first.
		 * Cause if call sender_close to detach the message queue first, the retriever
		 * may read NULL from message queue, then retrieve mark itself down.
		 *
		 * So here, we need to make sure signal retrieve abort first before endpoint
		 * detach message queue.
		 */
		sender_close();
	}
}

/*
 * If endpoint/sender on sub xact abort, we need to do sender clean jobs.
 */
void
sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		sender_xact_abort_callback(XACT_EVENT_ABORT, arg);
	}
}

static void
check_end_point_allocated(void)
{
	if (EndpointCtl.Gp_prce_role != PRCER_SENDER)
		elog(ERROR, "%s could not check endpoint allocated status",
			 EndpointRoleToString(EndpointCtl.Gp_prce_role));

	if (!my_shared_endpoint)
		elog(ERROR, "endpoint for token %s is not allocated", PrintToken(EndpointCtl.Gp_token));

	CheckTokenValid();

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	if (!token_equals(my_shared_endpoint->token, EndpointCtl.Gp_token))
	{
		LWLockRelease(ParallelCursorEndpointLock);
		elog(ERROR, "endpoint for token %s is not allocated", PrintToken(EndpointCtl.Gp_token));
	}
	LWLockRelease(ParallelCursorEndpointLock);
}

static void
set_attach_status(enum AttachStatus status)
{
	if (EndpointCtl.Gp_prce_role != PRCER_SENDER)
		elog(ERROR, "%s could not set endpoint", EndpointRoleToString(EndpointCtl.Gp_prce_role));

	if (!my_shared_endpoint && !my_shared_endpoint->empty)
		elog(ERROR, "endpoint doesn't exist");

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	my_shared_endpoint->attach_status = status;

	LWLockRelease(ParallelCursorEndpointLock);

	if (status == Status_Finished)
		my_shared_endpoint = NULL;
}

/*
 * gp_operate_endpoints_token - Operation for EndpointDesc entries on endpoint.
 *
 * Alloc/free/unset sender pid operations for a EndpointDesc entry base on the token.
 *
 * We dispatch this UDF by "CdbDispatchCommandToSegments" and "CdbDispatchCommand",
 * It'll always dispatch to writer gang, which is the gang that allocate endpoints on.
 * Since we always allocate endpoints on the top most slice gang.
 * FIXME: refactor to make wait and free work on endpoint QE/entry QD
 */
Datum
gp_operate_endpoints_token(PG_FUNCTION_ARGS)
{
	char operation;
	const char *token_str = NULL;
	const char *cursor_name = NULL;
	int8 token[ENDPOINT_TOKEN_LEN] = {0};
	Assert(Gp_role == GP_ROLE_EXECUTE);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	operation = PG_GETARG_CHAR(0);
	token_str = PG_GETARG_CSTRING(1);
	cursor_name = PG_GETARG_CSTRING(2);

	if (Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
	{
		switch (operation)
		{
			case 'p':
				/* Push endpoint */
				ParseToken(token, token_str);
//				AllocEndpointOfToken(token, cursor_name);
				break;
			case 'w':
				wait_for_init_by_cursor_name(cursor_name);
				break;
			case 'f':
				/* Free endpoint */
				free_endpoint_by_cursor_name(cursor_name);
				break;
			default:
				elog(ERROR, "Failed to execute gp_operate_endpoints_token('%c', '%s')", operation, token_str);
				PG_RETURN_BOOL(false);
		}
	}
	PG_RETURN_BOOL(true);
}

/*
 * Generate the endpoint name based on the PARALLEL RETRIEVE CURSOR name,
 * session ID and the segment index.
 * The endpoint name should be unique across sessions.
 */
void generate_endpoint_name(char *name,
		const char *cursor_name, int32 session_id, int32 segindex)
{
    snprintf(name, ENDPOINT_NAME_LEN, "%s_%08x_%08x", cursor_name,
	     session_id, segindex);
}

/*
 * Find the EndpointDesc entry by the given cursor name in current session.
 */
EndpointDesc * find_endpoint_by_cursor_name(const char *cursor_name)
{
	EndpointDesc *res = NULL;
	char *endpoint_name = palloc(ENDPOINT_NAME_LEN);
	generate_endpoint_name(endpoint_name, cursor_name, gp_session_id, GpIdentity.segindex);

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!SharedEndpoints[i].empty && SharedEndpoints[i].session_id == gp_session_id &&
			strncmp(SharedEndpoints[i].name, endpoint_name, ENDPOINT_NAME_LEN) == 0)
		{

			res = &SharedEndpoints[i];
			break;
		}
	}
	LWLockRelease(ParallelCursorEndpointLock);
	pfree(endpoint_name);
	return res;
}

