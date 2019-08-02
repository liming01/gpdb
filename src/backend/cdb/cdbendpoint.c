/*
 * cdbendpoint.c
 *
 * When define and execute a parallel cursor, the results are written to endpoints.
 *
 * Endpoint may exist on master or segments, depends on the query of the parallel cursor:
 * (1) An endpoint is on QD only if the query of the parallel
 *     cursor needs to be finally gathered by the master. e.g.
 *     > CREATE c1 PARALLEL CURSOR FOR SELECT * FROM T1 ORDER BY C1;
 * (2) The endpoints are on specific segments node if the direct dispatch happens. e.g.
 *     > CREATE c1 PARALLEL CURSOR FOR SELECT * FROM T1 WHERE C1=1 OR C1=2;
 * (3) The endpoints are on all segments node. e.g.
 *     > CREATE c1 PARALLEL CURSOR FOR SELECT * FROM T1;
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

#define TOKEN_STR_LEN                   21     /* length 21 = length of max int64 value + '\0' */
#define WAIT_RECEIVE_TIMEOUT            50
#define ENDPOINT_TUPLE_QUEUE_SIZE       65536  /* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */
#define DummyToken                      (0)    /* For fault injection */

#define ENDPOINT_KEY_TUPLE_DESC_LEN     1
#define ENDPOINT_KEY_TUPLE_DESC         2
#define ENDPOINT_KEY_TUPLE_QUEUE        3

#define SHMEM_TOKENCTX                  "ShareTokenCTX"
#define SHMEM_PARALLEL_CURSOR_ENTRIES   "SharedMemoryParallelCursorTokens"
#define SHMEM_ENDPOINTS_ENTRIES         "SharedMemoryEndpointDescEntries"

EndpointSharedCTX *endpointSC = NULL;                    /* Shared memory context with LWLocks */
ParallelCursorTokenDesc *SharedTokens = NULL;            /* Point to ParallelCursorTokenDesc entries in shared memory */
EndpointDesc *SharedEndpoints = NULL;                    /* Point to EndpointDesc entries in shared memory */

struct EndpointControl EndpointCtl = {                   /* Endpoint ctrl */
	InvalidToken, PCER_NONE, NIL, NIL
};

static MsgQueueStatusEntry *currentMQEntry = NULL;       /* Current message queue entry */
static volatile EndpointDesc *my_shared_endpoint = NULL; /* Current EndpointDesc entry */

/* Endpoint and parallel cursor token helper function */
static void init_shared_endpoints(void *address);
static void init_shared_tokens(void *address);
static void parallel_cursor_exit_callback(int code, Datum arg);
static bool remove_parallel_cursor(int64 token, bool *on_qd, List **seg_list);

/* sender which is an endpoint */
static void set_sender_pid(void);
static void create_and_connect_mq(TupleDesc tupleDesc);
static void wait_receiver(void);
static void sender_finish(void);
static void sender_close(void);
static void unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc);
static void signal_receiver_abort(volatile EndpointDesc *endPointDesc);
static void endpoint_cleanup(void);
static void register_endpoint_callbacks(void);
static void sender_xact_abort_callback(XactEvent ev, void *vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);

/* utility */
static int16 dbid_to_contentid(CdbComponentDatabases *dbs, int16 dbid);
extern char *get_token_name_format_str(void);
static void check_end_point_allocated(void);
static void set_attach_status(enum AttachStatus status);

/*
 * Endpoint_ShmemSize - Calculate the shared memory size for parallel cursor execute.
 *
 * The size contains LWLocks and EndpointSharedCTX.
 */
Size
Endpoint_ShmemSize(void)
{
	Size size;
	size = MAXALIGN(sizeof(EndpointSharedCTX));
	size = add_size(size, mul_size(sizeof(LWLockPadded), 2));
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		size += MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(ParallelCursorTokenDesc)));
	}
	size += MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc)));
	return size;
}

/*
 * Endpoint_CTX_ShmemInit - Init shared memory structure for parallel cursor execute.
 */
void
Endpoint_CTX_ShmemInit(void)
{
	bool is_shmem_ready;
	Size offset;
	char *ptr;

	endpointSC = (EndpointSharedCTX *) ShmemInitStruct(
		SHMEM_TOKENCTX,
		MAXALIGN(sizeof(EndpointSharedCTX)) + sizeof(LWLockPadded) * 2,
		&is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);
	if (!is_shmem_ready)
	{
		offset = MAXALIGN(sizeof(EndpointSharedCTX));
		ptr = (char *) endpointSC;
		endpointSC->tranche_id = LWLockNewTrancheId();
		endpointSC->tranche.name = "EndpointLocks";
		endpointSC->tranche.array_base = ptr + offset;
		endpointSC->tranche.array_stride = sizeof(LWLockPadded);
		endpointSC->endpointLWLocks = (LWLockPadded *) (ptr + offset);

		LWLockRegisterTranche(endpointSC->tranche_id, &endpointSC->tranche);
		LWLockInitialize(EndpointsLWLock, endpointSC->tranche_id);
		LWLockInitialize(TokensLWLock, endpointSC->tranche_id);
	}
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
	// Register callback to deal with proc exit.
	register_endpoint_callbacks();
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
		endpoints[i].token = InvalidToken;
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
		tokens[i].token = InvalidToken;
		memset(tokens[i].cursor_name, 0, NAMEDATALEN);
		tokens[i].session_id = InvalidSession;
		tokens[i].user_id = InvalidOid;
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
 *         --> remove_parallel_cursor all tokens, no need to free endpoints since
 *         endpoint's callback will free itself.
 *     --> ... (other before shmem callback if exists)
 *     --> ... (other callbacks)
 *     --> ShutdownPostgres (the last before shmem callback)
 *         --> AbortOutOfAnyTransaction
 *             --> ...
 *             --> CleanupTransaction
 *                 --> AtCleanup_Portals
 *                     --> PortalDrop
 *                         --> DestoryParallelCursor
 *                         	   --> remove_parallel_cursor will do nothing cause
 *                         	   clean job already done in parallel_cursor_exit_callback
 */
static void
parallel_cursor_exit_callback(int code, Datum arg)
{
	ListCell *l;

	if (EndpointCtl.Cursor_tokens != NIL)
	{
		foreach(l, EndpointCtl.Cursor_tokens)
		{
			int64 token = atoll(lfirst(l));
			remove_parallel_cursor(token, NULL, NULL);
			pfree(lfirst(l));
		}
		list_free(EndpointCtl.Cursor_tokens);
		EndpointCtl.Cursor_tokens = NIL;
	}
}

/*
 * GetUniqueGpToken - Generate an unique int64 token
 */
int64
GetUniqueGpToken(void)
{
	int64 token;
	char *token_str;
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);

	LWLockAcquire(TokensLWLock, LW_SHARED);

	srand(ts.tv_nsec);
	REGENERATE:
	token = llabs(((int64) rand() << 32) | rand());
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token == SharedTokens[i].token)
			goto REGENERATE;
	}

	LWLockRelease(TokensLWLock);

	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	token_str = palloc0(TOKEN_STR_LEN);
	pg_lltoa(token, token_str);
	/* During declare parallel cursor, record the token in case proc exit with error, so
	 * we can clean it */
	EndpointCtl.Cursor_tokens = lappend(EndpointCtl.Cursor_tokens, token_str);
	MemoryContextSwitchTo(oldcontext);
	return token;
}

/*
 * GetParallelCursorEndpointPosition - get parallel cursor endpoint allocate position
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
	} else
	{
		if (planTree->flow->flotype == FLOW_SINGLETON)
		{
			/*
			 * In this case, the plan is for replicated table.
			 * locustype must be CdbLocusType_SegmentGeneral.
			 */
			Assert(planTree->flow->locustype == CdbLocusType_SegmentGeneral);
			return ENDPOINT_ON_SINGLE_QE;
		} else if (planTree->directDispatch.isDirectDispatch &&
				   planTree->directDispatch.contentIds != NULL)
		{
			/*
			 * Direct dispatch to some segments, so end-points only exist
			 * on these segments
			 */
			return ENDPOINT_ON_SOME_QE;
		} else
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
 * AddParallelCursorToken - allocate parallel cursor token into shared memory.
 * Memory the information of parallel cursor tokens on all or which segments,
 * while DECLARE PARALLEL CURSOR
 *
 * The UDF gp_endpoints_info() queries the information.
 */
void
AddParallelCursorToken(int64 token, const char *name, int session_id, Oid user_id,
					   bool all_seg, List *seg_list)
{
	int i;

	Assert(token != InvalidToken && name != NULL
		   && session_id != InvalidSession);

	LWLockAcquire(TokensLWLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault to set end-point shared memory slot full. */
	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		const char *FJ_CURSOR = "FAULT_INJECTION_CURSOR";

		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedTokens[i].token == InvalidToken)
			{
				/* pretend to set a valid token */
				snprintf(SharedTokens[i].cursor_name, NAMEDATALEN, "%s", FJ_CURSOR);
				SharedTokens[i].session_id = session_id;
				SharedTokens[i].token = DummyToken;
				SharedTokens[i].user_id = user_id;
				SharedTokens[i].all_seg = all_seg;
			}
		}
	} else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedTokens[i].token == DummyToken)
			{
				memset(SharedTokens[i].cursor_name, '\0', NAMEDATALEN);
				SharedTokens[i].token = InvalidToken;
			}
		}
	}
#endif

	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == InvalidToken)
		{
			snprintf(SharedTokens[i].cursor_name, NAMEDATALEN, "%s", name);
			SharedTokens[i].session_id = session_id;
			SharedTokens[i].token = token;
			SharedTokens[i].user_id = user_id;
			SharedTokens[i].all_seg = all_seg;
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
			elog(DEBUG3, "CDB_ENDPOINT: added a new token: "
				INT64_FORMAT
				", session id: %d, cursor name: %s, into shared memory",
				 token, session_id, SharedTokens[i].cursor_name);
			break;
		}
	}

	LWLockRelease(TokensLWLock);

	/* no empty entry to save this token */
	if (i == MAX_ENDPOINT_SIZE)
	{
		elog(ERROR, "can't add a new token %s into shared memory", PrintToken(token));
	}

}

/*
 * Returns true if the given token is created by the current user.
 */
bool
CheckParallelCursorPrivilege(int64 token)
{
	bool result = false;

	Assert(IsUnderPostmaster);
	LWLockAcquire(TokensLWLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == token)
		{
			result = SharedTokens[i].user_id == GetUserId();
			break;
		}
	}
	LWLockRelease(TokensLWLock);
	return result;
}

/*
 * CreateTQDestReceiverForEndpoint - Create the dest receiver of parallel cursor
 *
 * Also create shared memory message queue here. Alloc local currentMQEntry to track
 * endpoint info.
 * Create TupleQueueDestReceiver base on the message queue to pass tuples to retriever.
 */
DestReceiver *
CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc)
{
	set_sender_pid();
	check_end_point_allocated();

	currentMQEntry = MemoryContextAllocZero(TopMemoryContext, sizeof(MsgQueueStatusEntry));
	currentMQEntry->retrieve_token = EndpointCtl.Gp_token;
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

	if (EndpointCtl.Gp_pce_role != PCER_SENDER)
		elog(ERROR, "%s could not allocate endpoint slot",
			 EndpointRoleToString(EndpointCtl.Gp_pce_role));

	if (my_shared_endpoint && my_shared_endpoint->token != InvalidToken)
		elog(ERROR, "endpoint is already allocated");

	CheckTokenValid();

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	/*
     * Presume that for any token, only one parallel cursor is activated at
     * that time.
     */
	/* find the slot with the same token */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].token == EndpointCtl.Gp_token)
		{
			found_idx = i;
			break;
		}
	}

	if (found_idx != -1)
	{
		SharedEndpoints[i].database_id = MyDatabaseId;
		SharedEndpoints[i].sender_pid = MyProcPid;
		SharedEndpoints[i].receiver_pid = InvalidPid;
		SharedEndpoints[i].token = EndpointCtl.Gp_token;
		SharedEndpoints[i].session_id = gp_session_id;
		SharedEndpoints[i].user_id = GetUserId();
		SharedEndpoints[i].attach_status = Status_NotAttached;
		SharedEndpoints[i].empty = false;
		OwnLatch(&SharedEndpoints[i].ack_done);
	}

	my_shared_endpoint = &SharedEndpoints[i];

	LWLockRelease(EndpointsLWLock);

	if (!my_shared_endpoint)
		elog(ERROR, "failed to allocate endpoint");
}

/*
 * UnsetSenderPidOfToken - unset sender pid for parallel cursor token
 *
 * When the endpoint send all data finish the cursor portal, unset the
 * sender pid and related info in EndpointDesc entry.
 */
void
UnsetSenderPidOfToken(int64 token)
{
	volatile EndpointDesc *endPointDesc = find_endpoint_by_token(token);
	if (!endPointDesc)
	{
		elog(ERROR, "no valid endpoint info for token "
			INT64_FORMAT
			"", token);
	}
	unset_endpoint_sender_pid(endPointDesc);
}

/*
 * DestoryParallelCursor - Remove the target token information from parallel
 * cursor token shared memory. We need clean the token from shared memory
 * when cursor close and exception happens.
 */
void
DestoryParallelCursor(int64 token)
{
	Assert(token != InvalidToken);
	bool found;
	bool on_qd = false;
	List *seg_list = NIL;

	found = remove_parallel_cursor(token, &on_qd, &seg_list);

	if (found)
	{
		/* free end-point */
		if (on_qd)
		{
			FreeEndpointOfToken(token);
		} else
		{
			char cmd[255];
			sprintf(cmd, "select __gp_operate_endpoints_token('f', '"
				INT64_FORMAT
				"')", token);
			if (seg_list != NIL)
			{
				/* dispatch to some segments. */
				CdbDispatchCommandToSegments(cmd, DF_CANCEL_ON_ERROR, seg_list, NULL);
			} else
			{
				/* dispatch to all segments. */
				CdbDispatchCommand(cmd, DF_CANCEL_ON_ERROR, NULL);
			}
		}
	}
}

bool
remove_parallel_cursor(int64 token, bool *on_qd, List **seg_list)
{
	Assert(token != InvalidToken);
	bool found = false;

	LWLockAcquire(TokensLWLock, LW_EXCLUSIVE);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == token)
		{
			found = true;
			if (on_qd != NULL && endpoint_on_qd(&SharedTokens[i]))
			{
				*on_qd = true;
			} else
			{
				if (seg_list != NULL && !SharedTokens[i].all_seg)
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

			elog(DEBUG3, "CDB_ENDPOINT: <RemoveParallelCursorToken> removed token: "
				INT64_FORMAT
				", session id: %d, cursor name: %s from shared memory",
				 token, SharedTokens[i].session_id, SharedTokens[i].cursor_name);
			SharedTokens[i].token = InvalidToken;
			memset(SharedTokens[i].cursor_name, 0, NAMEDATALEN);
			SharedTokens[i].session_id = InvalidSession;
			SharedTokens[i].user_id = InvalidOid;
			SharedTokens[i].endpoint_cnt = 0;
			SharedTokens[i].all_seg = false;
			memset(SharedTokens[i].dbIds, 0, sizeof(int32) * MAX_NWORDS);
			break;
		}
	}
	LWLockRelease(TokensLWLock);
	return found;
}

/*
 * AllocEndpointOfToken - Allocate an EndpointDesc entry in shared memroy.
 *
 * Find a free slot in DSM and set token and other info.
 */
void
AllocEndpointOfToken(int64 token)
{
	int i;
	int found_idx = -1;
	char *token_str;

	if (token == InvalidToken)
		elog(ERROR, "allocate endpoint of invalid token ID");
	Assert(SharedEndpoints);
	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */


	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedEndpoints[i].token == InvalidToken)
			{
				/* pretend to set a valid token */
				SharedEndpoints[i].database_id = MyDatabaseId;
				SharedEndpoints[i].token = DummyToken;
				SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
				SharedEndpoints[i].session_id = gp_session_id;
				SharedEndpoints[i].user_id = GetUserId();
				SharedEndpoints[i].sender_pid = InvalidPid;
				SharedEndpoints[i].receiver_pid = InvalidPid;
				SharedEndpoints[i].attach_status = Status_NotAttached;
				SharedEndpoints[i].empty = false;
			}
		}
	} else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedEndpoints[i].token == DummyToken)
			{
				SharedEndpoints[i].token = InvalidToken;
				SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
				SharedEndpoints[i].empty = true;
			}
		}
	}
#endif

	/*
	 * Presume that for any token, only one parallel cursor is activated at
	 * that time.
	 */
	/* find the slot with the same token */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].token == token)
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
		SharedEndpoints[i].database_id = MyDatabaseId;
		SharedEndpoints[i].token = token;
		SharedEndpoints[i].session_id = gp_session_id;
		SharedEndpoints[i].user_id = GetUserId();
		SharedEndpoints[i].sender_pid = InvalidPid;
		SharedEndpoints[i].receiver_pid = InvalidPid;
		SharedEndpoints[i].attach_status = Status_NotAttached;
		SharedEndpoints[i].empty = false;

		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		token_str = palloc0(TOKEN_STR_LEN);
		pg_lltoa(token, token_str);
		EndpointCtl.Endpoint_tokens = lappend(EndpointCtl.Endpoint_tokens, token_str);
		MemoryContextSwitchTo(oldcontext);
	}

	LWLockRelease(EndpointsLWLock);

	if (found_idx == -1)
		elog(ERROR, "failed to allocate endpoint");
}

/*
 * FreeEndpointOfToken - Free an EndpointDesc entry.
 *
 * Find the EndpointDesc entry by token and free it.
 */
void
FreeEndpointOfToken(int64 token)
{
	volatile EndpointDesc *endPointDesc = find_endpoint_by_token(token);

	if (!endPointDesc)
		return;

	if (!endPointDesc && !endPointDesc->empty)
		elog(ERROR, "not an valid endpoint");

	unset_endpoint_sender_pid(endPointDesc);

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);
	endPointDesc->database_id = InvalidOid;
	endPointDesc->token = InvalidToken;
	endPointDesc->handle = DSM_HANDLE_INVALID;
	endPointDesc->session_id = InvalidSession;
	endPointDesc->user_id = InvalidOid;
	endPointDesc->empty = true;
	LWLockRelease(EndpointsLWLock);
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

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);
	dsm_seg = dsm_create(toc_size);
	if (dsm_seg == NULL)
	{
		LWLockRelease(EndpointsLWLock);
		sender_close();
		elog(ERROR, "failed to create shared message queue for send tuples.");
	}
	my_shared_endpoint->handle = dsm_segment_handle(dsm_seg);
	LWLockRelease(EndpointsLWLock);
	dsm_pin_mapping(dsm_seg);

	toc = shm_toc_create(my_shared_endpoint->token, dsm_segment_address(dsm_seg), toc_size);

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
		} else if (r > 0)
		{
			elog(ERROR, "query dispatcher should get nothing until QE backend finished processing");
		} else
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

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

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

	LWLockRelease(EndpointsLWLock);
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

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	receiver_pid = endPointDesc->receiver_pid;
	is_attached = endPointDesc->attach_status == Status_Attached;

	LWLockRelease(EndpointsLWLock);
	if (receiver_pid != InvalidPid && is_attached && receiver_pid != MyProcPid)
	{
		pg_signal_backend(receiver_pid, SIGINT, "Signal the receiver to abort.");
	}
}

/*
 * Clean up EndpointDesc entry for specify token.
 *
 * The sender should only have one in EndpointCtl.TokensInXact list.
 */
static void endpoint_cleanup(void)
{
	ListCell *l;
	if (EndpointCtl.Endpoint_tokens != NIL)
	{
		foreach(l, EndpointCtl.Endpoint_tokens)
		{
			int64 token = atoll(lfirst(l));
			FreeEndpointOfToken(token);
			pfree(lfirst(l));
		}
		list_free(EndpointCtl.Endpoint_tokens);
		EndpointCtl.Endpoint_tokens = NIL;
	}
	my_shared_endpoint = NULL;
	ClearGpToken();
	ClearParallelCursorExecRole();
}

/*
 * Register callback for endpoint/sender to deal with xact abort.
 */
static void
register_endpoint_callbacks(void)
{
	// Register callback to deal with proc endpoint xact abort.
	RegisterSubXactCallback(sender_subxact_callback, NULL);
	RegisterXactCallback(sender_xact_abort_callback, NULL);
}

/*
 * If endpoint/sender on xact abort, we need to do sender clean jobs.
 */
static void sender_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT)
	{
		sender_close();
		endpoint_cleanup();
	}
}

/*
 * If endpoint/sender on sub xact abort, we need to do sender clean jobs.
 */
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		sender_xact_abort_callback(XACT_EVENT_ABORT, arg);
	}
}

/*
 * Return the value of static variable Gp_token
 */
int64
GpToken(void)
{
	return EndpointCtl.Gp_token;
}

void
CheckTokenValid(void)
{
	if (Gp_role == GP_ROLE_EXECUTE && EndpointCtl.Gp_token == InvalidToken)
		elog(ERROR, "invalid endpoint token");
}

/*
 * Set the variable Gp_token
 */
void
SetGpToken(int64 token)
{
	if (EndpointCtl.Gp_token != InvalidToken)
		elog(ERROR, "endpoint token %s is already set", PrintToken(EndpointCtl.Gp_token));

	EndpointCtl.Gp_token = token;
}

/*
 * Clear the variable Gp_token
 */
void
ClearGpToken(void)
{
	EndpointCtl.Gp_token = InvalidToken;
}

/*
 * Convert the string tk0123456789 to int 0123456789
 */
int64
ParseToken(char *token)
{
	int64 token_id = InvalidToken;
	char *tokenFmtStr = get_token_name_format_str();

	if (token[0] == tokenFmtStr[0] && token[1] == tokenFmtStr[1])
	{
		token_id = atoll(token + 2);
	} else
	{
		elog(ERROR, "invalid token \"%s\"", token);
	}

	return token_id;
}

/*
 * Generate a string tk0123456789 from int 0123456789
 *
 * Note: need to pfree() the result
 */
char *
PrintToken(int64 token_id)
{
	Insist(token_id != InvalidToken);

	char *res = palloc(23);        /* length 13 = 2('tk') + 20(length of max int64 value) + 1('\0') */

	sprintf(res, get_token_name_format_str(), token_id);
	return res;
}

List *
GetContentIDsByToken(int64 token)
{
	List *l = NIL;

	LWLockAcquire(TokensLWLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == token)
		{
			if (SharedTokens[i].all_seg)
			{
				l = NIL;
				break;
			} else
			{
				int16 x = -1;
				CdbComponentDatabases *cdbs = cdbcomponent_getCdbComponents();

				while ((x = get_next_dbid_from_bitmap(SharedTokens[i].dbIds, x)) >= 0)
				{
					l = lappend_int(l, dbid_to_contentid(cdbs, x));
				}
				Assert(l->length == SharedTokens[i].endpoint_cnt);
				break;
			}
		}
	}
	LWLockRelease(TokensLWLock);
	return l;
}

/*
 * Set the role of endpoint, sender or receiver
 */
void
SetParallelCursorExecRole(enum ParallelCursorExecRole role)
{
	if (EndpointCtl.Gp_pce_role != PCER_NONE)
		elog(ERROR, "endpoint role %s is already set",
			 EndpointRoleToString(EndpointCtl.Gp_pce_role));

	elog(DEBUG3, "CDB_ENDPOINT: set endpoint role to %s", EndpointRoleToString(role));

	EndpointCtl.Gp_pce_role = role;
}

/*
 * Clear the role of endpoint
 */
void
ClearParallelCursorExecRole(void)
{
	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint role %s", EndpointRoleToString(EndpointCtl.Gp_pce_role));

	EndpointCtl.Gp_pce_role = PCER_NONE;
}

/*
 * Return the value of static variable Gp_pce_role
 */
enum ParallelCursorExecRole GetParallelCursorExecRole(void)
{
	return EndpointCtl.Gp_pce_role;
}

const char *
EndpointRoleToString(enum ParallelCursorExecRole role)
{
	switch (role)
	{
		case PCER_SENDER:
			return "[END POINT SENDER]";

		case PCER_RECEIVER:
			return "[END POINT RECEIVER]";

		case PCER_NONE:
			return "[END POINT NONE]";

		default:
			elog(ERROR, "unknown end point role %d", role);
			return NULL;
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

char *
get_token_name_format_str(void)
{
	static char tokenNameFmtStr[64] = "";
	if (strlen(tokenNameFmtStr) == 0)
	{
		char *p = INT64_FORMAT;
		snprintf(tokenNameFmtStr, sizeof(tokenNameFmtStr), "tk%%020%s", p + 1);
	}
	return tokenNameFmtStr;
}

static void
check_end_point_allocated(void)
{
	if (EndpointCtl.Gp_pce_role != PCER_SENDER)
		elog(ERROR, "%s could not check endpoint allocated status",
			 EndpointRoleToString(EndpointCtl.Gp_pce_role));

	if (!my_shared_endpoint)
		elog(ERROR, "endpoint for token %s is not allocated", PrintToken(EndpointCtl.Gp_token));

	CheckTokenValid();

	LWLockAcquire(EndpointsLWLock, LW_SHARED);
	if (my_shared_endpoint->token != EndpointCtl.Gp_token)
	{
		LWLockRelease(EndpointsLWLock);
		elog(ERROR, "endpoint for token %s is not allocated", PrintToken(EndpointCtl.Gp_token));
	}
	LWLockRelease(EndpointsLWLock);
}

static void
set_attach_status(enum AttachStatus status)
{
	if (EndpointCtl.Gp_pce_role != PCER_SENDER)
		elog(ERROR, "%s could not set endpoint", EndpointRoleToString(EndpointCtl.Gp_pce_role));

	if (!my_shared_endpoint && !my_shared_endpoint->empty)
		elog(ERROR, "endpoint doesn't exist");

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	my_shared_endpoint->attach_status = status;

	LWLockRelease(EndpointsLWLock);

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
 */
Datum
gp_operate_endpoints_token(PG_FUNCTION_ARGS)
{
	char operation;
	const char *token;
	Assert(Gp_role == GP_ROLE_EXECUTE);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	operation = PG_GETARG_CHAR(0);
	token = PG_GETARG_CSTRING(1);

	int64 tokenid;
	tokenid = atoll(token);

	if (tokenid != InvalidToken && Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
	{
		switch (operation)
		{
			case 'p':
				/* Push endpoint */
				AllocEndpointOfToken(tokenid);
				break;
			case 'f':
				/* Free endpoint */
				FreeEndpointOfToken(tokenid);
				break;
			case 'u':
				/* Unset sender pid of endpoint */
				UnsetSenderPidOfToken(tokenid);
				break;
			default:
				elog(ERROR, "Failed to execute gp_operate_endpoints_token('%c', '%s')", operation, token);
				PG_RETURN_BOOL(false);
		}
	}
	PG_RETURN_BOOL(true);
}
