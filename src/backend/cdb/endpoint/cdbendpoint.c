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
#include <cdb/cdbdisp_dtx.h>
#include <utils/portal.h>
#include <cdb/cdbdispatchresult.h>

#include "cdb/cdbendpoint.h"
#include "cdbendpointinternal.h"
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
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/backend_cancel.h"
#include "storage/procsignal.h"

/* The timeout before returns failure for endpoints initialization. */
#define WAIT_ENDPOINT_INIT_TIMEOUT      5000
#define WAIT_NORMAL_TIMEOUT             300
#define ENDPOINT_TUPLE_QUEUE_SIZE       65536  /* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */

#define SHMEM_ENDPOINTS_ENTRIES         "SharedMemoryEndpointDescEntries"
#define SHMEM_ENPOINTS_SESSION_INFO     "EndpointsSessionInfosHashtable"

#ifdef FAULT_INJECTOR
#define DUMMY_ENDPOINT_NAME "DUMMYENDPOINTNAME"
#define DUMMY_CURSOR_NAME   "DUMMYCURSORNAME"
#endif

typedef struct SessionTokenTag
{
	int sessionID;
	Oid userID;
} SessionTokenTag;

typedef struct SessionInfoEntry
{
	SessionTokenTag tag;
	/* Write Gang will set this if needed and wait for the procLatch. When sender
	 * sees a non-null value, it should set the procLatch to notify the write
	 * gang. */
	PGPROC *init_wait_proc;
	/* The auth token for this session. */
	int8 token[ENDPOINT_TOKEN_LEN];
} SessionInfoEntry;

EndpointDesc *SharedEndpoints = NULL;                    /* Point to EndpointDesc entries in shared memory */
HTAB *SharedSessionInfoHash = NULL;	                     /* Shared hash table for session infos */

static MsgQueueStatusEntry *currentMQEntry = NULL;       /* Current message queue entry */
static EndpointDesc *my_shared_endpoint = NULL;          /* Current EndpointDesc entry */

/* Init helper functions */
static void init_shared_endpoints(void *address);

/* QD utility functions */
static bool call_endpoint_udf_on_qd(const struct Plan *planTree, const char *cursorName, char operator);
static const int8 * get_or_create_token_on_qd(void);

/* sender(which is an endpoint) helper function */
static void alloc_endpoint_for_cursor(const char *cursorName);
static void create_and_connect_mq(TupleDesc tupleDesc);
static void declare_parallel_retrieve_ready(void);
static void wait_receiver(void);
static void sender_finish(void);
static void sender_close(void);
static void unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc);
static void signal_receiver_abort(volatile EndpointDesc *endPointDesc);
static void endpoint_abort(void);
static void wait_parallel_retrieve_close(void);
static void free_endpoint_by_cursor_name(const char *cursorName);
static void register_endpoint_callbacks(void);
static void endpoint_exit_callback(int code, Datum arg);
static void sender_xact_abort_callback(XactEvent ev, void *vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);

/* utility */
static void check_endpoint_allocated(void);
static void generate_endpoint_name(char *name, const char *cursorName,
								   int32 sessionID, int32 segindex);
static EndpointDesc * find_endpoint_by_cursor_name(const char *name, bool with_lock);
static void set_attach_status(enum AttachStatus status);
static void check_dispatch_connection(void);

/* Endpoints internal operation UDF's helper function  */
static bool check_endpoint_finished_by_cursor_name(const char *cursorName, bool isWait);
static void wait_for_init_by_cursor_name(const char *cursorName, const char *tokenStr);
static bool check_parallel_retrieve_cursor(const char *cursorName, bool isWait);
static bool check_parallel_cursor_errors(QueryDesc *queryDesc);

/*
 * Endpoint_ShmemSize - Calculate the shared memory size for PARALLEL RETRIEVE CURSOR execute.
 *
 * The size contains LWLocks and EndpointSharedCTX.
 */
Size
EndpointShmemSize(void)
{
	Size size;
	size = MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc)));
	size = add_size(size, hash_estimate_size(MAX_ENDPOINT_SIZE, sizeof(SessionInfoEntry)));
	return size;
}

/*
 * Endpoint_CTX_ShmemInit - Init shared memory structure for PARALLEL RETRIEVE CURSOR execute.
 */
void
EndpointCTXShmemInit(void)
{
	bool is_shmem_ready;
	HASHCTL hctl;

	SharedEndpoints = (EndpointDesc *) ShmemInitStruct(
		SHMEM_ENDPOINTS_ENTRIES,
		MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc))),
		&is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);
	if (!is_shmem_ready)
	{
		init_shared_endpoints(SharedEndpoints);
	}

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(SessionTokenTag);
	hctl.entrysize = sizeof(SessionInfoEntry);
	hctl.hash = tag_hash;
	SharedSessionInfoHash =
		ShmemInitHash(SHMEM_ENPOINTS_SESSION_INFO, MAX_ENDPOINT_SIZE,
					  MAX_ENDPOINT_SIZE, &hctl, HASH_ELEM | HASH_FUNCTION);
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
		endpoints[i].handle = DSM_HANDLE_INVALID;
		endpoints[i].session_id = InvalidSession;
		endpoints[i].user_id = InvalidOid;
		endpoints[i].attach_status = Status_NotAttached;
		endpoints[i].empty = true;
		InitSharedLatch(&endpoints[i].ack_done);
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
		return ENDPOINT_ON_Entry_DB;
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
		case ENDPOINT_ON_Entry_DB:
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

void
WaitEndpointReady(const struct Plan *planTree, const char *cursorName)
{
	call_endpoint_udf_on_qd(planTree, cursorName, 'w');
}

/*
 *  Calling endpoint UDF on QD process, and it will dispatch the query to all the endpoints nodes basing on this
 *  parallel retrieve cursor plan. And return true if all endpoints return true.
 */
bool
call_endpoint_udf_on_qd(const struct Plan *planTree, const char *cursorName, char operator)
{
	bool ret_val = true;
	char cmd[255];
	List *cids;
	enum EndPointExecPosition endPointExecPosition;
	char *token_str = "";

	if (operator == 'w') {
		token_str = print_token(get_or_create_token_on_qd());
	}

	cids = ChooseEndpointContentIDForParallelCursor(
		planTree, &endPointExecPosition);

	if (endPointExecPosition == ENDPOINT_ON_Entry_DB)
	{
		ret_val = DatumGetBool(DirectFunctionCall3(gp_operate_endpoints_token, CharGetDatum(operator),
												   CStringGetDatum(token_str), CStringGetDatum(cursorName)));
	}
	else
	{
		CdbPgResults cdb_pgresults = {NULL, 0};

		snprintf(cmd, 255,
				 "select __gp_operate_endpoints_token('%c', '%s', '%s')", operator,
				 token_str, cursorName);
		if (endPointExecPosition == ENDPOINT_ON_ALL_QE)
		{
			/* Push token to all segments */
			CdbDispatchCommand(cmd, DF_CANCEL_ON_ERROR, &cdb_pgresults);
		}
		else
		{
			CdbDispatchCommandToSegments(cmd, DF_CANCEL_ON_ERROR, cids, &cdb_pgresults);
			list_free(cids);
		}

		for (int i = 0; i < cdb_pgresults.numResults; i++)
		{
			PGresult   *res = cdb_pgresults.pg_results[i];
			int			ntuples;
			int			nfields;
			bool        ret_val_seg = false; /*return value from segment UDF calling*/

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				char	   *msg = pstrdup(PQresultErrorMessage(res));
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							errmsg("could not get return value of UDF __gp_operate_endpoints_token() from segment"),
							errdetail("%s", msg)));
			}

			ntuples = PQntuples(res);
			nfields = PQnfields(res);
			if (ntuples != 1 || nfields != 1)
			{
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("unexpected result set received from __gp_operate_endpoints_token()"),
							errdetail("Result set expected to be 1 row and 1 column, but it had %d rows and %d columns",
									  ntuples, nfields)));
			}

			ret_val_seg = (strncmp(PQgetvalue(res, 0, 0), "t", 1) == 0);

			/* If any segment return false, then return false for this func*/
			if (!ret_val_seg)
			{
				ret_val = ret_val_seg;
				break;
			}
		}

		cdbdisp_clearCdbPgResults(&cdb_pgresults);
	}
	if (operator == 'w') {
		pfree(token_str);
	}
	return ret_val;
}

/*
 * Get or create a authentication token for current session.
 * Token is unique for every session id. This is guaranteed by using the session
 * id as a part of the token. And same session will have the same token. Thus the
 * retriever will know which session to attach when doing authentication.
 */
const int8 *
get_or_create_token_on_qd()
{
#ifdef HAVE_STRONG_RANDOM
	static int session_id						  = InvalidSession;
	static int8 current_token[ENDPOINT_TOKEN_LEN] = {0};
	static int session_id_len					  = sizeof(session_id);

	if (session_id != gp_session_id)
	{
		session_id = gp_session_id;
		memcpy(current_token, &session_id, session_id_len);
		if (!pg_strong_random(current_token + session_id_len,
							  ENDPOINT_TOKEN_LEN - session_id_len))
		{
			elog(ERROR, "Failed to generate a new random token.");
		}
	}
	return current_token;
#else
#error A strong random number source is needed.
#endif
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
	register_endpoint_callbacks();
	alloc_endpoint_for_cursor(cursorName);
	check_endpoint_allocated();

	currentMQEntry = MemoryContextAllocZero(TopMemoryContext, sizeof(MsgQueueStatusEntry));
	memcpy(currentMQEntry->endpoint_name, my_shared_endpoint->name, ENDPOINT_NAME_LEN);
	currentMQEntry->mq_seg = NULL;
	currentMQEntry->mq_handle = NULL;
	currentMQEntry->retrieve_status = RETRIEVE_STATUS_INIT;
	currentMQEntry->retrieve_ts = NULL;
	currentMQEntry->tq_reader = NULL;
	create_and_connect_mq(tupleDesc);
	declare_parallel_retrieve_ready();
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

	// instead.
	set_attach_status(Status_Finished);
	unset_endpoint_sender_pid(my_shared_endpoint);

	wait_parallel_retrieve_close();

	free_endpoint_by_cursor_name(EndpointCtl.cursor_name);
	my_shared_endpoint = NULL;
	ClearParallelCursorExecRole();
}

/*
 * AllocEndpointOfToken - Allocate an EndpointDesc entry in shared memroy.
 *
 * Find a free slot in DSM and set token and other info.
 */
void
alloc_endpoint_for_cursor(const char *cursorName)
{
	int i;
	int found_idx = -1;

	Assert(SharedEndpoints);
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */
	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedEndpoints[i].empty)
			{
				/* pretend to set a valid token */
				snprintf(SharedEndpoints[i].name, ENDPOINT_NAME_LEN, "%s", DUMMY_ENDPOINT_NAME);
				snprintf(SharedEndpoints[i].cursor_name, NAMEDATALEN, "%s", DUMMY_CURSOR_NAME);
				SharedEndpoints[i].database_id = MyDatabaseId;
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
			if (endpoint_name_equals(SharedEndpoints[i].name, DUMMY_ENDPOINT_NAME))
			{
				SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
				SharedEndpoints[i].empty = true;
			}
		}
	}
#endif

	/* find a new slot */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].empty)
		{
			found_idx = i;
			break;
		}
	}

	if (found_idx == -1)
		elog(ERROR, "failed to allocate endpoint");

	generate_endpoint_name(SharedEndpoints[i].name, cursorName, gp_session_id,
						   GpIdentity.segindex);
	snprintf(EndpointCtl.cursor_name, NAMEDATALEN, "%s", cursorName);
	memcpy(SharedEndpoints[i].cursor_name, cursorName, NAMEDATALEN);
	SharedEndpoints[i].database_id = MyDatabaseId;
	SharedEndpoints[i].session_id	= gp_session_id;
	SharedEndpoints[i].user_id		 = GetUserId();
	SharedEndpoints[i].sender_pid	= MyProcPid;
	SharedEndpoints[i].receiver_pid  = InvalidPid;
	InitSharedLatch(&SharedEndpoints[i].check_wait_latch);
	SharedEndpoints[i].attach_status = Status_NotAttached;
	SharedEndpoints[i].empty		 = false;
	OwnLatch(&SharedEndpoints[i].ack_done);
	my_shared_endpoint				 = &SharedEndpoints[i];

	LWLockRelease(ParallelCursorEndpointLock);
	/* track current session id for endpoint_exit_callback  */
	EndpointCtl.session_id = gp_session_id;
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
	Assert(Gp_role == GP_ROLE_EXECUTE);
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
	dsm_seg = dsm_create(toc_size, 0);
	if (dsm_seg == NULL)
	{
		LWLockRelease(ParallelCursorEndpointLock);
		sender_close();
		elog(ERROR, "failed to create shared message queue for send tuples.");
	}
	my_shared_endpoint->handle = dsm_segment_handle(dsm_seg);
	LWLockRelease(ParallelCursorEndpointLock);
	dsm_pin_mapping(dsm_seg);

	toc = shm_toc_create(create_magic_num_for_endpoint(my_shared_endpoint), dsm_segment_address(dsm_seg),
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

static void declare_parallel_retrieve_ready(void)
{
	SessionInfoEntry *info_entry = NULL;
	Latch *latch = NULL;
	SessionTokenTag tag;

	tag.sessionID = gp_session_id;
	tag.userID = GetUserId();

	info_entry = (SessionInfoEntry *) hash_search(
		SharedSessionInfoHash, &tag, HASH_FIND, NULL);

	/* The write gang is waiting on the latch. */
	if (info_entry && info_entry->init_wait_proc)
	{
		latch = &info_entry->init_wait_proc->procLatch;
		info_entry->init_wait_proc = NULL;
	}
	if (latch)
	{
		SetLatch(latch);
	}
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

		check_dispatch_connection();

		elog(DEBUG5, "CDB_ENDPOINT: sender wait latch in wait_receiver()");
		wr = WaitLatch(&my_shared_endpoint->ack_done,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_NORMAL_TIMEOUT);
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
        SetLatch(&endPointDesc->check_wait_latch);
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
		SetBackendCancelMessage(receiver_pid, "Signal the receiver to abort.");
		kill(receiver_pid, SIGINT);
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
	ClearParallelCursorExecRole();
}

void
wait_parallel_retrieve_close(void)
{
	ResetLatch(&MyProc->procLatch);
	while (true)
	{
		int wr;

		CHECK_FOR_INTERRUPTS();

		if (QueryFinishPending)
			break;

		check_dispatch_connection();

		elog(DEBUG5, "CDB_ENDPOINT: wait for parallel retrieve cursor close");
		wr = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_NORMAL_TIMEOUT);
		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT: parallel retrieve cursor close, get latch");
		ResetLatch(&MyProc->procLatch);
		break;
	}
}

/*
 * Find and free the EndpointDesc entry by the cursor name.
 */
void
free_endpoint_by_cursor_name(const char *cursorName)
{
	EndpointDesc *endpointDesc = find_endpoint_by_cursor_name(cursorName, true);

	if (!endpointDesc || endpointDesc->empty)
	{
		elog(LOG, "Can not find endpoint for parallel retrieve cursor: %s", cursorName);
		return;
	}
	elog(DEBUG3, "CDB_ENDPOINTS: Free endpoint '%s'.", endpointDesc->name);

	unset_endpoint_sender_pid(endpointDesc);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	endpointDesc->database_id = InvalidOid;
	endpointDesc->handle = DSM_HANDLE_INVALID;
	endpointDesc->session_id = InvalidSession;
	endpointDesc->user_id = InvalidOid;
	endpointDesc->empty = true;
	invalidate_endpoint_name(endpointDesc->name);
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Register callback for endpoint/sender to deal with xact abort.
 */
static void
register_endpoint_callbacks(void)
{
	static bool is_registered = false;

	if (!is_registered)
	{
		/* Register endpoint_exit_callback only for QD/QE processes */
		before_shmem_exit(endpoint_exit_callback, (Datum) 0);
		// Register callback to deal with proc endpoint xact abort.
		RegisterSubXactCallback(sender_subxact_callback, NULL);
		RegisterXactCallback(sender_xact_abort_callback, NULL);
		is_registered = true;
	}
}

/*
 * endpoint_exit_callback - callback when endpoint backend process exit
 *
 * When an endpoint backend process exit, we need to clean "session - token" mapping
 * entry in shared memory if there's no endpoint in current entry.
 * Cause when session id wrapped, we don't want old token exists in shared memory
 * for same session id(which is actually a new session id). Cause this may lead to
 * use old token to auth for new session.
 */
static void
endpoint_exit_callback(int code, Datum arg)
{
	if (!(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE))
		return;
	bool endpoint_exists = false;
	elog(DEBUG3, "CDB_ENDPOINT: endpoint_exit_callback session %d.", EndpointCtl.session_id);

	/* Remove session info entry if current session don't have any exists EndpointDesc slot */
	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
	{
		if (SharedEndpoints[i].session_id == EndpointCtl.session_id)
		{
			endpoint_exists = true;
			break;
		}
	}
	if (!endpoint_exists)
	{
		elog(LOG, "CDB_ENDPOINT: endpoint_exit_callback clean token for session %d", EndpointCtl.session_id);
		SessionInfoEntry *entry;
		SessionTokenTag tag;

		tag.sessionID = EndpointCtl.session_id;
		tag.userID = GetUserId();
		entry = hash_search(SharedSessionInfoHash, &tag, HASH_REMOVE, NULL);
		if (!entry)
			elog(LOG, "CDB_ENDPOINT: endpoint_exit_callback no entry exists for user id: %d, session: %d",
				 tag.userID, EndpointCtl.session_id);
	}
	LWLockRelease(ParallelCursorEndpointLock);
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

EndpointDesc*
get_endpointdesc_by_index(int index)
{
	Assert(SharedEndpoints);
	Assert(index > -1 && index < MAX_ENDPOINT_SIZE);
	return &SharedEndpoints[index];
}

/*
 * Find the endpoint by given endpoint name and session id.
 * For the sender, the session_id is the gp_session_id since it is the same with
 * the session which created the parallel retrieve cursor.
 * For the retriever, the session_id is picked by the token when doing the
 * authentication.
 *
 * The caller is responsible for acquiring ParallelCursorEndpointLock lock.
 */
EndpointDesc *
find_endpoint(const char *endpointName, int sessionID)
{
	EndpointDesc *res = NULL;

	Assert(endpointName);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		// endpoint_name is unique across sessions. But it is not right, we need
		// to find the endpoint created by in the session with the given
		// session_id.
		if (!SharedEndpoints[i].empty && SharedEndpoints[i].session_id == sessionID &&
			endpoint_name_equals(SharedEndpoints[i].name, endpointName) &&
			SharedEndpoints[i].database_id == MyDatabaseId)
		{
			res = &SharedEndpoints[i];
			break;
		}
	}

	return res;
}

const int8 *
get_token_by_session_id(int sessionId, Oid userID)
{
	SessionInfoEntry *info_entry = NULL;
	SessionTokenTag tag;

	tag.sessionID = sessionId;
	tag.userID = userID;

	info_entry = (SessionInfoEntry *) hash_search(
		SharedSessionInfoHash, &tag, HASH_FIND, NULL);
	if (info_entry == NULL)
	{
		elog(ERROR, "Token for user id: %d, session: %d doesn't exist",
			 tag.userID, sessionId);
	}
	return info_entry->token;
}

/*
 * Find the corresponding session id by the given token.
 */
int
get_session_id_for_auth(Oid userID, const int8 *token)
{
	int session_id = InvalidSession;
	SessionInfoEntry *info_entry = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, SharedSessionInfoHash);
	while ((info_entry = (SessionInfoEntry *) hash_seq_search(&status)) != NULL)
	{
		if (token_equals(info_entry->token, token) && userID == info_entry->tag.userID)
		{
			session_id = info_entry->tag.sessionID;
			hash_seq_term(&status);
			break;
		}
	}

	return session_id;
}

void
check_endpoint_allocated(void)
{
	if (EndpointCtl.Gp_prce_role != PRCER_SENDER)
		elog(ERROR, "%s could not check endpoint allocated status",
			 endpoint_role_to_string(EndpointCtl.Gp_prce_role));

	if (!my_shared_endpoint)
		elog(ERROR, "endpoint for cursor %s is not allocated", EndpointCtl.cursor_name);
}

/*
 * Generate the endpoint name based on the PARALLEL RETRIEVE CURSOR name,
 * session ID and the segment index.
 * The endpoint name should be unique across sessions.
 */
void generate_endpoint_name(char *name,
							const char *cursorName, int32 sessionID, int32 segindex)
{
	snprintf(name, ENDPOINT_NAME_LEN, "%s_%08x_%08x", cursorName,
			 sessionID, segindex);
}

/*
 * Find the EndpointDesc entry by the given cursor name in current session.
 */
EndpointDesc * find_endpoint_by_cursor_name(const char *cursor_name, bool with_lock)
{
	EndpointDesc *res = NULL;

	if (with_lock)
		LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!SharedEndpoints[i].empty && SharedEndpoints[i].session_id == gp_session_id &&
			strncmp(SharedEndpoints[i].cursor_name, cursor_name, NAMEDATALEN) == 0)
		{
			res = &SharedEndpoints[i];
		}
	}

	if (with_lock)
		LWLockRelease(ParallelCursorEndpointLock);

	return res;
}

static void
set_attach_status(enum AttachStatus status)
{
	if (EndpointCtl.Gp_prce_role != PRCER_SENDER)
		elog(ERROR, "%s could not set endpoint", endpoint_role_to_string(EndpointCtl.Gp_prce_role));

	if (!my_shared_endpoint && !my_shared_endpoint->empty)
		elog(ERROR, "endpoint doesn't exist");

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	my_shared_endpoint->attach_status = status;

	LWLockRelease(ParallelCursorEndpointLock);
}

void
check_dispatch_connection(void)
{
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
    bool ret_val = false;
	char operation;
	const char *token_str = NULL;
	const char *cursor_name = NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	operation = PG_GETARG_CHAR(0);
	token_str = PG_GETARG_CSTRING(1);
	cursor_name = PG_GETARG_CSTRING(2);

    switch (operation)
    {
        case 'c':
            ret_val = check_endpoint_finished_by_cursor_name(cursor_name, false);
            break;
        case 'h':
            ret_val = check_endpoint_finished_by_cursor_name(cursor_name, true);
            break;
        case 'w':
            wait_for_init_by_cursor_name(cursor_name, token_str);
            ret_val = true;
            break;
        default:
            elog(ERROR, "Failed to execute gp_operate_endpoints_token('%c', '%s')", operation, token_str);
            ret_val = false;
    }

	PG_RETURN_BOOL(ret_val);
}

/*
 * Check if the endpoint has finished retrieving data
 *
 * This func is called in UDF in the write gang. 
 * 
 * In NOWAIT mode, this func should not report any error about the endpoint info or endpoint QE 
 * execute error, because if the write gang error, it need time to handle cancel, and report error 
 * to QD, when next statement comes, it will report error at this time, it make the error message 
 * misunderstanding (User may think that the endpoint backend of the latter cursor issues error). 
 * Also it also issues 2 error messages (one is from endpoint backend, the other is from the 
 * related write gang process). So we need to check query dispatcher (which dispatch the
 * orginal query of the parallel retrieve cursor) in addition in case of any error issues.
 * 
 * In WAIT mode, we cannot use the same way as NOWAIT mode, because if one of the endpoint backend reports
 * error, then the query dispacher (which dispatch this UDF) will not return because other endpoints still
 * waiting for retrieving session. So just report error in this func, although still 2 error messages issues,
 * but the error is regarded as this statement's error (because there is no other statement running at the 
 * same session)
 * 
 * Parameters:
 * - cursorName: specify the cursor this endpoint belongs to.
 * - isWait: wait until finished or not
 *
 * Return Value: 
 *   whether this endpoint finished or not.
 */
bool
check_endpoint_finished_by_cursor_name(const char *cursorName, bool isWait)
{
	bool isFinished = false;
	EndpointDesc *endpointDesc = find_endpoint_by_cursor_name(cursorName, true);

	if (endpointDesc == NULL)
	{
	    if(isWait){
            elog(ERROR, "Endpoint doesn't exist.");
	    }else{
            /* if no endpoint found, it maybe something wrong, just return false, i.e. not finished successfully. */
            return false;
	    }
	}

	if (endpointDesc->user_id != GetUserId())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("The PARALLEL RETRIEVE CURSOR was created by a different user."),
					errhint("Using the same user as the PARALLEL RETRIEVE CURSOR creator.")));
	}
	isFinished = endpointDesc->attach_status == Status_Finished;
	if(isWait && !isFinished){
		elog(LOG, "CDB_ENDPOINT: WaitLatch on endpointDesc->check_wait_latch by pid: %d", MyProcPid);
		while (true)
		{
			int wr;
			CHECK_FOR_INTERRUPTS();

			if (QueryFinishPending)
				break;
			wr = WaitLatch(&endpointDesc->check_wait_latch,
						   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
						   WAIT_NORMAL_TIMEOUT);
			if (wr & WL_TIMEOUT)
				continue;
			if (wr & WL_POSTMASTER_DEATH)
			{
				proc_exit(0);
			}
			break;
		}
		if (endpointDesc->empty){
            /* if the endpoint backend has something wrong, report error because in wait mode, otherwise it 
			 * will hang at this UDF query dispatch.
			 */
            elog(ERROR, "Endpoint for '%s' get aborted.", cursorName);
            return false;
		}
		isFinished = endpointDesc->attach_status == Status_Finished;
	}
	return isFinished;
}

/*
 * Waits until the QE is ready -- the dest receiver will be ready after this
 * function returns successfully.
 */
void
wait_for_init_by_cursor_name(const char *cursorName, const char *tokenStr)
{
	EndpointDesc *desc			 = NULL;
	SessionInfoEntry *info_entry = NULL;
	bool found					 = false;
	Latch *latch				 = NULL;
	int wr						 = 0;
	SessionTokenTag   tag;

	tag.sessionID = gp_session_id;
	tag.userID = GetUserId();

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	info_entry = (SessionInfoEntry *) hash_search(
		SharedSessionInfoHash, &tag, HASH_ENTER, &found);

	elog(DEBUG3, "CDB_ENDPOINT: Finish endpoint init. Found SessionInfoEntry: %d",
		 found);
	/* Save the token if it is the first time we create endpoint in current
	 * session. We guarantee that one session will map to one token only.*/
	if (!found)
	{
		info_entry->init_wait_proc = NULL;
	}
	/* Overwrite exists token in case the wrapped session id entry not get removed
	 * For example, 1 hours ago, a session 7 exists and have entry with token 123.
	 * And for some reason the entry not get remove by endpoint_exit_callback.
	 * Now current session is session 7 again. Here need to overwrite the old token. */
	parse_token(info_entry->token, tokenStr);
	elog(DEBUG3, "CDB_ENDPOINT: set new token %s, for session %d", tokenStr, gp_session_id);

	desc = find_endpoint_by_cursor_name(cursorName, false);
	/* If the endpoints have been created and attach status is prepared, just return.
	 * Otherwise, set the init_wait_proc for current session and wait on the
	 * latch.*/
	if (!desc || desc->attach_status != Status_Prepared)
	{
		info_entry->init_wait_proc = MyProc;
		latch					   = &MyProc->procLatch;
		ResetLatch(latch);
	}

	LWLockRelease(ParallelCursorEndpointLock);

	if (latch)
	{
		wr = WaitLatch(latch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_ENDPOINT_INIT_TIMEOUT);
		ResetLatch(latch);
	}

	if (wr & WL_TIMEOUT)
	{
		elog(ERROR, "Creating endpoint timeout");
	}

	if (wr & WL_POSTMASTER_DEATH)
	{
		elog(DEBUG3, "CDB_ENDPOINT: Postmaster exit.");
		proc_exit(1);
	}

	if (desc == NULL)
	{
		desc = find_endpoint_by_cursor_name(cursorName, true);
	}
	OwnLatch(&desc->check_wait_latch);
	elog(LOG, "CDB_ENDPOINT: OwnLatch on endpointDesc->check_wait_latch by pid: %d", desc->check_wait_latch.owner_pid);
}

Datum
gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
    const char *cursor_name = NULL;
    cursor_name = PG_GETARG_CSTRING(0);

    PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursor_name, false));
}

Datum
gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
    const char *cursor_name = NULL;
    cursor_name = PG_GETARG_CSTRING(0);

    PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursor_name, true));
}

bool
check_parallel_retrieve_cursor(const char *cursorName, bool isWait)
{
	bool ret_val = false;
	bool is_parallel_retrieve = false;
	Portal		portal;

	/* get the portal from the portal name */
	portal = GetPortalByName(cursorName);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
					errmsg("cursor \"%s\" does not exist", cursorName)));
		return false;					/* keep compiler happy */
	}
	is_parallel_retrieve = (portal->cursorOptions & CURSOR_OPT_PARALLEL_RETRIEVE) > 0 ;
	if (!is_parallel_retrieve)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("This UDF only works for PARALLEL RETRIEVE CURSOR.")));
		return false;
	}

	/** See commens at check_endpoint_finished_by_cursor_name()
	 *  
	 * In NOWAIT mode, need to check the query dispatcher for the orginal query of the parallel 
	 * retrieve cursor, because the UDF will not report error.
	 * 
	 * In WAIT mode, the UDF will report error, don't need to check the query dispatcher.
	 */
	PlannedStmt* stmt = (PlannedStmt *) linitial(portal->stmts);
	ret_val = call_endpoint_udf_on_qd(stmt->planTree, cursorName, isWait ? 'h' : 'c');

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("check_parallel_retrieve_cursor_after_udf");
	RESUME_INTERRUPTS();
#endif

	if(!isWait && !ret_val)
		check_parallel_cursor_errors(portal->queryDesc);
	return ret_val;
}

/*
 * Check the PARALLEL RETRIEVE CURSOR execution status, if get error, then rethrow the error
 *
 * isWait:  support 2 modes - WAIT/NOWAIT
 * @return true if the PARALLEL RETRIEVE CURSOR Execution Finished
 */
bool
check_parallel_cursor_errors(QueryDesc *queryDesc)
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
		CdbDispatcherState *ds = estate->dispatcherState;
		if (cdbdisp_checkForCancel(ds))
		{
			ErrorData *qeError = NULL;
			cdbdisp_getDispatchResults(ds, &qeError);
			Assert(qeError);
			estate->dispatcherState = NULL;
			cdbdisp_cancelDispatch(ds);
			cdbdisp_destroyDispatcherState(ds);
			ReThrowError(qeError);
		}
		isParallelRetrCursorFinished = cdbdisp_isDispatchFinished(ds);
	}
	return isParallelRetrCursorFinished;
}
