/*
 * cdbendpoint.c
 *
 * When define and execute a PARALLEL RETRIEVE CURSOR, the results are written to
 * endpoints.
 *
 * Endpoint may exist on master or segments, depends on the query of the PARALLEL
 * RETRIEVE CURSOR: (1) An endpoint is on QD only if the query of the parallel
 *     cursor needs to be finally gathered by the master. e.g.
 *     > CREATE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 ORDER BY C1;
 * (2) The endpoints are on specific segments node if the direct dispatch happens.
 * e.g. > CREATE c1 PARALLEL RETRIEVE CURSOR FOR SELECT * FROM T1 WHERE C1=1 OR
 * C1=2; (3) The endpoints are on all segments node. e.g. > CREATE c1 PARALLEL
 * RETRIEVE CURSOR FOR SELECT * FROM T1;
 *
 * When QE or QD write results to endpoint, it will replace normal dest receiver
 * with TQueueDestReceiver, so that the query results will be write into a shared
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

#include <utils/portal.h>

#include "access/tupdesc.h"
#include "access/xact.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdbendpointinternal.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "utils/backend_cancel.h"
#include "utils/elog.h"
#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/* The timeout before returns failure for endpoints initialization. */
#define WAIT_ENDPOINT_INIT_TIMEOUT      5000
#define WAIT_NORMAL_TIMEOUT             300
/* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */
#define ENDPOINT_TUPLE_QUEUE_SIZE       65536

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

	/* We implement two UDFs for below purpose:
	 * 1: During 'DECLARE PARALLEL RETRIEVE CURSOR',
	 *    dispatch a UDF to wait until all endpoints are ready.
	 * 2: After 'DECLARE PARALLEL RETRIEVE CURSOR', user can execute
	 * 'gp_wait_parallel_retrieve_cursor' to wait until all endpoints finished.
	 * (For track status purpose) So we create the latch and endpoint set latch at
	 * right moment.
	 *
	 * cursorName is used to track which 'PARALLEL RETRIEVE CURSOR' to set latch.
	 */
	Latch udf_check_latch;
	char  cursorName[NAMEDATALEN];

	/* The auth token for this session. */
	int8 token[ENDPOINT_TOKEN_LEN];
} SessionInfoEntry;

/* Point to EndpointDesc entries in shared memory */
static EndpointDesc *sharedEndpoints = NULL;
/* Shared hash table for session infos */
static HTAB *sharedSessionInfoHash = NULL;

/* Current EndpointDesc entry for sender.
 * It is set when create dest receiver and unset when destroy it. */
static volatile EndpointDesc *activeSharedEndpoint = NULL;
/* Current dsm_segment pointer, saved it for detach. */
static dsm_segment *activeDsmSeg = NULL;

/* Init helper functions */
static void init_shared_endpoints(void *address);

/* QD utility functions */
static bool		   call_endpoint_udf_on_qd(const struct Plan *planTree,
										   const char *cursorName, char operator);
static const int8 *get_or_create_token_on_qd(void);

/* Endpoint helper function */
static EndpointDesc *alloc_endpoint(const char *cursorName, dsm_handle dsmHandle);
static void			 free_endpoint(volatile EndpointDesc *endpoint);
static void			 create_and_connect_mq(TupleDesc	   tupleDesc,
										   dsm_segment **  mqSeg /*out*/,
										   shm_mq_handle **mqHandle /*out*/);
static void			 detach_mq(dsm_segment *dsmSeg);
static void			 declare_parallel_retrieve_ready(const char *cursorName);
static void			 wait_receiver(void);
static void unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc);
static void signal_receiver_abort(pid_t				receiverPid,
								  enum AttachStatus attachStatus);
static void endpoint_abort(void);
static void wait_parallel_retrieve_close(void);
static void register_endpoint_callbacks(void);
static void sender_xact_abort_callback(XactEvent ev, void *vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg);

/* utility */
static void			 generate_endpoint_name(char *name, const char *cursorName,
											int32 sessionID, int32 segindex);
static EndpointDesc *find_endpoint_by_cursor_name(const char *name,
												  bool		  with_lock);
static void			 check_dispatch_connection(void);

/* Endpoints internal operation UDF's helper function */
static void register_session_info_callback(void);
static void session_info_clean_callback(XactEvent ev, void *vp);
static bool check_endpoint_finished_by_cursor_name(const char *cursorName,
												   bool		   isWait);
static void wait_for_init_by_cursor_name(const char *cursorName,
										 const char *tokenStr);
static bool check_parallel_retrieve_cursor(const char *cursorName, bool isWait);
static bool check_parallel_cursor_errors(QueryDesc *queryDesc);

/*
 * Endpoint_ShmemSize - Calculate the shared memory size for PARALLEL RETRIEVE
 * CURSOR execute.
 *
 * The size contains LWLocks and EndpointSharedCTX.
 */
Size
EndpointShmemSize(void)
{
	Size size;
	size = MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc)));
	size = add_size(
		size, hash_estimate_size(MAX_ENDPOINT_SIZE, sizeof(SessionInfoEntry)));
	return size;
}

/*
 * Endpoint_CTX_ShmemInit - Init shared memory structure for PARALLEL RETRIEVE
 * CURSOR execute.
 */
void
EndpointCTXShmemInit(void)
{
	bool	is_shmem_ready;
	HASHCTL hctl;

	sharedEndpoints = (EndpointDesc *) ShmemInitStruct(
		SHMEM_ENDPOINTS_ENTRIES,
		MAXALIGN(mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc))),
		&is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);
	if (!is_shmem_ready)
	{
		init_shared_endpoints(sharedEndpoints);
	}

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize   = sizeof(SessionTokenTag);
	hctl.entrysize = sizeof(SessionInfoEntry);
	hctl.hash	  = tag_hash;
	sharedSessionInfoHash =
		ShmemInitHash(SHMEM_ENPOINTS_SESSION_INFO, MAX_ENDPOINT_SIZE,
					  MAX_ENDPOINT_SIZE, &hctl, HASH_ELEM | HASH_FUNCTION);
}

/*
 * Init EndpointDesc entries.
 */
static void
init_shared_endpoints(void *address)
{
	Endpoint endpoints = (Endpoint) address;

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		endpoints[i].database_id   = InvalidOid;
		endpoints[i].sender_pid	   = InvalidPid;
		endpoints[i].receiver_pid  = InvalidPid;
		endpoints[i].mq_dsm_handle = DSM_HANDLE_INVALID;
		endpoints[i].session_id	   = InvalidSession;
		endpoints[i].user_id	   = InvalidOid;
		endpoints[i].attach_status = Status_Invalid;
		endpoints[i].empty		   = true;
		InitSharedLatch(&endpoints[i].ack_done);
	}
}

/*
 * GetParallelCursorEndpointPosition - get PARALLEL RETRIEVE CURSOR endpoint
 * allocate position
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
 * ChooseEndpointContentIDForParallelCursor - choose endpoints position base on
 * plan.
 *
 * Base on different condition, we need figure out which
 * segments that endpoints should be allocated in.
 */
List *
ChooseEndpointContentIDForParallelCursor(const struct Plan		   *planTree,
										 enum EndPointExecPosition *position)
{
	List *cids = NIL;
	*position  = GetParallelCursorEndpointPosition(planTree);
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
			foreach (cell, planTree->directDispatch.contentIds)
			{
				int contentid = lfirst_int(cell);
				cids		  = lappend_int(cids, contentid);
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
 * Calling endpoint UDF on QD process, and it will dispatch the query to all the
 * endpoints nodes basing on this parallel retrieve cursor plan. And return true
 * if all endpoints return true.
 */
bool
call_endpoint_udf_on_qd(const struct Plan *planTree, const char *cursorName,
						char operator)
{
	bool					  ret_val = true;
	char					  cmd[255];
	List					  *cids;
	enum EndPointExecPosition endPointExecPosition;
	char 					  *token_str = "";

	if (operator == 'w')
	{
		token_str = print_token(get_or_create_token_on_qd());
	}

	cids =
		ChooseEndpointContentIDForParallelCursor(planTree, &endPointExecPosition);

	if (endPointExecPosition == ENDPOINT_ON_Entry_DB)
	{
		ret_val = DatumGetBool(DirectFunctionCall3(
			gp_operate_endpoints_token, CharGetDatum(operator),
			CStringGetDatum(token_str), CStringGetDatum(cursorName)));
	}
	else
	{
		CdbPgResults cdb_pgresults = {NULL, 0};

		snprintf(
			cmd, 255,
			"select __gp_operate_endpoints_token('%c', '%s', '%s')", operator,
			token_str, cursorName);
		if (endPointExecPosition == ENDPOINT_ON_ALL_QE)
		{
			/* Push token to all segments */
			CdbDispatchCommand(cmd, DF_CANCEL_ON_ERROR, &cdb_pgresults);
		}
		else
		{
			CdbDispatchCommandToSegments(cmd, DF_CANCEL_ON_ERROR, cids,
										 &cdb_pgresults);
			list_free(cids);
		}

		for (int i = 0; i < cdb_pgresults.numResults; i++)
		{
			PGresult *res = cdb_pgresults.pg_results[i];
			int		  ntuples;
			int		  nfields;
			bool ret_val_seg = false; /*return value from segment UDF calling*/

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				char *msg = pstrdup(PQresultErrorMessage(res));
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("could not get return value of UDF "
								"__gp_operate_endpoints_token() from segment"),
						 errdetail("%s", msg)));
			}

			ntuples = PQntuples(res);
			nfields = PQnfields(res);
			if (ntuples != 1 || nfields != 1)
			{
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unexpected result set received from "
								"__gp_operate_endpoints_token()"),
						 errdetail("Result set expected to be 1 row and 1 "
								   "column, but it had %d rows and %d columns",
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
	if (operator == 'w')
	{
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
	static int		 session_id						   = InvalidSession;
	static int8		 current_token[ENDPOINT_TOKEN_LEN] = {0};
	const static int session_id_len					   = sizeof(session_id);

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
 * CreateTQDestReceiverForEndpoint - Creates a dest receiver for PARALLEL RETRIEVE
 * CURSOR
 *
 * Also creates shared memory message queue here. Set the local
 * Create TupleQueueDestReceiver base on the message queue to pass tuples to
 * retriever.
 */
DestReceiver *
CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc, const char *cursorName)
{
	shm_mq_handle *mq_handle;

	Assert(!activeSharedEndpoint);
	Assert(!activeDsmSeg);
	Assert(EndpointCtl.Gp_prce_role == PRCER_SENDER);

	/* Register callback to deal with proc exit. */
	register_endpoint_callbacks();
	/* The message queue needs to be created first since the dsm_handle
	 * has to be ready when create EndpointDesc entry. */
	create_and_connect_mq(tupleDesc, &activeDsmSeg, &mq_handle);
	/* Alloc endpoint and set it as the active one for sender.
	 * Once the endpoint has been created in shared memory, write gang
	 * can return from waiting for declare UDF and the message queue is
	 * ready to retrieve.*/
	activeSharedEndpoint =
		alloc_endpoint(cursorName, dsm_segment_handle(activeDsmSeg));

	/* Unblock the latch to finish declare statement. */
	declare_parallel_retrieve_ready(cursorName);
	return CreateTupleQueueDestReceiver(mq_handle);
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
	Assert(activeSharedEndpoint);
	Assert(activeDsmSeg);
	/* wait for receiver to retrieve the first row.
	 * ack_done latch will be reset to be re-used when retrieving finished. */
	wait_receiver();

	/* tqueueShutdownReceiver() (rShutdown callback) will call shm_mq_detach(),
	 * so need to call it before detach_mq().
	 * Retrieving session will set ack_done latch again after shm_mq_detach()
	 * called here. */
	(*endpointDest->rShutdown)(endpointDest);
	(*endpointDest->rDestroy)(endpointDest);

	/* Wait until all data is retrieved by receiver.
	 * This is needed because when endpoint send all data to shared message queue.
	 * The retrieve session may still not get all data from */
	wait_receiver();

	unset_endpoint_sender_pid(activeSharedEndpoint);

	/* If all data get sent, hang the process and wait for QD to close it.
	 * The purpose is to not clean up EndpointDesc entry until
	 * CLOSE/COMMIT/ABORT (i.e. ProtalCleanup get executed).
	 * So user can still see the finished endpoint status through
	 * gp_endpoints_info UDF. This is needed because pg_cusor view can still see
	 * the PARALLEL RETRIEVE CURSOR */
	wait_parallel_retrieve_close();

	free_endpoint(activeSharedEndpoint);
	activeSharedEndpoint = NULL;
	detach_mq(activeDsmSeg);
	activeDsmSeg = NULL;
	ClearParallelCursorExecRole();
}

/*
 * AllocEndpointOfToken - Allocate an EndpointDesc entry in shared memroy.
 *
 * cursorName - the parallel retrieve cursor name.
 * dsmHandle  - dsm handle of shared memory message queue.
 */
EndpointDesc *
alloc_endpoint(const char *cursorName, dsm_handle dsmHandle)
{
	int			  i;
	int			  found_idx = -1;
	EndpointDesc *ret		= NULL;

	Assert(sharedEndpoints);
	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */
	FaultInjectorType_e typeE =
		SIMPLE_FAULT_INJECTOR("endpoint_shared_memory_slot_full");

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (sharedEndpoints[i].empty)
			{
				/* pretend to set a valid token */
				snprintf(sharedEndpoints[i].name, ENDPOINT_NAME_LEN, "%s",
						 DUMMY_ENDPOINT_NAME);
				snprintf(sharedEndpoints[i].cursor_name, NAMEDATALEN, "%s",
						 DUMMY_CURSOR_NAME);
				sharedEndpoints[i].database_id   = MyDatabaseId;
				sharedEndpoints[i].mq_dsm_handle = DSM_HANDLE_INVALID;
				sharedEndpoints[i].session_id	= gp_session_id;
				sharedEndpoints[i].user_id		 = GetUserId();
				sharedEndpoints[i].sender_pid	= InvalidPid;
				sharedEndpoints[i].receiver_pid  = InvalidPid;
				sharedEndpoints[i].empty		 = false;
			}
		}
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
	{
		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (endpoint_name_equals(sharedEndpoints[i].name,
									 DUMMY_ENDPOINT_NAME))
			{
				sharedEndpoints[i].mq_dsm_handle = DSM_HANDLE_INVALID;
				sharedEndpoints[i].empty		 = true;
			}
		}
	}
#endif

	/* find a new slot */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (sharedEndpoints[i].empty)
		{
			found_idx = i;
			break;
		}
	}

	if (found_idx == -1)
		elog(ERROR, "failed to allocate endpoint");

	generate_endpoint_name(sharedEndpoints[i].name, cursorName, gp_session_id,
						   GpIdentity.segindex);
	StrNCpy(sharedEndpoints[i].cursor_name, cursorName, NAMEDATALEN);
	sharedEndpoints[i].database_id   = MyDatabaseId;
	sharedEndpoints[i].session_id	 = gp_session_id;
	sharedEndpoints[i].user_id		 = GetUserId();
	sharedEndpoints[i].sender_pid	 = MyProcPid;
	sharedEndpoints[i].receiver_pid  = InvalidPid;
	sharedEndpoints[i].attach_status = Status_Prepared;
	sharedEndpoints[i].empty		 = false;
	sharedEndpoints[i].mq_dsm_handle = dsmHandle;
	OwnLatch(&sharedEndpoints[i].ack_done);
	ret = &sharedEndpoints[i];

	LWLockRelease(ParallelCursorEndpointLock);
	return ret;
}

/*
 * Create and setup the shared memory message queue.
 *
 * Create a dsm which contains a TOC(table of content). It has 3 parts:
 * 1. Tuple's TupleDesc length.
 * 2. Tuple's TupleDesc.
 * 3. Shared memory message queue.
 */
void
create_and_connect_mq(TupleDesc tupleDesc, dsm_segment **mqSeg /*out*/,
					  shm_mq_handle **mqHandle /*out*/)
{
	shm_toc *		  toc;
	shm_mq *		  mq;
	shm_toc_estimator toc_est;
	Size			  toc_size;
	int				  tupdesc_len;
	char *			  tupdesc_ser;
	char *			  tdlen_space;
	char *			  tupdesc_space;
	TupleDescNode *   node = makeNode(TupleDescNode);

	Assert(Gp_role == GP_ROLE_EXECUTE);

	elog(DEBUG3,
		 "CDB_ENDPOINTS: create and setup the shared memory message queue.");

	/* Serialize TupleDesc */
	node->natts = tupleDesc->natts;
	node->tuple = tupleDesc;
	tupdesc_ser =
		serializeNode((Node *) node, &tupdesc_len, NULL /* uncompressed_size */);

	/*
	 * Calculate dsm size, size = toc meta + toc_nentry(3) * entry size + tuple
	 * desc length size + tuple desc size + queue size.
	 */
	shm_toc_initialize_estimator(&toc_est);
	shm_toc_estimate_chunk(&toc_est, sizeof(tupdesc_len));
	shm_toc_estimate_chunk(&toc_est, tupdesc_len);
	shm_toc_estimate_keys(&toc_est, 2);

	shm_toc_estimate_chunk(&toc_est, ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_estimate_keys(&toc_est, 1);
	toc_size = shm_toc_estimate(&toc_est);

	*mqSeg = dsm_create(toc_size, 0);
	if (*mqSeg == NULL)
	{
		elog(ERROR, "failed to create shared message queue for send tuples.");
	}
	dsm_pin_mapping(*mqSeg);

	toc = shm_toc_create(ENDPOINT_MSG_QUEUE_MAGIC, dsm_segment_address(*mqSeg),
						 toc_size);

	tdlen_space = shm_toc_allocate(toc, sizeof(tupdesc_len));
	memcpy(tdlen_space, &tupdesc_len, sizeof(tupdesc_len));
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC_LEN, tdlen_space);

	tupdesc_space = shm_toc_allocate(toc, tupdesc_len);
	memcpy(tupdesc_space, tupdesc_ser, tupdesc_len);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_DESC, tupdesc_space);

	mq = shm_mq_create(shm_toc_allocate(toc, ENDPOINT_TUPLE_QUEUE_SIZE),
					   ENDPOINT_TUPLE_QUEUE_SIZE);
	shm_toc_insert(toc, ENDPOINT_KEY_TUPLE_QUEUE, mq);
	shm_mq_set_sender(mq, MyProc);
	*mqHandle = shm_mq_attach(mq, *mqSeg, NULL);
}

/*
 * declare_parallel_retrieve_ready
 *
 * After endpoint is ready for retrieve, tell WaitEndpointReady
 * to finish wait and then DECLARE statement could finish.
 */
void
declare_parallel_retrieve_ready(const char *cursorName)
{
	SessionInfoEntry *info_entry = NULL;
	Latch *			  latch		 = NULL;
	SessionTokenTag   tag;

	tag.sessionID = gp_session_id;
	tag.userID	= GetUserId();

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	info_entry = (SessionInfoEntry *) hash_search(sharedSessionInfoHash, &tag,
												  HASH_FIND, NULL);

	/* The write gang is waiting on the latch. */
	if (info_entry)
	{
		latch = &info_entry->udf_check_latch;
	}
	/* Here compare the cursorName in SessionInfoEntry so endpoint process knows
	 * whether is the right time to set latch to tell WaitEndpointReady
	 * endpoint is ready for retrieve, DECLARE statement can finish.
	 *
	 * Must make sure the cursorName is set before current function.
	 * This is implemented by acquire LW_EXCLUSIVE EndpointLock when set
	 * cursorName in check_endpoint_finished_by_cursor_name function. */
	if (latch && strncmp(info_entry->cursorName, cursorName, NAMEDATALEN) == 0)
	{
		SetLatch(latch);
	}
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * wait_receiver - wait receiver to retrieve at least once from the
 * shared memory message queue.
 *
 * If the queue only attached by the sender and the queue is large enough
 * for all tuples, sender should wait receiver. Cause if sender detached
 * from the queue, the queue will be not available for receiver.
 */
void
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
		wr = WaitLatch(&activeSharedEndpoint->ack_done,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_NORMAL_TIMEOUT);
		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			endpoint_abort();
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory "
						 "message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT:sender reset latch in wait_receiver()");
		ResetLatch(&activeSharedEndpoint->ack_done);
		break;
	}
}

/*
 * Detach the shared memory message queue.
 * This should happen after free endpoint, otherwise endpoint->mq_dsm_handle
 * becomes invalid pointer.
 */
void
detach_mq(dsm_segment *dsmSeg)
{
	elog(DEBUG3, "CDB_ENDPOINT: Sender message queue detaching. '%p'",
		 (void *) dsmSeg);

	Assert(dsmSeg);
	dsm_detach(dsmSeg);
}

/*
 * Unset endpoint sender pid.
 *
 * Clean the EndpointDesc entry sender pid when endpoint finish it's
 * job or abort.
 */
void
unset_endpoint_sender_pid(volatile EndpointDesc *endPointDesc)
{
	SessionInfoEntry *sessionInfoEntry;
	SessionTokenTag   tag;

	tag.sessionID = gp_session_id;
	tag.userID	= GetUserId();

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	if (!endPointDesc || endPointDesc->empty)
	{
		LWLockRelease(ParallelCursorEndpointLock);
		return;
	}
	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint sender pid.");

	/*
	 * Since the receiver is not in the session, sender has the duty to
	 * cancel it.
	 */
	signal_receiver_abort(endPointDesc->receiver_pid,
						  endPointDesc->attach_status);

	/*
	 * Only the endpoint QE/QD execute this unset sender pid function.
	 * The sender pid in Endpoint entry must be MyProcPid or InvalidPid.
	 * Note the "gp_operate_endpoints_token" UDF dispatch comment.
	 */
	Assert(MyProcPid == endPointDesc->sender_pid ||
		   endPointDesc->sender_pid == InvalidPid);
	if (MyProcPid == endPointDesc->sender_pid)
	{
		endPointDesc->sender_pid = InvalidPid;
		sessionInfoEntry =
			hash_search(sharedSessionInfoHash, &tag, HASH_FIND, NULL);
		/* sessionInfoEntry may get removed on process(which executed
		 * gp_operate_endpoints_token('w', *) UDF). This means xact finished.
		 * No need to set latch. */
		if (sessionInfoEntry)
		{
			/* Here compare the cursorName in SessionInfoEntry so endpoint
			 * process knows whether is the right time to SetLatch that tells
			 * UDF gp_wait_parallel_retrieve_cursor the retrieve is finished.
			 */
			if (strncmp((const char *) endPointDesc->cursor_name,
						sessionInfoEntry->cursorName, NAMEDATALEN) == 0)
				SetLatch(&sessionInfoEntry->udf_check_latch);
		}
	}

	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * If endpoint exit with error, let retrieve role know exception happened.
 * Called by endpoint.
 */
void
signal_receiver_abort(pid_t receiverPid, enum AttachStatus attachStatus)
{
	bool is_attached;

	elog(DEBUG3, "CDB_ENDPOINT: signal the receiver to abort.");

	is_attached = attachStatus == Status_Attached;
	if (receiverPid != InvalidPid && is_attached && receiverPid != MyProcPid)
	{
		SetBackendCancelMessage(receiverPid, "Signal the receiver to abort.");
		kill(receiverPid, SIGINT);
	}
}

/*
 * endpoint_abort - xact abort routine for endpoint
 */
void
endpoint_abort(void)
{
	if (activeSharedEndpoint)
	{
		unset_endpoint_sender_pid(activeSharedEndpoint);
		free_endpoint(activeSharedEndpoint);
		activeSharedEndpoint = NULL;
	}
	/*
	 * During xact abort, should make sure the endpoint_cleanup called first.
	 * Cause if call detach_mq to detach the message queue first, the retriever
	 * may read NULL from message queue, then retrieve mark itself down.
	 *
	 * So here, need to make sure signal retrieve abort first before endpoint
	 * detach message queue.
	 */
	if (activeDsmSeg)
	{
		detach_mq(activeDsmSeg);
		activeDsmSeg = NULL;
	}
	ClearParallelCursorExecRole();
}

/*
 * Wait for PARALLEL RETRIEVE CURSOR cleanup after endpoint send all data.
 *
 * If all data get sent, hang the process and wait for QD to close it.
 * The purpose is to not clean up EndpointDesc entry until
 * CLOSE/COMMIT/ABORT (ie. ProtalCleanup get executed).
 */
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
			elog(DEBUG3, "CDB_ENDPOINT: postmaster exit, close shared memory "
						 "message queue.");
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		elog(DEBUG3, "CDB_ENDPOINT: parallel retrieve cursor close, get latch");
		ResetLatch(&MyProc->procLatch);
		break;
	}
}

/*
 * free_endpoint - Frees the given endpoint.
 */
void
free_endpoint(volatile EndpointDesc *endpoint)
{
	Assert(endpoint);
	Assert(!endpoint->empty);

	elog(DEBUG3, "CDB_ENDPOINTS: Free endpoint '%s'.", endpoint->name);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	endpoint->database_id   = InvalidOid;
	endpoint->mq_dsm_handle = DSM_HANDLE_INVALID;
	endpoint->session_id	= InvalidSession;
	endpoint->user_id		= InvalidOid;
	endpoint->empty			= true;
	memset((char *) endpoint->name, '\0', ENDPOINT_NAME_LEN);
	ResetLatch(&endpoint->ack_done);
	DisownLatch(&endpoint->ack_done);

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
		// Register callback to deal with proc endpoint xact abort.
		RegisterSubXactCallback(sender_subxact_callback, NULL);
		RegisterXactCallback(sender_xact_abort_callback, NULL);
		is_registered = true;
	}
}

/*
 * If endpoint/sender on xact abort, do endpoint clean jobs.
 */
void
sender_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT || ev == XACT_EVENT_PARALLEL_ABORT)
	{
		if (Gp_role == GP_ROLE_RETRIEVE || Gp_role == GP_ROLE_UTILITY)
		{
			return;
		}
		elog(DEBUG3, "CDB_ENDPOINT: sender xact abort callback");
		endpoint_abort();
	}
}

/*
 * If endpoint/sender on sub xact abort, do endpoint clean jobs.
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

EndpointDesc *
get_endpointdesc_by_index(int index)
{
	Assert(sharedEndpoints);
	Assert(index > -1 && index < MAX_ENDPOINT_SIZE);
	return &sharedEndpoints[index];
}

/*
 *
 * find_endpoint - Find the endpoint by given endpoint name and session id.
 *
 * For the endpoint, the session_id is the gp_session_id since it is the same
 * with the session which created the parallel retrieve cursor.
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
		// endpoint_name is unique across sessions. But it is not right,
		// need to fetch the endpoint created in the session with the
		// given session_id.
		if (!sharedEndpoints[i].empty &&
			sharedEndpoints[i].session_id == sessionID &&
			endpoint_name_equals(sharedEndpoints[i].name, endpointName) &&
			sharedEndpoints[i].database_id == MyDatabaseId)
		{
			res = &sharedEndpoints[i];
			break;
		}
	}

	return res;
}

/*
 * get_token_by_session_id - get token based on given session id and user.
 */
void
get_token_by_session_id(int sessionId, Oid userID, int8 *token /*out*/)
{
	SessionInfoEntry *info_entry = NULL;
	SessionTokenTag   tag;

	tag.sessionID = sessionId;
	tag.userID	= userID;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	info_entry = (SessionInfoEntry *) hash_search(sharedSessionInfoHash, &tag,
												  HASH_FIND, NULL);
	if (info_entry == NULL)
	{
		elog(ERROR, "Token for user id: %d, session: %d doesn't exist",
			 tag.userID, sessionId);
	}
	memcpy(token, info_entry->token, ENDPOINT_TOKEN_LEN);
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * get_session_id_for_auth - Find the corresponding session id by the given token.
 */
int
get_session_id_for_auth(Oid userID, const int8 *token)
{
	int				  session_id = InvalidSession;
	SessionInfoEntry *info_entry = NULL;
	HASH_SEQ_STATUS   status;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	hash_seq_init(&status, sharedSessionInfoHash);
	while ((info_entry = (SessionInfoEntry *) hash_seq_search(&status)) != NULL)
	{
		if (token_equals(info_entry->token, token) &&
			userID == info_entry->tag.userID)
		{
			session_id = info_entry->tag.sessionID;
			hash_seq_term(&status);
			break;
		}
	}
	LWLockRelease(ParallelCursorEndpointLock);

	return session_id;
}

/*
 * generate_endpoint_name
 *
 * Generate the endpoint name based on the PARALLEL RETRIEVE CURSOR name,
 * session ID and the segment index.lwlock.hlwlock.h
 * The endpoint name should be unique across sessions.
 */
void
generate_endpoint_name(char *name, const char *cursorName, int32 sessionID,
					   int32 segindex)
{
	/* Use counter to avoid duplicated endpoint names when error happens.
	 * Since the retrieve session won't be terminated when transaction abort,
	 * reuse the previous endpoint name may cause unexpected behavior for the
	 * retrieving session. */
	static uint8 counter = 0;
	snprintf(name, ENDPOINT_NAME_LEN, "%s%08x%08x%02x", cursorName, sessionID,
			 segindex, counter++);
}

/*
 * Find the EndpointDesc entry by the given cursor name in current session.
 */
EndpointDesc *
find_endpoint_by_cursor_name(const char *cursorName, bool withLock)
{
	EndpointDesc *res = NULL;

	if (withLock)
		LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!sharedEndpoints[i].empty &&
			sharedEndpoints[i].session_id == gp_session_id &&
			strncmp(sharedEndpoints[i].cursor_name, cursorName, NAMEDATALEN) == 0)
		{
			res = &sharedEndpoints[i];
			break;
		}
	}

	if (withLock)
		LWLockRelease(ParallelCursorEndpointLock);

	return res;
}

void
check_dispatch_connection(void)
{
	/* Check the QD dispatcher connection is lost */
	unsigned char firstchar;
	int			  r;

	pq_startmsgread();
	r = pq_getbyte_if_available(&firstchar);
	if (r < 0)
	{
		elog(ERROR, "unexpected EOF on query dispatcher connection");
	}
	else if (r > 0)
	{
		elog(ERROR, "query dispatcher should get nothing until QE backend "
					"finished processing");
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
 * Dispatch this UDF by "CdbDispatchCommandToSegments" and "CdbDispatchCommand",
 * It'll always dispatch to writer gang.
 *
 * c: check if the endpoints have been finished retrieving in a non-blocking way.
 * h: check if the endpoints have been finished retrieving in a blocking way. It
 *    will return until endpoints were finished, or any error occurs.
 * w: wait for the QE or the entry db initializing endpoints. It will return
 *    until dest receiver is ready or timeout(5 seconds).
 */
Datum gp_operate_endpoints_token(PG_FUNCTION_ARGS)
{
	char		operation;
	bool		ret_val		= false;
	const char *token_str   = NULL;
	const char *cursor_name = NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_BOOL(false);

	operation   = PG_GETARG_CHAR(0);
	token_str   = PG_GETARG_CSTRING(1);
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
			elog(ERROR,
				 "Failed to execute gp_operate_endpoints_token('%c', '%s')",
				 operation, token_str);
			ret_val = false;
	}

	PG_RETURN_BOOL(ret_val);
}


/*
 * Register SessionInfoEntry clean up callback
 *
 * Since the SessionInfoEntry is created during WaitEndpointReady through
 * gp_operate_endpoints_token('w', *) UDF execute process(which is write gang or
 * QD), so callback needs to be registered on these processes.
 */
void
register_session_info_callback(void)
{
	static bool registered = false;
	if (!registered)
	{
		/* Register session_info_clean_callback only for UDF execute processes */
		RegisterXactCallback(session_info_clean_callback, NULL);
	}
}


/*
 * session_info_clean_callback - callback when xact finished
 *
 * When an xact finished, clean "session - token" mapping entry in
 * shared memory is needed, since there's no endpoint for current session.
 */
void
session_info_clean_callback(XactEvent ev, void *vp)
{
	if ((ev == XACT_EVENT_COMMIT || ev == XACT_EVENT_PARALLEL_COMMIT ||
		 ev == XACT_EVENT_ABORT || ev == XACT_EVENT_PARALLEL_ABORT) &&
		(Gp_is_writer || Gp_role == GP_ROLE_DISPATCH))
	{
		elog(
			LOG,
			"CDB_ENDPOINT: sender_xact_abort_callback clean token for session %d",
			EndpointCtl.session_id);
		SessionInfoEntry *entry;
		SessionTokenTag   tag;

		/* When proc exit, the gp_session_id is -1, so use our record session id
		 * instead */
		tag.sessionID = EndpointCtl.session_id;
		tag.userID	= GetUserId();

		LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
		entry = hash_search(sharedSessionInfoHash, &tag, HASH_REMOVE, NULL);
		if (!entry)
		{
			elog(LOG,
				 "CDB_ENDPOINT: sender_xact_abort_callback no entry exists for "
				 "user id: %d, session: %d",
				 tag.userID, EndpointCtl.session_id);
		}
		LWLockRelease(ParallelCursorEndpointLock);
	}
}

/*
 * Check if the endpoint has finished retrieving data
 *
 * This func is called in UDF in the write gang.
 *
 * In NOWAIT mode, this func should not report any error about the endpoint info
 * or endpoint QE execute error, because if the write gang error, it need time to
 * handle cancel, and report error to QD, when next statement comes, it will
 * report error at this time, it make the error message misunderstanding (User may
 * think that the endpoint backend of the latter cursor issues error). Also it
 * also issues 2 error messages (one is from endpoint backend, the other is from
 * the related write gang process). So we need to check query dispatcher (which
 * dispatch the original query of the parallel retrieve cursor) in addition in
 * case of any error issues.
 *
 * In WAIT mode, we cannot use the same way as NOWAIT mode, because if one of the
 * endpoint backend reports error, then the query dispatcher (which dispatch this
 * UDF) will not return because other endpoints still waiting for retrieving
 * session. So just report error in this func, although still 2 error messages
 * issues, but the error is regarded as this statement's error (because there is
 * no other statement running at the same session)
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
	SessionTokenTag   tag;
	SessionInfoEntry  *sessionInfoEntry;
	bool			  isFinished    = false;
	EndpointDesc	  *endpointDesc = NULL;
	Latch			  *latch		= NULL;

	tag.userID	= GetUserId();
	tag.sessionID = gp_session_id;

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	endpointDesc = find_endpoint_by_cursor_name(cursorName, false);
	if (endpointDesc)
	{
		if (endpointDesc->user_id != GetUserId())
		{
			LWLockRelease(ParallelCursorEndpointLock);
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("The PARALLEL RETRIEVE CURSOR was created by "
								   "a different user."),
							errhint("Using the same user as the PARALLEL "
									"RETRIEVE CURSOR creator.")));
		}
		isFinished = endpointDesc->attach_status == Status_Finished;
	}
	else
	{
		LWLockRelease(ParallelCursorEndpointLock);
		if (isWait)
		{
			elog(ERROR, "Endpoint doesn't exist.");
		}
		else
		{
			/* if no endpoint found, it maybe something wrong, just return false,
			 * i.e. not finished successfully. */
			return false;
		}
	}

	sessionInfoEntry = hash_search(sharedSessionInfoHash, &tag, HASH_FIND, NULL);
	Assert(sessionInfoEntry);

	/* If wait is needed, set right cursorName in SessionInfoEntry when holding
	 * LW_EXCLUSIVE EndpointLock. This guarantee sender will set latch when
	 * retrieve finished.
	 * Reset latch is needed before release the EndpointLock. Cause here need
	 * clean the latch for waiting. And sender may SetLatch right after release
	 * the EndpointLock. If reset latch out the lock, here may WaitLatch forever.
	 *
	 * See declare_parallel_retrieve_ready for more details. */
	if (isWait && !isFinished)
	{
		latch = &sessionInfoEntry->udf_check_latch;
		ResetLatch(latch);
		snprintf(sessionInfoEntry->cursorName, NAMEDATALEN, "%s", cursorName);
	}

	LWLockRelease(ParallelCursorEndpointLock);

	if (latch)
	{
		elog(LOG,
			 "CDB_ENDPOINT: WaitLatch on sessionInfoEntry->udf_check_latch by "
			 "pid: %d",
			 MyProcPid);
		while (true)
		{
			int wr;
			CHECK_FOR_INTERRUPTS();

			if (QueryFinishPending)
				break;
			wr = WaitLatch(latch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
						   WAIT_NORMAL_TIMEOUT);
			if (wr & WL_TIMEOUT)
				continue;
			if (wr & WL_POSTMASTER_DEATH)
			{
				snprintf(sessionInfoEntry->cursorName, NAMEDATALEN, "%s", "");
				proc_exit(0);
			}
			break;
		}

		LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
		sessionInfoEntry =
			hash_search(sharedSessionInfoHash, &tag, HASH_FIND, NULL);
		if (sessionInfoEntry)
		{
			/* It's ok not clean cursorName for interrupts or proc_exit,
			 * since ResetLatch is always executed before WaitLatch,
			 * it's also ok the latch get set by others already. */
			snprintf(sessionInfoEntry->cursorName, NAMEDATALEN, "%s", "");
		}
		endpointDesc = find_endpoint_by_cursor_name(cursorName, false);
		if (endpointDesc)
		{
			isFinished = endpointDesc->attach_status == Status_Finished;
		}
		LWLockRelease(ParallelCursorEndpointLock);
		if (endpointDesc == NULL)
		{
			elog(ERROR, "Endpoint for '%s' get aborted.", cursorName);
		}
	}
	return isFinished;
}

/*
 * Waits until the QE is ready -- the endpoint will be ready for retrieve
 * after this function returns successfully.
 *
 * Create/reuse SessionInfoEntry for current session in shared memory.
 * SessionInfoEntry is used for retrieve auth and tracking parallel retrieve
 * from QD through UDF.
 */
void
wait_for_init_by_cursor_name(const char *cursorName, const char *tokenStr)
{
	EndpointDesc *desc			 = NULL;
	SessionInfoEntry *info_entry = NULL;
	Latch *latch                 = NULL;
	bool found					 = false;
	int wr						 = 0;
	SessionTokenTag   tag;

	/* Since current process create SessionInfoEntry. Register clean callback
	 * here. */
	register_session_info_callback();

	tag.sessionID = gp_session_id;
	tag.userID	= GetUserId();

	/* track current session id for session_info_clean_callback  */
	EndpointCtl.session_id = gp_session_id;

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	info_entry = (SessionInfoEntry *) hash_search(sharedSessionInfoHash, &tag,
												  HASH_ENTER, &found);

	elog(DEBUG3, "CDB_ENDPOINT: Finish endpoint init. Found SessionInfoEntry: %d",
		 found);
	/* Save the token if it is the first time we create endpoint in current
	 * session. We guarantee that one session will map to one token only.*/
	if (!found)
	{
		InitSharedLatch(&info_entry->udf_check_latch);
		OwnLatch(&info_entry->udf_check_latch);
	}
	/* Overwrite exists token in case the wrapped session id entry not get removed
	 * For example, 1 hours ago, a session 7 exists and have entry with token 123.
	 * And for some reason the entry not get remove by
	 * session_info_clean_callback. Now current session is session 7 again. Here
	 * need to overwrite the old token. */
	parse_token(info_entry->token, tokenStr);
	elog(DEBUG3, "CDB_ENDPOINT: set new token %s, for session %d", tokenStr,
		 gp_session_id);

	desc = find_endpoint_by_cursor_name(cursorName, false);
	/* If the endpoints have been created and attach status is prepared, just
	 * return. Otherwise, set the init_wait_proc for current session and wait on
	 * the latch.*/
	if (!desc || desc->attach_status != Status_Prepared)
	{
		snprintf(info_entry->cursorName, NAMEDATALEN, "%s", cursorName);
		ResetLatch(&info_entry->udf_check_latch);
		latch = &info_entry->udf_check_latch;
	}

	LWLockRelease(ParallelCursorEndpointLock);

	if (latch)
	{
		wr = WaitLatch(latch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   WAIT_ENDPOINT_INIT_TIMEOUT);
		ResetLatch(latch);
		/* Clear the cursor name so no need to set latch in
		 * declare_parallel_retrieve_ready in current session */
		LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
		snprintf(info_entry->cursorName, NAMEDATALEN, "%s", "");
		LWLockRelease(ParallelCursorEndpointLock);
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
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished immediately.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursor_name = NULL;
	cursor_name				= PG_GETARG_CSTRING(0);

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursor_name, false));
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Wait until given parallel retrieve cursor is finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursor_name = NULL;
	cursor_name				= PG_GETARG_CSTRING(0);

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursor_name, true));
}

/*
 * check_parallel_retrieve_cursor
 *
 * Support function for UDFs:
 * gp_check_parallel_retrieve_cursor
 * gp_wait_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished.
 * If isWait is true, hang until parallel retrieve cursor finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
bool
check_parallel_retrieve_cursor(const char *cursorName, bool isWait)
{
	bool   ret_val				= false;
	bool   is_parallel_retrieve = false;
	Portal portal;

	/* get the portal from the portal name */
	portal = GetPortalByName(cursorName);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
						errmsg("cursor \"%s\" does not exist", cursorName)));
		return false; /* keep compiler happy */
	}
	is_parallel_retrieve =
		(portal->cursorOptions & CURSOR_OPT_PARALLEL_RETRIEVE) > 0;
	if (!is_parallel_retrieve)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("This UDF only works for PARALLEL RETRIEVE CURSOR.")));
		return false;
	}

	/* See comments at check_endpoint_finished_by_cursor_name()
	 *
	 * In NOWAIT mode, need to check the query dispatcher for the orginal query of
	 * the parallel retrieve cursor, because the UDF will not report error.
	 *
	 * In WAIT mode, the UDF will report error, don't need to check the query
	 * dispatcher.
	 */
	PlannedStmt *stmt = (PlannedStmt *) linitial(portal->stmts);
	ret_val =
		call_endpoint_udf_on_qd(stmt->planTree, cursorName, isWait ? 'h' : 'c');

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("check_parallel_retrieve_cursor_after_udf");
	RESUME_INTERRUPTS();
#endif

	if (!isWait && !ret_val)
		check_parallel_cursor_errors(portal->queryDesc);
	return ret_val;
}

/*
 * check_parallel_cursor_errors - Check the PARALLEL RETRIEVE CURSOR execution
 * status
 *
 * If get error, then rethrow the error.
 * Return true if the PARALLEL RETRIEVE CURSOR Execution Finished.
 */
bool
check_parallel_cursor_errors(QueryDesc *queryDesc)
{
	EState *estate;
	bool	isParallelRetrCursorFinished = false;

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
