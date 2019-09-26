/*
 * cdbendpointretrieve.c
 *
 * After define and execute a PARALLEL RETRIEVE CURSOR(see cdbendpoint.c), the results
 * are written to endpoints. Then connect the endpoint with retrieve role to
 * retrieve data from endpoint backend.
 *
 * The retrieve backend use TupleQueueReader to read query results from the
 * shared message queue.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbendpointretrieve.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbendpoint.h"
#include "cdbendpointinternal.h"
#include "access/xact.h"
#include "cdb/cdbsrlz.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/dynahash.h"
#include "utils/backend_cancel.h"

/*
 * For receiver, we have a hash table to store connected endpoint's shared message queue.
 * So that we can retrieve from different endpoints in the same retriever and switch
 * between different endpoints.
 *
 * For endpoint(on Entry DB/QE), only keep one entry to track current message queue.
 */
typedef struct MsgQueueStatusEntry
{
	char endpoint_name[ENDPOINT_NAME_LEN];     /* The name of endpoint to be retrieved, also behave as hash key */
	dsm_segment *mq_seg;                       /* The dsm handle which contains shared memory message queue */
	shm_mq_handle *mq_handle;                  /* Shared memory message queue */
	TupleTableSlot *retrieve_ts;               /* tuple slot used for retrieve data */
	TupleQueueReader *tq_reader;               /* TupleQueueReader to read tuple from message queue */
	enum RetrieveStatus retrieve_status;       /* Track retrieve status */
} MsgQueueStatusEntry;
/* Hash table to cache tuple descriptors for all endpoint_names which have been retrieved
 * in this retrieve session */
static HTAB *MsgQueueHTB = NULL;
static MsgQueueStatusEntry* currentMQEntry = NULL;

static dsm_handle attach_endpoint(MsgQueueStatusEntry *entry);
static void detach_endpoint(MsgQueueStatusEntry *entry, bool resetPID);
static void attach_receiver_mq(MsgQueueStatusEntry *entry, dsm_handle dsmHandle);
static void detach_receiver_mq(MsgQueueStatusEntry *entry);
static void notify_sender(MsgQueueStatusEntry *entry, bool isFinished);
static TupleDesc read_tuple_desc_info(shm_toc *toc);
static TupleTableSlot *receive_tuple_slot(MsgQueueStatusEntry *entry);
static void retrieve_cancel_action(const char *endpointName, char *msg);
static void retrieve_exit_callback(int code, Datum arg);
static void retrieve_xact_abort_callback(XactEvent ev, void *vp);
static void retrieve_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);
/*
 * FindEndpointTokenByUser - authenticate for retrieve role connection.
 *
 * Return true if the user has PARALLEL RETRIEVE CURSOR/endpoint of the token
 * Used by retrieve role authentication
 *
 */
bool
FindEndpointTokenByUser(Oid userID, const char *tokenStr)
{
	bool isFound = false;
	bool parseError = false;
	int8 token[ENDPOINT_TOKEN_LEN] = {0};

	before_shmem_exit(retrieve_exit_callback, (Datum) 0);
	RegisterSubXactCallback(retrieve_subxact_callback, NULL);
	RegisterXactCallback(retrieve_xact_abort_callback, NULL);

	PG_TRY();
	{
		parse_token(token, tokenStr);
	}
	PG_CATCH();
	{
		parseError = true;
	}
	PG_END_TRY();

	if (parseError)
		return isFound;

	EndpointCtl.session_id = get_session_id_for_auth(userID, token);
	if (EndpointCtl.session_id != InvalidSession)
		isFound = true;

	return isFound;
}

/*
 * attach_endpoint - Attach to an endpoint which's name is specified in the given entry.
 * When call RETRIEVE statement in PQprepare() & PQexecPrepared(), this
 * func will be called 2 times.
 */
dsm_handle
attach_endpoint(MsgQueueStatusEntry *entry)
{
	const char *endpointName = entry->endpoint_name;
	pid_t attached_pid = InvalidPid;
	dsm_handle handle;

	if (EndpointCtl.Gp_prce_role != PRCER_RECEIVER)
		elog(ERROR, "%s could not attach endpoint",
			 endpoint_role_to_string(EndpointCtl.Gp_prce_role));

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	Endpoint endpointDesc = find_endpoint(endpointName, EndpointCtl.session_id);
	if (!endpointDesc)
	{
		elog(ERROR, "failed to attach non-existing endpoint %s", endpointName);
	}

	if (endpointDesc->user_id != GetUserId())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("The PARALLEL RETRIEVE CURSOR was created by "
							   "a different user."),
						errhint("Using the same user as the PARALLEL "
								"RETRIEVE CURSOR creator to retrieve.")));
	}

	if (endpointDesc->attach_status == Status_Attached && endpointDesc->receiver_pid != MyProcPid)
	{
		attached_pid = endpointDesc->receiver_pid;
		elog(ERROR, "Endpoint %s is already being retrieved by receiver(pid: %d)",
			 endpointName, attached_pid);
	}

	if (endpointDesc->receiver_pid != InvalidPid &&
		endpointDesc->receiver_pid != MyProcPid)
	{
		/* already attached by other process before */
		attached_pid = endpointDesc->receiver_pid;
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Endpoint %s is already attached by receiver(pid: %d)",
						endpointName, attached_pid),
				 errdetail("An endpoint can only be attached by one retrieving "
						   "session.")));
	}

	if (endpointDesc->sender_pid == InvalidPid)
	{
		/* Should not happen. */
		Assert(endpointDesc->attach_status == Status_Finished);
	}

	if (endpointDesc->receiver_pid == InvalidPid)
	{
		endpointDesc->receiver_pid = MyProcPid;
		entry->retrieve_status	 = RETRIEVE_STATUS_INIT;
	}
	endpointDesc->attach_status = Status_Attached;
	/* Not set if Status_Finished */
	if (endpointDesc->attach_status == Status_Prepared)
	{
		endpointDesc->attach_status = Status_Attached;
	}
	handle = endpointDesc->mq_dsm_handle;

	LWLockRelease(ParallelCursorEndpointLock);
	currentMQEntry = entry;
	return handle;
}

/*
 * Retrieve role need to attach to EndpointDesc entry's
 * shared memory message queue.
 */
void
attach_receiver_mq(MsgQueueStatusEntry *entry, dsm_handle dsmHandle)
{
	TupleDesc td;
	bool found;
	dsm_segment *dsm_seg = NULL;
	MemoryContext oldcontext;

	Assert(entry);
	Assert(!entry->mq_seg);
	Assert(!entry->mq_handle);

	/*
	 * Store the result slot all the retrieve mode QE life cycle, we only
	 * have one chance to built it.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	elog(DEBUG3, "CDB_ENDPOINTS: init message queue conn for receiver");

	dsm_seg = dsm_attach(dsmHandle);
	if (dsm_seg == NULL)
	{
		hash_search(MsgQueueHTB, &entry->endpoint_name, HASH_REMOVE,
					&found);
		Assert(found);
		MemoryContextSwitchTo(oldcontext);
		elog(ERROR, "attach to shared message queue failed.");
	}

	dsm_pin_mapping(dsm_seg);
	shm_toc *toc =
		shm_toc_attach(ENDPOINT_MSG_QUEUE_MAGIC, dsm_segment_address(dsm_seg));
	shm_mq *mq = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_QUEUE);
	shm_mq_set_receiver(mq, MyProc);
	entry->mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
	entry->mq_seg	= dsm_seg;

	td				 = read_tuple_desc_info(toc);
	entry->tq_reader = CreateTupleQueueReader(entry->mq_handle, td);

	if (entry->retrieve_ts != NULL)
		ExecClearTuple(entry->retrieve_ts);
	entry->retrieve_ts = MakeTupleTableSlot();
	ExecSetSlotDescriptor(entry->retrieve_ts, td);
	entry->retrieve_status = RETRIEVE_STATUS_GET_TUPLEDSCR;

    MemoryContextSwitchTo(oldcontext);
}

void
detach_receiver_mq(MsgQueueStatusEntry *entry)
{
	Assert(entry);
	Assert(entry->mq_seg);
	Assert(entry->mq_handle);

	/* No need to call shm_mq_detach since mq will register mq_detach_callback on
	 * seg->on_detach to do that. */
	dsm_detach(entry->mq_seg);
	entry->mq_seg = NULL;
	entry->mq_handle = NULL;
}

/*
 * Notify the sender to stop waiting on the ack_done latch.
 */
void
notify_sender(MsgQueueStatusEntry *entry, bool isFinished)
{
	EndpointDesc *endpoint;
	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	endpoint = find_endpoint(entry->endpoint_name, EndpointCtl.session_id);
	if (endpoint == NULL)
	{
		LWLockRelease(ParallelCursorEndpointLock);
		elog(ERROR, "Failed to notify non-existing endpoint %s", entry->endpoint_name);
	}
	if (isFinished)
	{
		endpoint->attach_status = Status_Finished;
	}
	SetLatch(&endpoint->ack_done);
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Read TupleDesc from the shared memory message queue.
 */
static TupleDesc
read_tuple_desc_info(shm_toc *toc)
{
	int *tdlen_plen;

	char *tdlen_space;
	char *tupdesc_space;

	tdlen_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC_LEN);
	tdlen_plen = (int *) tdlen_space;

	tupdesc_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC);

	TupleDescNode *tupdescnode = (TupleDescNode *) deserializeNode(tupdesc_space, *tdlen_plen);
	return tupdescnode->tuple;
}

/*
 * GetEndpointTupleDesc - Gets TupleDesc to retrieve from endpoint.
 *
 * Returns the tuple description for retrieve statement for the given endpoint
 * name.
 */
TupleDesc
GetEndpointTupleDesc(const char *endpointName)
{
	dsm_handle handle;
	bool isFound;
	MsgQueueStatusEntry *entry;

	Assert(endpointName);
	Assert(endpointName[0]);

	if (MsgQueueHTB == NULL)
	{
		HASHCTL ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = ENDPOINT_NAME_LEN;
		ctl.entrysize = sizeof(MsgQueueStatusEntry);
		ctl.hash = string_hash;
		MsgQueueHTB = hash_create("endpoint hash", MAX_ENDPOINT_SIZE, &ctl,
								  (HASH_ELEM | HASH_FUNCTION));
	}

	/*
	 * Search all endpoint_names that retrieved in this session
	 */
	entry = hash_search(MsgQueueHTB, endpointName, HASH_ENTER, &isFound);
	if (!isFound)
	{
		entry->mq_seg = NULL;
		entry->mq_handle = NULL;
		entry->retrieve_status = RETRIEVE_STATUS_INVALID;
		entry->retrieve_ts = NULL;
	}

	handle = attach_endpoint(entry);

	if (entry->retrieve_status == RETRIEVE_STATUS_INIT)
	{
		attach_receiver_mq(entry, handle);
	}

	Assert(entry->retrieve_ts);
	Assert(entry->retrieve_ts->tts_tupleDescriptor);

	return entry->retrieve_ts->tts_tupleDescriptor;
}

/*
 * RetrieveResults - As retrieve role, get all tuples from endpoint.
 *
 * Detach endpoint for each retrieve.
 */
void
RetrieveResults(RetrieveStmt *stmt, DestReceiver *dest)
{
	MsgQueueStatusEntry *entry = NULL;
	TupleTableSlot *result	 = NULL;
	int64 retrieve_count;

	entry = hash_search(MsgQueueHTB, stmt->endpoint_name, HASH_FIND, NULL);
	if (entry == NULL)
	{
		elog(ERROR, "Endpoint %s has not been attached.", stmt->endpoint_name);
	}

	retrieve_count = stmt->count;
	if (retrieve_count <= 0 && !stmt->is_all)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("RETRIEVE statement only supports forward scan, count should not be: %ld", retrieve_count)));
	}

	if (entry->retrieve_status < RETRIEVE_STATUS_FINISH)
	{
		while (retrieve_count > 0)
		{
			result = receive_tuple_slot(entry);
			if (!result)
			{
				break;
			}
			(*dest->receiveSlot)(result, dest);
			retrieve_count--;
		}

		if (stmt->is_all)
		{
			while (true)
			{
				result = receive_tuple_slot(entry);
				if (!result)
				{
					break;
				}
				(*dest->receiveSlot)(result, dest);
			}
		}
	}

	detach_endpoint(entry, false);
	ClearParallelCursorExecRole();
}

/*
 * Read a tuple from shared memory message queue.
 *
 * When read all tuples, should tell endpoint/sender that the retrieve is done.
 */
static TupleTableSlot *
receive_tuple_slot(MsgQueueStatusEntry *entry)
{
	TupleTableSlot *result = NULL;
	HeapTuple tup = NULL;
	bool readerdone = false;

	CHECK_FOR_INTERRUPTS();

	Assert(entry->tq_reader != NULL);

	/* at the first time to retrieve data */
	if (entry->retrieve_status == RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/* try to receive data with nowait, so that empty result will not hang here */
		tup = TupleQueueReaderNext(entry->tq_reader, true, &readerdone);

		entry->retrieve_status = RETRIEVE_STATUS_GET_DATA;

		/* at the first time to retrieve data, tell sender not to wait at wait_receiver()*/
		elog(DEBUG3, "CDB_ENDPOINT: receiver set latch in receive_tuple_slot() at the first time to retrieve data");
		notify_sender(entry, false);
	}

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("fetch_tuples_from_endpoint");
	RESUME_INTERRUPTS();
#endif

	/* re retrieve data in wait mode
	 * if not the first time retrieve data
	 * or if the first time retrieve an invalid data, but not finish */
	if (readerdone == false && tup == NULL)
	{
		tup = TupleQueueReaderNext(entry->tq_reader, false, &readerdone);
	}

	/* readerdone returns true only after sender detach mq */
	if (readerdone)
	{
		Assert(!tup);
		DestroyTupleQueueReader(entry->tq_reader);
		entry->tq_reader = NULL;
		/* dsm_detach will send SIGUSR1 to sender which may interrupt the
		 * procLatch. But sender will wait on procLatch after finishing sending.
		 * So dsm_detach has to be called earlier to ensure the SIGUSR1 is coming
		 * from the CLOSE CURSOR.
		 */
		detach_receiver_mq(entry);
		entry->retrieve_status = RETRIEVE_STATUS_FINISH;
		notify_sender(entry, true);
		return NULL;
	}

	if (HeapTupleIsValid(tup))
	{
		Assert(entry->mq_handle);
		Assert(entry->retrieve_ts);
		ExecClearTuple(entry->retrieve_ts);
		result = entry->retrieve_ts;
		ExecStoreHeapTuple(tup,        /* tuple to store */
						   result,    /* slot in which to store the tuple */
						   InvalidBuffer,    /* buffer associated with this tuple */
						   false);    /* slot should not pfree tuple */
	}
	return result;
}

/*
 * DetachEndpoint - Retrieve role detaches endpoint.
 *
 * When detach endpoint, if this process have not yet finish this mq reading,
 * then don't reset it's pid, so that we can know the process is the first time
 * of attaching endpoint (need to re-read tuple descriptor).
 *
 * Note: don't drop the result slot, we only have one chance to built it.
 * Errors in these function is not expect to be raised.
 */
void
detach_endpoint(MsgQueueStatusEntry *entry, bool resetPID)
{
	EndpointDesc *endpoint = NULL;

	Assert(EndpointCtl.Gp_prce_role == PRCER_RECEIVER);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	endpoint = find_endpoint(entry->endpoint_name, EndpointCtl.session_id);
	if (endpoint == NULL)
	{
		/*
		 * The endpoint has already cleaned the EndpointDesc entry.
		 * Or during the retrieve abort stage, sender cleaned the EndpointDesc
		 * entry. And another endpoint gets allocated just after the clean, which
		 * will occupy current endpoint entry.
		 */
		LWLockRelease(ParallelCursorEndpointLock);
		currentMQEntry = NULL;
		return;
	}

	/*
	 * If the receiver pid get retrieve_cancel_action, the receiver pid is
	 * invalid.
	 */
	if (endpoint->receiver_pid != MyProcPid &&
		endpoint->receiver_pid != InvalidPid)
		elog(ERROR, "unmatched pid, expected %d but it's %d", MyProcPid,
			 endpoint->receiver_pid);

	if (resetPID)
	{
		endpoint->receiver_pid = InvalidPid;
	}

	/* Don't set if Status_Finished */
	if (endpoint->attach_status == Status_Attached)
	{
		if (entry->retrieve_status == RETRIEVE_STATUS_FINISH)
		{
			endpoint->attach_status = Status_Finished;
		}
		else
		{
			endpoint->attach_status = Status_Prepared;
		}
	}

	LWLockRelease(ParallelCursorEndpointLock);
	currentMQEntry = NULL;
}

/*
 * When retrieve role exit with error, let endpoint/sender know exception happened.
 */
static void
retrieve_cancel_action(const char *endpointName, char *msg)
{
	/*
	 * If current role is not receiver, the retrieve must already finished success
	 * or get cleaned before.
	 */
	if (EndpointCtl.Gp_prce_role != PRCER_RECEIVER)
		elog(DEBUG3, "CDB_ENDPOINT: retrieve_cancel_action current role is not receiver.");

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	EndpointDesc *endpointDesc = find_endpoint(endpointName, EndpointCtl.session_id);
	if (endpointDesc && endpointDesc->receiver_pid == MyProcPid
		&& endpointDesc->attach_status != Status_Finished)
	{
		endpointDesc->receiver_pid = InvalidPid;
		endpointDesc->attach_status = Status_Released;
		if (endpointDesc->sender_pid != InvalidPid)
		{
			elog(DEBUG3, "CDB_ENDPOINT: signal sender to abort");
			SetBackendCancelMessage(endpointDesc->sender_pid, msg);
			kill(endpointDesc->sender_pid, SIGINT);
		}
	}

	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Callback when retrieve role on proc exit, before shmem exit.
 *
 * If retrieve role session do retrieve for more than one endpoint_name.
 * On exit, we need to detach all message queue.
 * It's a callback in before shmem exit.
 *
 * shmem_exit()
 * --> ... (other before shmem callback if exists)
 * --> retrieve_exit_callback
 *     --> cancel sender if needed.
 *     --> detach all message queue dsm
 * --> endpoint_exit_callback
 *     --> dsm_detach, called by both endpoint and retriever
 * --> ... (other callbacks)
 * --> ShutdownPostgres (the last before shmem callback)
 *     --> AbortOutOfAnyTransaction
 *         --> AbortTransaction
 *             --> CallXactCallbacks
 *                 --> retrieve_xact_abort_callback
 *         --> CleanupTransaction
 * --> dsm_backend_shutdown
 *
 * If is normal abort, retriever clean job will be done in xact abort
 * callback retrieve_xact_abort_callback
 *
 * If is proc exit, retriever clean job must be done in retrieve_exit_callback before
 * dsm detach.
 *
 * Question:
 * Is it better to detach the dsm we created/attached before dsm_backend_shutdown?
 * Or we can let dsm_backend_shutdown do the detach for us, so we don't need register
 * call back in before_shmem_exit.
 */
static void retrieve_exit_callback(int code, Datum arg)
{
	HASH_SEQ_STATUS status;
	MsgQueueStatusEntry *entry;

	elog(DEBUG3, "CDB_ENDPOINTS: retrieve exit callback");

	/* Nothing to do if hashtable not set up */
	if (MsgQueueHTB == NULL)
		return;

	if (currentMQEntry)
	{
		detach_endpoint(currentMQEntry, true);
	}

	/* Cancel all partially retrieved endpoints in this retrieve session */
	hash_seq_init(&status, MsgQueueHTB);
	while ((entry = (MsgQueueStatusEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->retrieve_status != RETRIEVE_STATUS_FINISH)
			retrieve_cancel_action(entry->endpoint_name, "Endpoint retrieve session quit, "
														  "all unfinished endpoint backends will be cancelled");
		if (entry->mq_seg) {
			/* It could have been detached already when finish. */
			detach_receiver_mq(entry);
		}
	}
	MsgQueueHTB = NULL;
	ClearParallelCursorExecRole();
}

/*
 * Retrieve role xact abort callback.
 *
 * If normal abort, DetachEndpoint and retrieve_cancel_action will only
 * be called once in current function for current endpoint_name.
 *
 * Buf if it's proc exit, these two methods will be called twice for current endpoint_name.
 * Since we call these two methods before dsm detach.
 */
static void retrieve_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT)
	{
		elog(DEBUG3, "CDB_ENDPOINT: retrieve xact abort callback");
		if (EndpointCtl.Gp_prce_role == PRCER_RECEIVER &&
            EndpointCtl.session_id != InvalidSession &&
			currentMQEntry)
		{
			if (currentMQEntry->retrieve_status != RETRIEVE_STATUS_FINISH)
				retrieve_cancel_action(currentMQEntry->endpoint_name,
									   "Endpoint retrieve statement aborted");
			detach_endpoint(currentMQEntry, true);
		}
		ClearParallelCursorExecRole();
	}
}

/*
 * Retrieve role sub xact abort callback.
 */
static void retrieve_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		retrieve_xact_abort_callback(XACT_EVENT_ABORT, NULL);
	}
}

