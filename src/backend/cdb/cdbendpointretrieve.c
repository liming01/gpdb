/*
 * cdbendpointretrieve.c
 *
 * After define and execute a parallel cursor(see cdbendpoint.c), the results
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
#include "access/xact.h"
#include "cdb/cdbsrlz.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/dynahash.h"

/* Hash table to cache tuple descriptors for all tokens which have been retrieved
 * in this retrieve session */
static HTAB *MsgQueueHTB = NULL;
static MsgQueueStatusEntry *currentMQEntry = NULL;

/* Current EndpointDesc entry */
static volatile EndpointDesc *my_shared_endpoint = NULL;

/* receiver which is a backend connected by retrieve mode */
static void init_conn_for_receiver(void);
static TupleDesc read_tuple_desc_info(shm_toc *toc);
static TupleTableSlot *receive_tuple_slot(void);
static void receiver_finish(void);
static void receiver_mq_close(void);
static void retrieve_cancel_action(int64 token, char *msg);
static void retrieve_exit_callback(int code, Datum arg);
static void retrieve_xact_abort_callback(XactEvent ev, void *vp);
static void retrieve_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);

/*
 * FindEndpointTokenByUser - authenticate for retrieve role connection.
 *
 * Return true if the user has parallel cursor/endpoint of the token
 * Used by retrieve role authentication
 */
bool
FindEndpointTokenByUser(Oid user_id, const char *token_str)
{
	bool isFound = false;
	before_shmem_exit(retrieve_exit_callback, (Datum) 0);
	RegisterSubXactCallback(retrieve_subxact_callback, NULL);
	RegisterXactCallback(retrieve_xact_abort_callback, NULL);
	LWLockAcquire(EndpointsLWLock, LW_SHARED);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (!SharedEndpoints[i].empty &&
			SharedEndpoints[i].user_id == user_id)
		{
			/*
			 * Here convert token from int32 to string before comparation so
			 * that even if the password can not be parsed to int32, there is
			 * no crash.
			 */
			char *token = PrintToken(SharedEndpoints[i].token);

			if (strcmp(token, token_str) == 0)
			{
				isFound = true;
				pfree(token);
				break;
			}
			pfree(token);
		}
	}
	LWLockRelease(EndpointsLWLock);
	return isFound;
}

/*
 * AttachEndpoint - Retrieve attach to endpoint.
 *
 * Find the endpoint to retrieve from EndpointDesc entries.
 */
void
AttachEndpoint(void)
{
	int i;
	bool isFound = false;
	bool already_attached = false;        /* now is attached? */
	bool is_self_pid = false;             /* indicate this process has been
										   * attached to this token before */
	bool is_other_pid = false;            /* indicate other process has been
										   * attached to this token before */
	bool is_invalid_sendpid = false;
	bool has_privilege = true;
	pid_t attached_pid = InvalidPid;

	if (EndpointCtl.Gp_pce_role != PCER_RECEIVER)
		elog(ERROR, "%s could not attach endpoint", EndpointRoleToString(EndpointCtl.Gp_pce_role));

	if (my_shared_endpoint)
		elog(ERROR, "endpoint is already attached");

	CheckTokenValid();

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].database_id == MyDatabaseId &&
			SharedEndpoints[i].token == EndpointCtl.Gp_token &&
			!SharedEndpoints[i].empty)
		{
			if (SharedEndpoints[i].user_id != GetUserId())
			{
				has_privilege = false;
				break;
			}
			if (SharedEndpoints[i].sender_pid == InvalidPid)
			{
				is_invalid_sendpid = true;
				break;
			}

			if (SharedEndpoints[i].attach_status == Status_Attached)
			{
				already_attached = true;
				attached_pid = SharedEndpoints[i].receiver_pid;
				break;
			}

			if (SharedEndpoints[i].receiver_pid == MyProcPid)    /* already attached by
																 * this process before */
			{
				is_self_pid = true;
			} else if (SharedEndpoints[i].receiver_pid != InvalidPid)        /* already attached by
																		 * other process before */
			{
				is_other_pid = true;
				attached_pid = SharedEndpoints[i].receiver_pid;
				break;
			} else
			{
				SharedEndpoints[i].receiver_pid = MyProcPid;
			}

			/* Not set if Status_Finished */
			if (SharedEndpoints[i].attach_status == Status_Prepared)
			{
				SharedEndpoints[i].attach_status = Status_Attached;
			}
			my_shared_endpoint = &SharedEndpoints[i];
			break;
		}
	}

	LWLockRelease(EndpointsLWLock);

	if (!has_privilege)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("The parallel cursor was created by a different user."),
					errhint("Using the same user as the parallel cursor creator to retrieve.")));
	}

	if (is_invalid_sendpid)
	{
		elog(ERROR, "the PARALLEL CURSOR related to endpoint token %s is not EXECUTED",
			 PrintToken(EndpointCtl.Gp_token));
	}

	if (already_attached)
		elog(ERROR, "Endpoint %s is already being retrieved by receiver(pid: %d)",
			 PrintToken(EndpointCtl.Gp_token), attached_pid);

	if (is_other_pid)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Endpoint %s is already attached by receiver(pid: %d)",
						   PrintToken(EndpointCtl.Gp_token), attached_pid),
					errdetail("An endpoint can be attached by only one retrieving session "
							  "for each 'EXECUTE PARALLEL CURSOR'")));

	if (!my_shared_endpoint)
		elog(ERROR, "failed to attach non-existing endpoint of token %s", PrintToken(EndpointCtl.Gp_token));

	/*
	 * Search all tokens that retrieved in this session, set
	 * CurrentRetrieveToken to it's array index
	 */
	if (MsgQueueHTB == NULL)
	{
		HASHCTL ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(EndpointCtl.Gp_token);
		ctl.entrysize = sizeof(MsgQueueStatusEntry);
		ctl.hash = tag_hash;
		MsgQueueHTB = hash_create("endpoint hash", MAX_ENDPOINT_SIZE, &ctl,
								  (HASH_ELEM | HASH_FUNCTION));
	}
	currentMQEntry = hash_search(MsgQueueHTB, &EndpointCtl.Gp_token, HASH_ENTER, &isFound);
	if (!isFound)
	{
		currentMQEntry->mq_seg = NULL;
		currentMQEntry->mq_handle = NULL;
		currentMQEntry->retrieve_status = RETRIEVE_STATUS_INVALID;
		currentMQEntry->retrieve_ts = NULL;
	}
	if (!is_self_pid)
	{
		currentMQEntry->retrieve_status = RETRIEVE_STATUS_INIT;
	}

}

/*
 * Retrieve role need to attach to EndpointDesc entry's
 * shared memory message queue.
 */
static void
init_conn_for_receiver(void)
{
	CheckTokenValid();
	Assert(currentMQEntry);

	dsm_segment *dsm_seg;
	LWLockAcquire(EndpointsLWLock, LW_SHARED);
	if (currentMQEntry->mq_seg && dsm_segment_handle(currentMQEntry->mq_seg) == my_shared_endpoint->handle)
	{
		LWLockRelease(EndpointsLWLock);
		return;
	}
	if (currentMQEntry->mq_seg)
	{
		dsm_detach(currentMQEntry->mq_seg);
	}
	dsm_seg = dsm_attach(my_shared_endpoint->handle);
	LWLockRelease(EndpointsLWLock);
	if (dsm_seg == NULL)
	{
		receiver_mq_close();
		elog(ERROR, "attach to shared message queue failed.");
	}
	dsm_pin_mapping(dsm_seg);
	shm_toc *toc = shm_toc_attach(EndpointCtl.Gp_token, dsm_segment_address(dsm_seg));
	shm_mq *mq = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_QUEUE);
	shm_mq_set_receiver(mq, MyProc);
	currentMQEntry->mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
	currentMQEntry->mq_seg = dsm_seg;
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
 * TupleDescOfRetrieve - get TupleDesc for RETRIEVE.
 * Return the tuple description for retrieve statement
 */
TupleDesc
TupleDescOfRetrieve(void)
{
	TupleDesc td;
	MemoryContext oldcontext;

	Assert(currentMQEntry);
	if (currentMQEntry->retrieve_status < RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/*
		 * Store the result slot all the retrieve mode QE life cycle, we only
		 * have one chance to built it.
		 */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		init_conn_for_receiver();

		Assert(currentMQEntry->mq_handle);
		shm_toc *toc = shm_toc_attach(GpToken(), dsm_segment_address(currentMQEntry->mq_seg));
		td = read_tuple_desc_info(toc);
		currentMQEntry->tq_reader = CreateTupleQueueReader(currentMQEntry->mq_handle, td);

		if (currentMQEntry->retrieve_ts != NULL)
			ExecClearTuple(currentMQEntry->retrieve_ts);
		currentMQEntry->retrieve_ts = MakeTupleTableSlot();
		ExecSetSlotDescriptor(currentMQEntry->retrieve_ts, td);
		currentMQEntry->retrieve_status = RETRIEVE_STATUS_GET_TUPLEDSCR;

		MemoryContextSwitchTo(oldcontext);
	}

	Assert(currentMQEntry->retrieve_ts);
	Assert(currentMQEntry->retrieve_ts->tts_tupleDescriptor);

	return currentMQEntry->retrieve_ts->tts_tupleDescriptor;
}

/*
 * RetrieveResults - As retrieve role, get all tuples from endpoint.
 *
 * Detach endpoint for each retrieve.
 */
void
RetrieveResults(RetrieveStmt *stmt, DestReceiver *dest)
{
	TupleTableSlot *result;
	int64 retrieve_count;
	Assert(currentMQEntry);

	retrieve_count = stmt->count;
	if (retrieve_count <= 0 && !stmt->is_all)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("RETRIEVE statement only supports forward scan, count should not be: %ld", retrieve_count)));
	}

	if (currentMQEntry->retrieve_status < RETRIEVE_STATUS_FINISH)
	{
		while (retrieve_count > 0)
		{
			result = receive_tuple_slot();
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
				result = receive_tuple_slot();
				if (!result)
				{
					break;
				}
				(*dest->receiveSlot)(result, dest);
			}
		}

		receiver_finish();
	}

	DetachEndpoint(false);
	ClearParallelCursorExecRole();
	ClearGpToken();
}

/*
 * Read a tuple from shared memory message queue.
 *
 * When read all tuples, should tell endpoint/sender that the retrieve is done.
 */
static TupleTableSlot *
receive_tuple_slot(void)
{
	TupleTableSlot *result = NULL;
	HeapTuple tup = NULL;
	bool readerdone = false;

	CHECK_FOR_INTERRUPTS();

	Assert(currentMQEntry->tq_reader != NULL);

	/* at the first time to retrieve data */
	if (currentMQEntry->retrieve_status == RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/* try to receive data with nowait, so that empty result will not hang here */
		tup = TupleQueueReaderNext(currentMQEntry->tq_reader, true, &readerdone);

		currentMQEntry->retrieve_status = RETRIEVE_STATUS_GET_DATA;

		/* at the first time to retrieve data, tell sender not to wait at wait_receiver()*/
		elog(DEBUG3, "CDB_ENDPOINT:receiver set latch in receive_tuple_slot() at the first time to retrieve data");
		SetLatch(&my_shared_endpoint->ack_done);
	}

	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("fetch_tuples_from_endpoint");
	RESUME_INTERRUPTS();

	/* re retrieve data in wait mode
	 * if not the first time retrieve data
	 * or if the first time retrieve an invalid data, but not finish */
	if (readerdone == false && tup == NULL)
	{
		tup = TupleQueueReaderNext(currentMQEntry->tq_reader, false, &readerdone);
	}

	/* readerdone returns true only after sender detach mq */
	if (readerdone)
	{
		Assert(!tup);
		DestroyTupleQueueReader(currentMQEntry->tq_reader);
		currentMQEntry->tq_reader = NULL;
		/* when finish retrieving data, tell sender not to wait at sender_finish()*/
		elog(DEBUG3, "CDB_ENDPOINT:receiver set latch in receive_tuple_slot() when finish retrieving data");
		SetLatch(&my_shared_endpoint->ack_done);
		currentMQEntry->retrieve_status = RETRIEVE_STATUS_FINISH;
		return NULL;
	}

	if (HeapTupleIsValid(tup))
	{
		Assert(currentMQEntry->mq_handle);
		Assert(currentMQEntry->retrieve_ts);
		ExecClearTuple(currentMQEntry->retrieve_ts);
		result = currentMQEntry->retrieve_ts;
		ExecStoreHeapTuple(tup,        /* tuple to store */
						   result,    /* slot in which to store the tuple */
						   InvalidBuffer,    /* buffer associated with this tuple */
						   false);    /* slot should not pfree tuple */
		return result;
	}
	return result;
}

/*
 * Receiver finish
 */
static void
receiver_finish(void)
{
/* for now, receiver does nothing after finished */
}

/*
 * Detach shared memory message queue and clean current receiver status.
 */
static void
receiver_mq_close(void)
{
	bool found;

	// If error happened, currentMQEntry could be none.
	if (currentMQEntry != NULL && currentMQEntry->mq_seg != NULL)
	{
		dsm_detach(currentMQEntry->mq_seg);
		currentMQEntry->mq_seg = NULL;
		currentMQEntry->mq_handle = NULL;
		currentMQEntry->retrieve_status = RETRIEVE_STATUS_INVALID;
		if (currentMQEntry->retrieve_ts != NULL)
			ExecDropSingleTupleTableSlot(currentMQEntry->retrieve_ts);
		currentMQEntry->retrieve_ts = NULL;
		currentMQEntry = (MsgQueueStatusEntry *) hash_search(
			MsgQueueHTB, &currentMQEntry->retrieve_token, HASH_REMOVE, &found);
		if (!currentMQEntry)
			elog(ERROR, "CDB_ENDPOINT: Message queue status element destroy failed.");
		currentMQEntry = NULL;
	}
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
DetachEndpoint(bool reset_pid)
{
	if (EndpointCtl.Gp_pce_role != PCER_RECEIVER ||
		!my_shared_endpoint ||
		EndpointCtl.Gp_token == InvalidToken)
		return;

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	/*
	 * If the empty is true, the endpoint has already cleaned the EndpointDesc entry.
	 *
	 * Or during the retrieve abort stage, sender cleaned the EndpointDesc entry
	 * my_shared_endpoint pointed to. And another endpoint gets allocated just
	 * after the clean, which will occupy current my_shared_endpoint entry.
	 * Then DetachEndpoint gets the lock but at this time, the token in shared memory
	 * is not current retrieve token. Nothing should be done.
	 */
	if (!my_shared_endpoint->empty &&
		EndpointCtl.Gp_token == my_shared_endpoint->token)
	{
		/*
		 * If the receiver pid get retrieve_cancel_action, the pid is InvalidToken
		 */
		if (my_shared_endpoint->receiver_pid != MyProcPid &&
			my_shared_endpoint->receiver_pid != InvalidToken)
			elog(ERROR, "unmatched pid, expected %d but it's %d",
				 MyProcPid, my_shared_endpoint->receiver_pid);

		if (reset_pid)
		{
			my_shared_endpoint->receiver_pid = InvalidPid;
		}

		/* Don't set if Status_Finished */
		if (my_shared_endpoint->attach_status == Status_Attached)
		{
			my_shared_endpoint->attach_status = Status_Prepared;
		}
	}

	LWLockRelease(EndpointsLWLock);

	my_shared_endpoint = NULL;
	currentMQEntry = NULL;
}

/*
 * When retrieve role exit with error, let endpoint/sender know exception happened.
 */
static void
retrieve_cancel_action(int64 token, char *msg)
{
	/*
	 * If current role is not receiver, the retrieve must already finished success
	 * or get cleaned before.
	 */
	if (EndpointCtl.Gp_pce_role != PCER_RECEIVER)
		elog(DEBUG3, "CDB_ENDPOINT: retrieve_cancel_action current role is not receiver.");

	LWLockAcquire(EndpointsLWLock, LW_EXCLUSIVE);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].token == token && SharedEndpoints[i].receiver_pid == MyProcPid
			&& SharedEndpoints[i].attach_status != Status_Finished)
		{
			SharedEndpoints[i].receiver_pid = InvalidPid;
			SharedEndpoints[i].attach_status = Status_NotAttached;
			elog(DEBUG3, "CDB_ENDPOINT: pg_signal_backend");
			pg_signal_backend(SharedEndpoints[i].sender_pid, SIGINT, msg);
			break;
		}
	}

	LWLockRelease(EndpointsLWLock);
}

/*
 * Callback when retrieve role on proc exit, before shmem exit.
 *
 * If retrieve role session do retrieve for more than one token.
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

	/* Cancel all partially retrieved endpoints in this retrieve session */
	hash_seq_init(&status, MsgQueueHTB);
	while ((entry = (MsgQueueStatusEntry *) hash_seq_search(&status)) != NULL)
	{
		retrieve_cancel_action(entry->retrieve_token, "Endpoint retrieve session quit, "
													  "all unfinished endpoint backends will be cancelled");
	}
	DetachEndpoint(true);

	ClearGpToken();
	ClearParallelCursorExecRole();

	/* Nothing to do if hashtable not set up */
	if (MsgQueueHTB == NULL)
		return;
	/* Detach all msg queue dsm*/
	hash_seq_init(&status, MsgQueueHTB);
	while ((entry = (MsgQueueStatusEntry *) hash_seq_search(&status)) != NULL)
	{
		elog(DEBUG3, "CDB_ENDPOINT: detach queue receiver");
		dsm_detach(entry->mq_seg);
	}
	hash_destroy(MsgQueueHTB);
	MsgQueueHTB = NULL;
}

/*
 * Retrieve role xact abort callback.
 *
 * If normal abort, DetachEndpoint and retrieve_cancel_action will only
 * be called once in current function for current token.
 *
 * Buf if it's proc exit, these two methods will be called twice for current token.
 * Since we call these two methods before dsm detach.
 */
static void retrieve_xact_abort_callback(XactEvent ev, void *vp)
{
	elog(DEBUG3, "CDB_ENDPOINT: retrieve xact abort callback");
	if (ev == XACT_EVENT_ABORT)
	{
		if (EndpointCtl.Gp_pce_role == PCER_RECEIVER &&
			my_shared_endpoint != NULL &&
			EndpointCtl.Gp_token != InvalidToken)
		{
			retrieve_cancel_action(EndpointCtl.Gp_token, "Endpoint retrieve statement aborted");
			DetachEndpoint(true);
		}
		ClearGpToken();
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
