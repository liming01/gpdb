/*-------------------------------------------------------------------------
 * cdbendpointretrieve.c
 *
 * Once a PARALLEL RETRIEVE CURSOR(see cdbendpoint.c) has been declared, the QE
 * starts to send results to endpoints. The results can be retrieved through a
 * retrieve session directly on QE. A retrieve session is a special mode session
 * on QE which can only execute endpoint related statements.
 *
 * To start a retrieve session, the endpoint's token is needed as the password for
 * authentication. The user should be the same as the one who declares the
 * parallel retrieve cursor. Also a runtime parameter gp_session_rol=retrieve
 * needs to be set to start the session. As long as login succeeds, the retrieve
 * session will be able to retrieve from all endpoints which have the same token.
 * Call AuthEndpoint() with user and token to do the retrieve authentication.
 *
 * To start retrieving from an endpoint, call GetRetrieveStmtTupleDesc() to obtain
 * a TupleDesc first. The function attachs the current retrieve session to the
 * given endpoint and connect to the shared message queue as receiver with
 * TupleQueueReader.
 *
 * To retrieve, call ExecRetrieveStmt(). It reads tuples from the shared message
 * queue and write them into the given DestReceiver. If no more tuples left an
 * empty result set will be returned.
 *
 * Once a endpoint is attached to a retrieve session, it cannot be attached to
 * other retrieve sessions. That means the RETRIEVE statement on the same endpoint
 * cannot be called on more than one retrieve sessions.
 *
 * It is possible to retrieve multiple endpoints from the same retrieve session if
 * they share the same token.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		src/backend/cdb/cdbendpointretrieve.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbsrlz.h"
#include "cdbendpointinternal.h"
#include "storage/ipc.h"
#include "utils/backend_cancel.h"
#include "utils/dynahash.h"
#include "utils/elog.h"
#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

/*
 * For receiver, we have a hash table to store connected endpoint's shared message
 * queue. So that we can retrieve from different endpoints in the same retriever
 * and switch between different endpoints.
 *
 * For endpoint(on Entry DB/QE), only keep one entry to track current message
 * queue.
 */
typedef struct MsgQueueStatusEntry
{
	/* The name of endpoint to be retrieved, also behave as hash key */
	char		endpointName[ENDPOINT_NAME_LEN];
	/* The dsm handle which contains shared memory message queue */
	dsm_segment *mqSeg;
	/* Shared memory message queue */
	shm_mq_handle *mqHandle;
	/* tuple slot used for retrieve data */
	TupleTableSlot *retrieveTs;
	/* TupleQueueReader to read tuple from message queue */
	TupleQueueReader *tqReader;
	/* Track retrieve status */
	enum RetrieveStatus retrieveStatus;
}	MsgQueueStatusEntry;

/* Hash table to cache tuple descriptors for all endpoint_names which have been
 * retrieved in this retrieve session */
static HTAB *msgQueueHTB = NULL;
static MsgQueueStatusEntry *currentMQEntry = NULL;

static dsm_handle attach_endpoint(MsgQueueStatusEntry * entry);
static void detach_endpoint(MsgQueueStatusEntry * entry, bool resetPID);
static void attach_receiver_mq(MsgQueueStatusEntry * entry, dsm_handle dsmHandle);
static void detach_receiver_mq(MsgQueueStatusEntry * entry);
static void notify_sender(MsgQueueStatusEntry * entry, bool isFinished);
static void retrieve_cancel_action(const char *endpointName, char *msg);
static void retrieve_exit_callback(int code, Datum arg);
static void retrieve_xact_abort_callback(XactEvent ev, void *vp);
static void retrieve_subxact_abort_callback(SubXactEvent event,
								SubTransactionId mySubid,
								SubTransactionId parentSubid,
								void *arg);
static TupleDesc read_tuple_desc_info(shm_toc *toc);
static TupleTableSlot *receive_tuple_slot(MsgQueueStatusEntry * entry);

/*
 * AuthEndpoint - authenticate for retrieve role connection.
 *
 * Return true if the user has PARALLEL RETRIEVE CURSOR/endpoint of the token.
 * Used by retrieve role authentication
 */
bool
AuthEndpoint(Oid userID, const char *tokenStr)
{
	bool		isFound = false;
	bool		parseError = false;
	int8		token[ENDPOINT_TOKEN_LEN] = {0};

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

	EndpointCtl.sessionID = get_session_id_for_auth(userID, token);
	if (EndpointCtl.sessionID != InvalidSession)
	{
		isFound = true;
		before_shmem_exit(retrieve_exit_callback, (Datum) 0);
		RegisterSubXactCallback(retrieve_subxact_abort_callback, NULL);
		RegisterXactCallback(retrieve_xact_abort_callback, NULL);
	}

	return isFound;
}

/*
 * GetRetrieveStmtTupleDesc - Gets TupleDesc for the given retrieve statement.
 *
 * This function tries to:
 * 1. Find the endpoint with the name in RetrieveStmt.
 * 2. Attach to the endpoint message queue and create a tuple descriptor for it.
 * 3. Returns the tuple descriptor.
 */
TupleDesc
GetRetrieveStmtTupleDesc(const RetrieveStmt * stmt)
{
	dsm_handle	handle;
	bool		isFound;
	MsgQueueStatusEntry *entry;
	const char *endpointName;

	Assert(stmt);
	endpointName = stmt->endpoint_name;
	Assert(endpointName);
	Assert(endpointName[0]);

	/* Init hashtable */
	if (msgQueueHTB == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = ENDPOINT_NAME_LEN;
		ctl.entrysize = sizeof(MsgQueueStatusEntry);
		ctl.hash = string_hash;
		msgQueueHTB = hash_create("endpoint hash", MAX_ENDPOINT_SIZE, &ctl,
								  (HASH_ELEM | HASH_FUNCTION));
	}

	/*
	 * Search all endpoint_names that have been retrieved in this session
	 */
	entry = hash_search(msgQueueHTB, endpointName, HASH_ENTER, &isFound);
	if (!isFound)
	{
		entry->mqSeg = NULL;
		entry->mqHandle = NULL;
		entry->retrieveStatus = RETRIEVE_STATUS_INVALID;
		entry->retrieveTs = NULL;
	}

	handle = attach_endpoint(entry);

	if (entry->retrieveStatus == RETRIEVE_STATUS_INIT)
	{
		attach_receiver_mq(entry, handle);
	}

	Assert(entry->retrieveTs);
	Assert(entry->retrieveTs->tts_tupleDescriptor);

	return entry->retrieveTs->tts_tupleDescriptor;
}

/*
 * ExecRetrieveStmt - Execute the given RetrieveStmt.
 *
 * This function tries to use the endpoint name in the RetrieveStmt to find the
 * attached endpoint in this retrieve session. If the endpoint can be found, then
 * read from the message queue to feed the given DestReceiver. And mark the
 * endpoint as detached before returning.
 */
void
ExecRetrieveStmt(const RetrieveStmt * stmt, DestReceiver *dest)
{
	MsgQueueStatusEntry *entry = NULL;
	TupleTableSlot *result = NULL;
	int64		retrieveCount;

	entry = hash_search(msgQueueHTB, stmt->endpoint_name, HASH_FIND, NULL);
	if (entry == NULL)
	{
		elog(ERROR, "Endpoint %s has not been attached.", stmt->endpoint_name);
	}

	retrieveCount = stmt->count;
	if (retrieveCount <= 0 && !stmt->is_all)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("RETRIEVE statement only supports forward scan, "
							"count should not be: %ld",
							retrieveCount)));
	}

	if (entry->retrieveStatus < RETRIEVE_STATUS_FINISH)
	{
		while (retrieveCount > 0)
		{
			result = receive_tuple_slot(entry);
			if (!result)
			{
				break;
			}
			(*dest->receiveSlot) (result, dest);
			retrieveCount--;
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
				(*dest->receiveSlot) (result, dest);
			}
		}
	}

	detach_endpoint(entry, false);
	ClearParallelCursorExecRole();
}

/*
 * attach_endpoint - Attach to an endpoint with the name in the given
 * entry and return the endpoint's message queue handle.
 *
 * When call RETRIEVE statement in PQprepare() & PQexecPrepared(), this func will
 * be called 2 times.
 */
dsm_handle
attach_endpoint(MsgQueueStatusEntry * entry)
{
	const char *endpointName = entry->endpointName;
	pid_t		attachedPid = InvalidPid;
	dsm_handle	handle;

	Assert(entry);
	Assert(entry->endpointName);

	if (EndpointCtl.GpPrceRole != PRCER_RECEIVER)
	{
		elog(ERROR, "%s could not attach endpoint",
			 endpoint_role_to_string(EndpointCtl.GpPrceRole));
	}

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	Endpoint	endpointDesc = find_endpoint(endpointName, EndpointCtl.sessionID);

	if (!endpointDesc)
	{
		elog(ERROR, "failed to attach non-existing endpoint %s", endpointName);
	}

	if (endpointDesc->userID != GetUserId())
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("The PARALLEL RETRIEVE CURSOR was created by "
							   "a different user."),
						errhint("Using the same user as the PARALLEL "
								"RETRIEVE CURSOR creator to retrieve.")));
	}

	if (endpointDesc->attachStatus == Status_Attached &&
		endpointDesc->receiverPid != MyProcPid)
	{
		attachedPid = endpointDesc->receiverPid;
		elog(ERROR, "Endpoint %s is already being retrieved by receiver(pid: %d)",
			 endpointName, attachedPid);
	}

	if (endpointDesc->receiverPid != InvalidPid &&
		endpointDesc->receiverPid != MyProcPid)
	{
		/* already attached by other process before */
		attachedPid = endpointDesc->receiverPid;
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
			   errmsg("Endpoint %s is already attached by receiver(pid: %d)",
					  endpointName, attachedPid),
			  errdetail("An endpoint can only be attached by one retrieving "
						"session.")));
	}

	if (endpointDesc->senderPid == InvalidPid)
	{
		/* Should not happen. */
		Assert(endpointDesc->attachStatus == Status_Finished);
	}

	if (endpointDesc->receiverPid == InvalidPid)
	{
		endpointDesc->receiverPid = MyProcPid;
		entry->retrieveStatus = RETRIEVE_STATUS_INIT;
	}
	endpointDesc->attachStatus = Status_Attached;
	/* Not set if Status_Finished */
	if (endpointDesc->attachStatus == Status_Prepared)
	{
		endpointDesc->attachStatus = Status_Attached;
	}
	handle = endpointDesc->mqDsmHandle;

	LWLockRelease(ParallelCursorEndpointLock);
	currentMQEntry = entry;
	return handle;
}

/*
 * Attach to the endpoint's shared memory message queue.
 */
void
attach_receiver_mq(MsgQueueStatusEntry * entry, dsm_handle dsmHandle)
{
	TupleDesc	td;
	bool		found;
	dsm_segment *dsmSeg = NULL;
	MemoryContext oldcontext;

	Assert(entry);
	Assert(!entry->mqSeg);
	Assert(!entry->mqHandle);

	/*
	 * Store the result slot all the retrieve mode QE life cycle, we only have
	 * one chance to built it.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	elog(DEBUG3, "CDB_ENDPOINTS: init message queue conn for receiver");

	dsmSeg = dsm_attach(dsmHandle);
	if (dsmSeg == NULL)
	{
		hash_search(msgQueueHTB, &entry->endpointName, HASH_REMOVE, &found);
		Assert(found);
		elog(ERROR, "attach to shared message queue failed.");
	}

	dsm_pin_mapping(dsmSeg);
	shm_toc    *toc =
	shm_toc_attach(ENDPOINT_MSG_QUEUE_MAGIC, dsm_segment_address(dsmSeg));
	shm_mq	   *mq = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_QUEUE);

	shm_mq_set_receiver(mq, MyProc);
	entry->mqHandle = shm_mq_attach(mq, dsmSeg, NULL);
	entry->mqSeg = dsmSeg;

	td = read_tuple_desc_info(toc);
	entry->tqReader = CreateTupleQueueReader(entry->mqHandle, td);

	if (entry->retrieveTs != NULL)
		ExecClearTuple(entry->retrieveTs);
	else
		entry->retrieveTs = MakeTupleTableSlot();

	ExecSetSlotDescriptor(entry->retrieveTs, td);
	entry->retrieveStatus = RETRIEVE_STATUS_GET_TUPLEDSCR;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Detach from the endpoint's message queue.
 */
void
detach_receiver_mq(MsgQueueStatusEntry * entry)
{
	Assert(entry);
	Assert(entry->mqSeg);
	Assert(entry->mqHandle);

	/*
	 * No need to call shm_mq_detach since mq will register mq_detach_callback
	 * on seg->on_detach to do that.
	 */
	dsm_detach(entry->mqSeg);
	entry->mqSeg = NULL;
	entry->mqHandle = NULL;
}

/*
 * Notify the sender to stop waiting on the ackDone latch.
 */
void
notify_sender(MsgQueueStatusEntry * entry, bool isFinished)
{
	EndpointDesc *endpoint;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	endpoint = find_endpoint(entry->endpointName, EndpointCtl.sessionID);
	if (endpoint == NULL)
	{
		LWLockRelease(ParallelCursorEndpointLock);
		elog(ERROR, "Failed to notify non-existing endpoint %s",
			 entry->endpointName);
	}
	if (isFinished)
	{
		endpoint->attachStatus = Status_Finished;
	}
	SetLatch(&endpoint->ackDone);
	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Read TupleDesc from the shared memory message queue.
 */
static TupleDesc
read_tuple_desc_info(shm_toc *toc)
{
	int		   *tdlen_plen;
	char	   *tdlen_space;
	char	   *tupdesc_space;

	tdlen_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC_LEN);
	tdlen_plen = (int *) tdlen_space;

	tupdesc_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC);

	TupleDescNode *tupdescnode =
	(TupleDescNode *) deserializeNode(tupdesc_space, *tdlen_plen);

	return tupdescnode->tuple;
}

/*
 * Read a tuple from shared memory message queue.
 *
 * When read all tuples, should tell endpoint/sender that the retrieve is done.
 */
static TupleTableSlot *
receive_tuple_slot(MsgQueueStatusEntry * entry)
{
	TupleTableSlot *result = NULL;
	HeapTuple	tup = NULL;
	bool		readerdone = false;

	CHECK_FOR_INTERRUPTS();

	Assert(entry->tqReader != NULL);

	/* at the first time to retrieve data */
	if (entry->retrieveStatus == RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/*
		 * try to receive data with nowait, so that empty result will not hang
		 * here
		 */
		tup = TupleQueueReaderNext(entry->tqReader, true, &readerdone);

		entry->retrieveStatus = RETRIEVE_STATUS_GET_DATA;

		/*
		 * at the first time to retrieve data, tell sender not to wait at
		 * wait_receiver()
		 */
		elog(DEBUG3, "CDB_ENDPOINT: receiver set latch in receive_tuple_slot() "
			 "at the first time to retrieve data");
		notify_sender(entry, false);
	}

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("fetch_tuples_from_endpoint");
	RESUME_INTERRUPTS();
#endif

	/*
	 * re retrieve data in wait mode if not the first time retrieve data or if
	 * the first time retrieve an invalid data, but not finish
	 */
	if (readerdone == false && tup == NULL)
	{
		tup = TupleQueueReaderNext(entry->tqReader, false, &readerdone);
	}

	/* readerdone returns true only after sender detach mq */
	if (readerdone)
	{
		Assert(!tup);
		DestroyTupleQueueReader(entry->tqReader);
		entry->tqReader = NULL;

		/*
		 * dsm_detach will send SIGUSR1 to sender which may interrupt the
		 * procLatch. But sender will wait on procLatch after finishing
		 * sending. So dsm_detach has to be called earlier to ensure the
		 * SIGUSR1 is coming from the CLOSE CURSOR.
		 */
		detach_receiver_mq(entry);
		entry->retrieveStatus = RETRIEVE_STATUS_FINISH;
		notify_sender(entry, true);
		return NULL;
	}

	if (HeapTupleIsValid(tup))
	{
		Assert(entry->mqHandle);
		Assert(entry->retrieveTs);
		ExecClearTuple(entry->retrieveTs);
		result = entry->retrieveTs;
		ExecStoreHeapTuple(tup, /* tuple to store */
						   result,		/* slot in which to store the tuple */
						   InvalidBuffer,		/* buffer associated with this
												 * tuple */
						   false);		/* slot should not pfree tuple */
	}
	return result;
}

/*
 * detach_endpoint - Retrieve role detaches endpoint.
 *
 * When detach endpoint, if this process have not yet finish this mq reading,
 * then don't reset it's pid, so that we can know the process is the first time
 * of attaching endpoint (need to re-read tuple descriptor).
 *
 * Note: don't drop the result slot, we only have one chance to built it.
 * Errors in these function is not expect to be raised.
 */
void
detach_endpoint(MsgQueueStatusEntry * entry, bool resetPID)
{
	EndpointDesc *endpoint = NULL;

	Assert(EndpointCtl.GpPrceRole == PRCER_RECEIVER);

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);
	endpoint = find_endpoint(entry->endpointName, EndpointCtl.sessionID);
	if (endpoint == NULL)
	{
		/*
		 * The endpoint has already cleaned the EndpointDesc entry. Or during
		 * the retrieve abort stage, sender cleaned the EndpointDesc entry.
		 * And another endpoint gets allocated just after the clean, which
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
	if (endpoint->receiverPid != MyProcPid &&
		endpoint->receiverPid != InvalidPid)
		elog(ERROR, "unmatched pid, expected %d but it's %d", MyProcPid,
			 endpoint->receiverPid);

	if (resetPID)
	{
		endpoint->receiverPid = InvalidPid;
	}

	/* Don't set if Status_Finished */
	if (endpoint->attachStatus == Status_Attached)
	{
		if (entry->retrieveStatus == RETRIEVE_STATUS_FINISH)
		{
			endpoint->attachStatus = Status_Finished;
		}
		else
		{
			endpoint->attachStatus = Status_Prepared;
		}
	}

	LWLockRelease(ParallelCursorEndpointLock);
	currentMQEntry = NULL;
}

/*
 * When retrieve role exit with error, let endpoint/sender know exception
 * happened.
 */
static void
retrieve_cancel_action(const char *endpointName, char *msg)
{
	/*
	 * If current role is not receiver, the retrieve must already finished
	 * success or get cleaned before.
	 */
	if (EndpointCtl.GpPrceRole != PRCER_RECEIVER)
		elog(
			 DEBUG3,
		"CDB_ENDPOINT: retrieve_cancel_action current role is not receiver.");

	LWLockAcquire(ParallelCursorEndpointLock, LW_EXCLUSIVE);

	EndpointDesc *endpointDesc =
	find_endpoint(endpointName, EndpointCtl.sessionID);

	if (endpointDesc && endpointDesc->receiverPid == MyProcPid &&
		endpointDesc->attachStatus != Status_Finished)
	{
		endpointDesc->receiverPid = InvalidPid;
		endpointDesc->attachStatus = Status_Released;
		if (endpointDesc->senderPid != InvalidPid)
		{
			elog(DEBUG3, "CDB_ENDPOINT: signal sender to abort");
			SetBackendCancelMessage(endpointDesc->senderPid, msg);
			kill(endpointDesc->senderPid, SIGINT);
		}
	}

	LWLockRelease(ParallelCursorEndpointLock);
}

/*
 * Callback when retrieve role on proc exit, before shmem exit.
 *
 * For Process Exists:
 * If a retrieve session has been retrieved from more than one endpoint, all of
 * the endpoints and their message queues in this session have to be detached when
 * process exits. In this case, the active MsgQueueStatusEntry will be detached
 * first in retrieve_exit_callback. Thus, no need to detach it again in
 * retrieve_xact_abort_callback.
 *
 * shmem_exit()
 * --> ... (other before shmem callback if exists)
 * --> retrieve_exit_callback
 *	   --> cancel sender if needed.
 *	   --> detach all message queue dsm
 * --> ShutdownPostgres (the last before shmem callback)
 *	   --> AbortOutOfAnyTransaction
 *		   --> AbortTransaction
 *			   --> CallXactCallbacks
 *				   --> retrieve_xact_abort_callback
 *		   --> CleanupTransaction
 * --> dsm_backend_shutdown
 *
 * For Normal Transaction Aborts:
 * Retriever clean up job will be done in xact abort callback
 * retrieve_xact_abort_callback which will only clean the active
 * MsgQueueStatusEntry.
 *
 * Question:
 * Is it better to detach the dsm we created/attached before dsm_backend_shutdown?
 * Or we can let dsm_backend_shutdown do the detach for us, so we don't need
 * register call back in before_shmem_exit.
 */
static void
retrieve_exit_callback(int code, Datum arg)
{
	HASH_SEQ_STATUS status;
	MsgQueueStatusEntry *entry;

	elog(DEBUG3, "CDB_ENDPOINTS: retrieve exit callback");

	/* Nothing to do if hashtable not set up */
	if (msgQueueHTB == NULL)
		return;

	/* If the MQ entry has not be retrieved in this run. */
	if (currentMQEntry)
	{
		detach_endpoint(currentMQEntry, true);
	}

	/* Cancel all partially retrieved endpoints in this retrieve session */
	hash_seq_init(&status, msgQueueHTB);
	while ((entry = (MsgQueueStatusEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->retrieveStatus != RETRIEVE_STATUS_FINISH)
			retrieve_cancel_action(
								   entry->endpointName,
								   "Endpoint retrieve session quit, "
					   "all unfinished endpoint backends will be cancelled");
		if (entry->mqSeg)
		{
			/* It could have been detached already when finish. */
			detach_receiver_mq(entry);
		}
	}
	msgQueueHTB = NULL;
	ClearParallelCursorExecRole();
}

/*
 * Retrieve role xact abort callback.
 *
 * If normal abort, detach_endpoint and retrieve_cancel_action will only
 * be called once in current function for current endpoint_name.
 *
 * Buf if it's proc exit, these two methods will be called twice for current
 * endpoint_name. Since we call these two methods before dsm detach.
 */
static void
retrieve_xact_abort_callback(XactEvent ev, void *vp)
{
	if (ev == XACT_EVENT_ABORT)
	{
		elog(DEBUG3, "CDB_ENDPOINT: retrieve xact abort callback");
		if (EndpointCtl.GpPrceRole == PRCER_RECEIVER &&
			EndpointCtl.sessionID != InvalidSession && currentMQEntry)
		{
			if (currentMQEntry->retrieveStatus != RETRIEVE_STATUS_FINISH)
				retrieve_cancel_action(currentMQEntry->endpointName,
									   "Endpoint retrieve statement aborted");
			detach_endpoint(currentMQEntry, true);
		}
		ClearParallelCursorExecRole();
	}
}

/*
 * Retrieve role sub xact abort callback.
 */
static void
retrieve_subxact_abort_callback(SubXactEvent event, SubTransactionId mySubid,
								SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		retrieve_xact_abort_callback(XACT_EVENT_ABORT, NULL);
	}
}
