/*
 * cdbendpoint.c
 *	Functions to export the query results from endpoints on segments
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * reference:
 * README.endpoint.md
 * https://github.com/greenplum-db/gpdb/wiki/Greenplum-to-Greenplum
 *
 */

#include "postgres.h"

#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cdb/cdbendpoint.h"

#include "access/xact.h"
#include "access/tupdesc.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "executor/tqueue.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq/libpq.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"

/*
 * Macros
 */
#define ep_log(level, ...) \
do { \
	if (!StatusInAbort) \
		elog(level, __VA_ARGS__); \
} \
while (0)

#define ENDPOINT_TUPLE_QUEUE_SIZE		65536  /* This value is copy from PG's PARALLEL_TUPLE_QUEUE_SIZE */

#define BITS_PER_BITMAPWORD 32
#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

#define ENDPOINT_KEY_TUPLE_DESC_LEN     1
#define ENDPOINT_KEY_TUPLE_DESC         2
#define ENDPOINT_KEY_TUPLE_QUEUE        3

#define SHMEM_TOKENDSMCTX "ShareTokenDSMCTX"
#define SHMEM_TOKEN_CTX_SLOCK "SharedMemoryTokenCTXSlock"
#define SHMEM_ENDPOINT_LWLOCKS "SharedMemoryEndpointLWLocks"

typedef struct MsgQueueStatusEntry {
    int64                retrieveToken;
    dsm_segment*         mq_seg;
    shm_mq_handle*       mq_handle;
    TupleTableSlot*      retrieveTupleSlots;
    TupleQueueReader     *tQReader;
    enum RetrieveStatus  retrieveStatus;
} MsgQueueStatusEntry;

typedef struct EndpointSharedCTX {
    dsm_handle    token_info_handle;
    dsm_handle    endpoint_info_handle;
    int           tranche_id;              /* Tranche id for parallel cursor endpoint lwlocks.
                                           Read only, don't need acquire lock*/
	LWLockTranche tranche;
} EndpointSharedCTX;

typedef struct EndpointControl {
	int64 Gp_token;
	enum EndpointRole Gp_endpoint_role;
	List *Endpoint_tokens;
	List *Cursor_tokens;
} EndpointControl;

/*
 * Shared memory variables
 */
EndpointSharedCTX* endpointSC;
LWLockPadded  *endpointLWLocks;
slock_t *shareCTXLock;				/* spinlock for shared memory
                                       dsm endpoint info handle*/

#define EndpointsDSMLWLock (LWLock*)endpointLWLocks
#define TokensDSMLWLock (LWLock*)(endpointLWLocks + 1)

/*
 * Static variables
 */
static bool StatusInAbort = false;
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

/* Cache tuple descriptors for all tokens which have been retrieved in this
 * retrieve session */
static HTAB* MsgQueueHTB = NULL;
MsgQueueStatusEntry *currentMQEntry;

static SharedTokenDesc *SharedTokens = NULL;
static EndpointDesc *SharedEndpoints = NULL;
static volatile EndpointDesc *my_shared_endpoint = NULL;

static struct EndpointControl EndpointCtl = {
	InvalidToken, EPR_NONE, NIL, NIL
};

/*
 * Static functions
 */

/* Endpoint token info dsm */
static dsm_segment * create_token_info_dsm();
static dsm_segment * create_endpoint_info_dsm();
static void init_shared_endpoints(void *address);
static void init_shared_tokens(void *address);
static void parallel_cursor_exit_callback(int code, Datum arg);
static void endpoint_exit_callback(int code, Datum arg);

/* msg queue life cycle */
static void finish_endpoint_connection(void);
static void close_endpoint_connection(void);

/* sender which is an endpoint */
static void set_sender_pid(void);
static void create_and_connect_mq(TupleDesc tupleDesc);
static void wait_receiver(void);
static void sender_finish(void);
static void sender_close(int code, Datum arg);
static void unset_endpoint_sender_pid(volatile EndpointDesc * endPointDesc);
static void endpoint_cleanup(void);
static void sender_xact_abort_callback(XactEvent ev, void* vp);
static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                    SubTransactionId parentSubid, void *arg);

/* receiver which is a backend connected by retrieve mode */
static void init_conn_for_receiver(void);
static TupleDesc read_tuple_desc_info(shm_toc *toc);
static TupleTableSlot *receive_tuple_slot(void);
static void receiver_finish(void);
static void receiver_mq_close(void);
static void retrieve_cancel_action(int64 token);
static void unset_endpoint_receiver_pid(volatile EndpointDesc * endPointDesc);
static void retrieve_exit_callback(int code, Datum arg);
static void retrieve_xact_abort_callback(XactEvent ev, void* vp);
static void retrieve_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                      SubTransactionId parentSubid, void *arg);

/* utility */
static bool dbid_in_bitmap(int32 *bitmap, int16 dbid);
static void add_dbid_into_bitmap(int32 *bitmap, int16 dbid);
static int get_next_dbid_from_bitmap(int32 *bitmap, int prevbit);
static bool dbid_has_token(SharedToken token, int16 dbid);
static int16 dbid_to_contentid(int16 dbid);
static const char *endpoint_role_to_string(enum EndpointRole role);
static void check_token_valid(void);
extern char* get_token_name_format_str(void);
static void check_end_point_allocated(void);
static EndpointStatus *find_endpoint_status(EndpointStatus * status_array, int number, int64 token, int dbid);
static void set_attach_status(AttachStatus status);
static bool endpoint_on_qd(SharedToken token);
static volatile EndpointDesc *find_endpoint_by_token(int64 token);

Size
Endpoint_ShmemSize(void)
{
    Size		size;
    size = mul_size(sizeof(LWLockPadded), 2);
    size = add_size(size, sizeof(EndpointSharedCTX));
    return size;
}

void Endpoint_CTX_ShmemInit(void) {
	bool foundLWLocks,
	     is_shmem_ready;

	endpointLWLocks = (LWLockPadded *)ShmemInitStruct(
		SHMEM_ENDPOINT_LWLOCKS, sizeof(LWLockPadded) * 2, &foundLWLocks);
	Assert(foundLWLocks || !IsUnderPostmaster);

	endpointSC = (EndpointSharedCTX *)ShmemInitStruct(
		SHMEM_TOKENDSMCTX, sizeof(EndpointSharedCTX), &is_shmem_ready);

	if(foundLWLocks || is_shmem_ready) {
		Assert(foundLWLocks && is_shmem_ready);
		LWLockRegisterTranche(endpointSC->tranche_id, &endpointSC->tranche);
	} else {
		Assert(is_shmem_ready || !IsUnderPostmaster);
		if (!is_shmem_ready)
		{
			endpointSC->token_info_handle = DSM_HANDLE_INVALID;
			endpointSC->endpoint_info_handle = DSM_HANDLE_INVALID;
			endpointSC->tranche_id = LWLockNewTrancheId();
			endpointSC->tranche.name = "EndpointDSMLocks";
			endpointSC->tranche.array_base = endpointLWLocks;
			endpointSC->tranche.array_stride = sizeof(LWLockPadded);
		}
		if (!foundLWLocks) {
			LWLockRegisterTranche(endpointSC->tranche_id, &endpointSC->tranche);
			LWLockInitialize(EndpointsDSMLWLock, endpointSC->tranche_id);
			LWLockInitialize(TokensDSMLWLock, endpointSC->tranche_id);
		}
	}

	shareCTXLock = (slock_t *)ShmemInitStruct(
		SHMEM_TOKEN_CTX_SLOCK, sizeof(slock_t), &is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);
	if (!is_shmem_ready)
		SpinLockInit(shareCTXLock);
}

/* Endpoint DSM detach and callbacks */
static void parallel_cursor_exit_callback(int code, Datum arg) {
    ListCell    *l;
	dsm_segment *dsm_seg = (dsm_segment*) DatumGetPointer(arg);

    if (EndpointCtl.Cursor_tokens != NIL)
    {
        foreach(l, EndpointCtl.Cursor_tokens)
        {
            int64		token = atoll(lfirst(l));
            RemoveParallelCursorToken(token);
            pfree(lfirst(l));
        }
        list_free(EndpointCtl.Cursor_tokens);
        EndpointCtl.Cursor_tokens = NIL;
    }

	if (dsm_seg != NULL) {
		dsm_detach(dsm_seg);
		elog(LOG, "CDB_ENDPOINT: Detach endpoint token dsm.");
	}
}

/*
 * If endpoint/sender on exit, we need to do sender clean jobs.
 * No need to detach msg queue dsm cause dest receiver will handle it.
 **/
static void endpoint_exit_callback(int code, Datum arg) {
    dsm_segment *dsm_seg = (dsm_segment*) DatumGetPointer(arg);

    endpoint_cleanup();

	if (dsm_seg != NULL) {
		dsm_detach(dsm_seg);
        elog(LOG, "CDB_ENDPOINT: Detach endpoint dsm.");
	}
}

static void on_endpoint_dsm_detach_callback (dsm_segment* seg, Datum arg) {
	SharedEndpoints = NULL;
}

static void on_token_dsm_detach_callback (dsm_segment* seg, Datum arg) {
	SharedTokens = NULL;
}

static void on_endpoint_dsm_destroy_callback (dsm_segment* seg, Datum arg) {
	SpinLockAcquire(shareCTXLock);
	endpointSC->endpoint_info_handle = DSM_HANDLE_INVALID;
	SpinLockRelease(shareCTXLock);
}

static void on_token_dsm_destroy_callback (dsm_segment* seg, Datum arg) {
	SpinLockAcquire(shareCTXLock);
	endpointSC->token_info_handle = DSM_HANDLE_INVALID;
	SpinLockRelease(shareCTXLock);
}

void AttachOrCreateEndpointAndTokenDSM(void) {
    AttachOrCreateEndpointDsm(false);
	if (Gp_role == GP_ROLE_DISPATCH) {
        // Init token info dsm only on QD.
        AttachOrCreateTokenDsm(false);
	}
}

bool AttachOrCreateEndpointDsm(bool attachOnly) {
	dsm_segment* dsm_seg;

	if (SharedEndpoints) {
		return false;
	}
	Assert(SharedEndpoints == NULL);

	SpinLockAcquire(shareCTXLock);
	if (endpointSC->endpoint_info_handle == DSM_HANDLE_INVALID) {
		// create
		if (attachOnly) {
			SpinLockRelease(shareCTXLock);
			elog(LOG, "CDB_ENDPOINT: SKIP create endpoint dsm since required attach only.");
			return false;
		}
		dsm_seg = create_endpoint_info_dsm();
		elog(LOG, "CDB_ENDPOINT: Create endpoint dsm ...");
		if (dsm_seg) {
			endpointSC->endpoint_info_handle = dsm_segment_handle(dsm_seg);
			init_shared_endpoints(dsm_segment_address(dsm_seg));
		}
	} else {
	    // attach
		dsm_seg = dsm_attach(endpointSC->endpoint_info_handle);
		elog(LOG, "CDB_ENDPOINT: Attach endpoint dsm ...");
	}
	SpinLockRelease(shareCTXLock);
	if (dsm_seg == NULL) {
		ep_log(ERROR, "CDB_ENDPOINT: Could not create / map endpoint dynamic shared memory segment.");
		return false; // Should not reach this line.
	}
	dsm_pin_mapping(dsm_seg);
	on_dsm_detach(dsm_seg, on_endpoint_dsm_detach_callback, (Datum)0);
	on_dsm_destroy(dsm_seg, on_endpoint_dsm_destroy_callback, 0);
    before_shmem_exit(endpoint_exit_callback, PointerGetDatum(dsm_seg));
    RegisterSubXactCallback(sender_subxact_callback, NULL);
    RegisterXactCallback(sender_xact_abort_callback, NULL);
	SharedEndpoints = dsm_segment_address(dsm_seg);
	return true;
}

bool AttachOrCreateTokenDsm(bool attachOnly) {
	dsm_segment* dsm_seg;

    Assert(Gp_role == GP_ROLE_DISPATCH);
	if (SharedTokens) {
		return false;
	}
	Assert(SharedTokens == NULL);

	SpinLockAcquire(shareCTXLock);
	if (endpointSC->token_info_handle == DSM_HANDLE_INVALID) {
        // create
        if (attachOnly) {
			SpinLockRelease(shareCTXLock);
			elog(LOG, "CDB_ENDPOINT: SKIP create endpoint token dsm since required attach only.");
			return false;
        }
		dsm_seg = create_token_info_dsm();
        elog(LOG, "CDB_ENDPOINT: Create endpoint token dsm ...");
        if (dsm_seg) {
            endpointSC->token_info_handle = dsm_segment_handle(dsm_seg);
            init_shared_tokens(dsm_segment_address(dsm_seg));
        }
	} else {
	    // attach
		dsm_seg = dsm_attach(endpointSC->token_info_handle);
        elog(LOG, "CDB_ENDPOINT: Attach endpoint token dsm ...");
	}
    SpinLockRelease(shareCTXLock);
    if (dsm_seg == NULL) {
        ep_log(ERROR, "CDB_ENDPOINT: Could not create / map endpoint token dynamic shared memory segment.");
        return false; // Should not reach this line.
    }
    dsm_pin_mapping(dsm_seg);
    on_dsm_detach(dsm_seg, on_token_dsm_detach_callback, (Datum)0);
    on_dsm_destroy(dsm_seg, on_token_dsm_destroy_callback, 0);
    before_shmem_exit(parallel_cursor_exit_callback, PointerGetDatum(dsm_seg));
    SharedTokens = dsm_segment_address(dsm_seg);
    return true;
}

static struct dsm_segment * create_endpoint_info_dsm() {
	// Calculate size of the dsm
	Size		size;
    dsm_segment *dsm_seg = NULL;

	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc));
    dsm_seg = dsm_create(size);
    return dsm_seg;
}

static struct dsm_segment* create_token_info_dsm() {
	// Calculate size of the dsm
	Size		size;
	dsm_segment *dsm_seg = NULL;

	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(SharedTokenDesc));
	dsm_seg = dsm_create(size);
	return dsm_seg;
}

static void init_shared_endpoints(void *address) {
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

static void init_shared_tokens(void *address) {
    SharedToken tokens = (SharedToken) address;
    for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
    {
        tokens[i].token = InvalidToken;
        memset(tokens[i].cursor_name, 0, NAMEDATALEN);
        tokens[i].session_id = InvalidSession;
        tokens[i].user_id = InvalidOid;
    }
}

/*
 * Generate an unique int64 token
 */
int64
GetUniqueGpToken(void)
{
	int64			token;
	char            *token_str;
	struct timespec ts;

	Assert(SharedTokens);

	clock_gettime(CLOCK_MONOTONIC, &ts);

	LWLockAcquire(TokensDSMLWLock, LW_SHARED);

	srand(ts.tv_nsec);
	REGENERATE:
	token = llabs(((int64)rand() << 32) | rand());
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token == SharedTokens[i].token)
			goto REGENERATE;
	}

	LWLockRelease(TokensDSMLWLock);

    MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    token_str = palloc0(21);		/* length 21 = length of max int64 value + '\0' */
    pg_lltoa(token, token_str);
    /* If the endpoint on QD, we will have duplicate tokens in this list, but it's ok
       since we assume the list is not so long and the burden likes nothing. */
    EndpointCtl.Cursor_tokens = lappend(EndpointCtl.Cursor_tokens, token_str);
    MemoryContextSwitchTo(oldcontext);
	return token;
}

/*
 * Memory the information of tokens on all or which segments, while DECLARE
 * PARALLEL CURSOR
 *
 * The UDF gp_endpoints_info() queries the information.
 */
void
AddParallelCursorToken(int64 token, const char *name, int session_id, Oid user_id,
					   bool all_seg, List *seg_list)
{
	int			i;

	Assert(token != InvalidToken && name != NULL
		   && session_id != InvalidSession);
	Assert(SharedTokens);

	LWLockAcquire(TokensDSMLWLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault to set end-point shared memory slot full. */
	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR(EndpointSharedMemorySlotFull);

	if (typeE == FaultInjectorTypeFullMemorySlot)
	{
		const char *FJ_CURSOR = "FAULT_INJECTION_CURSOR";

		for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
		{
			if (SharedTokens[i].token == InvalidToken)
			{
				/* pretend to set a valid token */
				strncpy(SharedTokens[i].cursor_name, FJ_CURSOR, strlen(FJ_CURSOR));
				SharedTokens[i].session_id = session_id;
				SharedTokens[i].token = DummyToken;
				SharedTokens[i].user_id = user_id;
				SharedTokens[i].all_seg = all_seg;
			}
		}
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
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
			strncpy(SharedTokens[i].cursor_name, name, strlen(name));
			SharedTokens[i].session_id = session_id;
			SharedTokens[i].token = token;
			SharedTokens[i].user_id = user_id;
			SharedTokens[i].all_seg = all_seg;
			if (seg_list != NIL)
			{
				ListCell   *l;

				foreach(l, seg_list)
				{
					int16		contentid = lfirst_int(l);

					add_dbid_into_bitmap(SharedTokens[i].dbIds,
										 contentid_get_dbid(contentid,
															GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
															false));
					SharedTokens[i].endpoint_cnt++;
				}
			}
			elog(LOG, "added a new token: " INT64_FORMAT ", session id: %d, cursor name: %s, into shared memory",
				 token, session_id, SharedTokens[i].cursor_name);
			break;
		}
	}

	LWLockRelease(TokensDSMLWLock);

	/* no empty entry to save this token */
	if (i == MAX_ENDPOINT_SIZE)
	{
		ep_log(ERROR, "can't add a new token %s into shared memory", printToken(token));
	}

}

/*
 * Create the dest receiver of parallel cursor
 */
DestReceiver *
CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc)
{
	set_sender_pid();
	check_end_point_allocated();
	currentMQEntry = palloc0(sizeof(MsgQueueStatusEntry));
	currentMQEntry->retrieveToken = EndpointCtl.Gp_token;
	currentMQEntry->mq_seg = NULL;
	currentMQEntry->mq_handle = NULL;
	currentMQEntry->retrieveStatus = RETRIEVE_STATUS_INIT;
	currentMQEntry->retrieveTupleSlots = NULL;
	currentMQEntry->tQReader = NULL;
	create_and_connect_mq(tupleDesc);
	return CreateTupleQueueDestReceiver(currentMQEntry->mq_handle);
}

void
DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest)
{
	/* wait for receiver to retrieve the first row */
    wait_receiver();
	/*tqueueShutdownReceiver() will call shm_mq_detach(), so need to call it before sender_close()*/
	(*endpointDest->rShutdown) (endpointDest);
	(*endpointDest->rDestroy) (endpointDest);
	sender_close(0, (Datum)0);

	sender_finish();
	set_attach_status(Status_Finished);
    ClearGpToken();
    ClearEndpointRole();
}

static void
set_sender_pid(void)
{
	int			i;
	int			found_idx = -1;

	Assert(SharedEndpoints);

	if (EndpointCtl.Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s could not allocate endpoint slot",
			   endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

	if (my_shared_endpoint && my_shared_endpoint->token != InvalidToken)
		ep_log(ERROR, "endpoint is already allocated");

	check_token_valid();

	LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

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

	LWLockRelease(EndpointsDSMLWLock);

	if (!my_shared_endpoint)
		ep_log(ERROR, "failed to allocate endpoint");
}

static void
finish_endpoint_connection(void)
{
    switch (EndpointCtl.Gp_endpoint_role)
    {
        case EPR_SENDER:
            sender_finish();
            break;
        case EPR_RECEIVER:
            receiver_finish();
            break;
        default:
            ep_log(ERROR, "invalid endpoint role");
    }
}

static void
close_endpoint_connection(void)
{
    check_token_valid();

    switch (EndpointCtl.Gp_endpoint_role)
    {
        case EPR_SENDER:
            sender_close(0, (Datum)0);
            break;
        case EPR_RECEIVER:
            receiver_mq_close();
            break;
        default:
            ep_log(ERROR, "invalid endpoint role");
    }
}

void
UnsetSenderPidOfToken(int64 token)
{
	volatile EndpointDesc *endPointDesc = find_endpoint_by_token(token);
	if (!endPointDesc)
	{
		ep_log(ERROR, "no valid endpoint info for token " INT64_FORMAT "", token);
	}
	unset_endpoint_sender_pid(endPointDesc);
}

/*
 * Remove the target token information from token shared memory.
 * We need clean the token from dsm for cursor close and exception happens.
 *
 * If PANIC exception happens, proc exit, the function will be called twice.
 * Cause the dsm get detached in shmem_exit. So we need make sure we remove token
 * info before detach.
 *
 * The system do PortalDrop after our dsm detach for exception. So when PortalDrop
 * happens, it's actually done the clean.
 */
void
RemoveParallelCursorToken(int64 token)
{
	Assert(token != InvalidToken);
	bool		endpoint_on_QD = false,
		found = false;
	List	   *seg_list = NIL;
	if (SharedTokens == NULL) {
		elog(LOG, "CDB_ENDPOINT: <RemoveParallelCursorToken> Need remove token " INT64_FORMAT ", "
				  "but seems already destroy the endpoint token DSM, we expect the token should "
				  "already removed before detach dsm.", token);
		return;
	}
	LWLockAcquire(TokensDSMLWLock, LW_EXCLUSIVE);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == token)
		{
			found = true;
			if (endpoint_on_qd(&SharedTokens[i]))
			{
				endpoint_on_QD = true;
			}
			else
			{
				if (!SharedTokens[i].all_seg)
				{
					int16		x = -1;

					while ((x = get_next_dbid_from_bitmap(SharedTokens[i].dbIds, x)) >= 0)
					{
						seg_list = lappend_int(seg_list, dbid_to_contentid(x));
					}
					Assert(seg_list->length == SharedTokens[i].endpoint_cnt);
				}
			}

			elog(LOG, "CDB_ENDPOINT: <RemoveParallelCursorToken> removed token: " INT64_FORMAT ", session id: %d, cursor name: %s from shared memory",
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

	LWLockRelease(TokensDSMLWLock);

	if (found)
	{
		/* free end-point */

		if (endpoint_on_QD)
		{
			FreeEndpointOfToken(token);
		}
		else
		{
			char		cmd[255];

			sprintf(cmd, "SET gp_endpoints_token_operation='f" INT64_FORMAT "'", token);
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
 * Allocate an endpoint slot of the shared memory
 */
void
AllocEndpointOfToken(int64 token)
{
	int			i;
	int			found_idx = -1;
	char	   *token_str;

	if (token == InvalidToken)
		ep_log(ERROR, "allocate endpoint of invalid token ID");
	Assert(SharedEndpoints);
	LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

#ifdef FAULT_INJECTOR
	/* inject fault "skip" to set end-point shared memory slot full */


	FaultInjectorType_e typeE = SIMPLE_FAULT_INJECTOR(EndpointSharedMemorySlotFull);

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
	}
	else if (typeE == FaultInjectorTypeRevertMemorySlot)
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
		token_str = palloc0(21);		/* length 21 = length of max int64 value + '\0' */
		pg_lltoa(token, token_str);
		EndpointCtl.Endpoint_tokens = lappend(EndpointCtl.Endpoint_tokens, token_str);
		MemoryContextSwitchTo(oldcontext);
	}

	LWLockRelease(EndpointsDSMLWLock);

	if (found_idx == -1)
		ep_log(ERROR, "failed to allocate endpoint");
}

/*
 * Free an endpoint slot of the shared memory
 */
void
FreeEndpointOfToken(int64 token)
{
	volatile EndpointDesc *endPointDesc = find_endpoint_by_token(token);

	if (!endPointDesc)
		return;

    if (!endPointDesc && !endPointDesc->empty)
        ep_log(ERROR, "not an valid endpoint");

    unset_endpoint_sender_pid(endPointDesc);

    LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);
    endPointDesc->database_id = InvalidOid;
    endPointDesc->token = InvalidToken;
    endPointDesc->handle = DSM_HANDLE_INVALID;
    endPointDesc->session_id = InvalidSession;
    endPointDesc->user_id = InvalidOid;
    endPointDesc->empty = true;
    LWLockRelease(EndpointsDSMLWLock);
}

/*
 * Endpoint actions, push, free or unset
 */
void
assign_gp_endpoints_token_operation(const char *newval, void *extra)
{
	const char *token;
	int64	tokenid;

	/*
	 * May be called in AtEOXact_GUC() to set to default value (i.e. empty
	 * string)
	 */
	if (newval == NULL || strlen(newval) == 0)
		return;

	token = newval + 1;
	tokenid = atoll(token);

	if (tokenid != InvalidToken && Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
	{
		if (AttachOrCreateEndpointDsm(false)) {
		    // Register endpoint/sender proc exit callback if not registered before.
		    // The endpoint is on QE.
		}
		switch (newval[0])
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
				elog(ERROR, "Failed to SET gp_endpoints_token_operation: %s", newval);
		}
	}
}

static void
create_and_connect_mq(TupleDesc tupleDesc)
{
    check_token_valid();
	Assert(currentMQEntry);
    if (currentMQEntry->mq_handle != NULL)
	    return;

    dsm_segment       *dsm_seg;
    shm_toc           *toc;
    shm_mq            *mq;
    shm_toc_estimator toc_est;
    Size              toc_size;
    int			      tupdesc_len;
    char              *tupdesc_ser;
    char	          *tdlen_space;
    char              *tupdesc_space;

    /*
     * Calculate dsm size, size = toc meta + toc_nentry(3) * entry size + tuple desc
     * length size + tuple desc size + queue size.
    */
    TupleDescNode *node = makeNode(TupleDescNode);
    node->natts = tupleDesc->natts;
    node->tuple = tupleDesc;
    tupdesc_ser = serializeNode((Node *) node, &tupdesc_len, NULL /* uncompressed_size */ );

    shm_toc_initialize_estimator(&toc_est);
    shm_toc_estimate_chunk(&toc_est, sizeof(tupdesc_len));
    shm_toc_estimate_chunk(&toc_est, tupdesc_len);
    shm_toc_estimate_keys(&toc_est, 2);

    shm_toc_estimate_chunk(&toc_est, ENDPOINT_TUPLE_QUEUE_SIZE);
    shm_toc_estimate_keys(&toc_est, 1);
    toc_size = shm_toc_estimate(&toc_est);

    LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);
    dsm_seg = dsm_create(toc_size);
    if (dsm_seg == NULL) {
        LWLockRelease(EndpointsDSMLWLock);
        close_endpoint_connection();
        ep_log(ERROR, "failed to create shared message queue for send tuples.");
    }
    my_shared_endpoint->handle = dsm_segment_handle(dsm_seg);
    LWLockRelease(EndpointsDSMLWLock);
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

static void
wait_receiver(void)
{
    while (true)
    {
        int			wr;

        CHECK_FOR_INTERRUPTS();

        if (QueryFinishPending)
            break;

        /* Check the QD dispatcher connection is lost */
        unsigned char firstchar;
        int			r;

        pq_startmsgread();
        r = pq_getbyte_if_available(&firstchar);
        if (r < 0)
        {
            /* unexpected error or EOF */
            ep_log(ERROR, "unexpected EOF on query dispatcher connection");
        }
        else if (r > 0)
        {
            /* unexpected error */
            ep_log(ERROR, "query dispatcher should get nothing until QE backend finished processing");
        }
        else
        {
            /* no data available without blocking */
            pq_endmsgread();
            /* continue processing as normal case */
        }

        ep_log(LOG, "sender wait latch in wait_receiver()");
        wr = WaitLatch(&my_shared_endpoint->ack_done,
                       WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
                       POLL_FIFO_TIMEOUT);
        if (wr & WL_TIMEOUT)
            continue;

        if (wr & WL_POSTMASTER_DEATH)
        {
            close_endpoint_connection();
            elog(LOG, "CDB_ENDPOINT: postmaster exit, close shared memory message queue.");
            proc_exit(0);
        }

        Assert(wr & WL_LATCH_SET);
        ep_log(LOG, "sender reset latch in wait_receiver()");
        ResetLatch(&my_shared_endpoint->ack_done);
        break;
    }
}

static void
sender_finish(void)
{
	/* wait for receiver to finish retrieving */
    wait_receiver();
}

static void
sender_close(int code, Datum arg)
{
    elog(LOG, "CDB_ENDPOINT: Sender message queue detaching.");
	// If error happened, currentMQEntry could be none.
    if (currentMQEntry != NULL && currentMQEntry->mq_seg != NULL) {
        dsm_detach(currentMQEntry->mq_seg);
		if (currentMQEntry->retrieveTupleSlots != NULL)
			ExecDropSingleTupleTableSlot(currentMQEntry->retrieveTupleSlots);
		pfree(currentMQEntry);
		currentMQEntry = NULL;
    }
}

static void
unset_endpoint_sender_pid(volatile EndpointDesc * endPointDesc)
{
    pid_t		pid;

    if (!endPointDesc && !endPointDesc->empty)
        return;

    /*
     * Since the receiver is not in the session, sender has the duty to cancel
     * it
     */
    unset_endpoint_receiver_pid(endPointDesc);

    while (true)
    {
        pid = InvalidPid;

        LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

        pid = endPointDesc->sender_pid;

        /*
         * Only reset by this process itself, other process just send signal
         * to sendpid
         */
        if (pid == MyProcPid)
        {
            endPointDesc->sender_pid = InvalidPid;
            ResetLatch(&endPointDesc->ack_done);
            DisownLatch(&endPointDesc->ack_done);
        }

        LWLockRelease(EndpointsDSMLWLock);
        if (pid != InvalidPid && pid != MyProcPid)
        {
            if (kill(pid, SIGINT) < 0)
            {
                /* no permission or non-existing */
                if (errno == EPERM || errno == ESRCH)
                    break;
                else
                    elog(WARNING, "failed to kill sender process(pid: %d): %m", (int) pid);
            }
        }
        else
            break;
    }
}

/*
 * If no PANIC exception happens, FreeEndpointOfToken will only
 * be called once in current function.
 *
 * Buf if the PANIC happens, proc exit, FreeEndpointOfToken method will be called twice.
 * Since we call it during dsm detach.
 *
 * The sender should only have one in EndpointCtl.TokensInXact list.
 * */
static void endpoint_cleanup(void) {
    ListCell *l;
    StatusInAbort = true;
    if (EndpointCtl.Endpoint_tokens != NIL)
    {
        foreach(l, EndpointCtl.Endpoint_tokens)
        {
            int64		token = atoll(lfirst(l));
            FreeEndpointOfToken(token);
            pfree(lfirst(l));
        }
        list_free(EndpointCtl.Endpoint_tokens);
        EndpointCtl.Endpoint_tokens = NIL;
    }
    my_shared_endpoint = NULL;
    StatusInAbort = false;
    ClearGpToken();
    ClearEndpointRole();
}

static void sender_xact_abort_callback(XactEvent ev, void* vp) {
    if (ev == XACT_EVENT_ABORT) {
        endpoint_cleanup();
    }
}

static void sender_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                    SubTransactionId parentSubid, void *arg) {
    if (event == SUBXACT_EVENT_ABORT_SUB) {
        endpoint_cleanup();
    }
}

/*
 * Return true if the user has parallel cursor/endpoint of the token
 *
 * Used by retrieve role authentication
 */
bool
FindEndpointTokenByUser(Oid user_id, const char *token_str)
{
	bool		isFound = false;
	AttachOrCreateEndpointDsm(true);
	before_shmem_exit(retrieve_exit_callback, (Datum)0);
	RegisterSubXactCallback(retrieve_subxact_callback, NULL);
    RegisterXactCallback(retrieve_xact_abort_callback, NULL);
    elog(LOG, "RegisterXactCallback --------------");
	if (SharedEndpoints == NULL) {
		return isFound;
	}
	Assert(SharedEndpoints);
	LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);

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

			char	   *token = printToken(SharedEndpoints[i].token);

			if (strcmp(token, token_str) == 0)
			{
				isFound = true;
				pfree(token);
				break;
			}
			pfree(token);
		}
	}

	LWLockRelease(EndpointsDSMLWLock);
	return isFound;
}

void
AttachEndpoint(void)
{
	int			i;
	bool		isFound = false;
	bool		already_attached = false;		/* now is attached? */
	bool		is_self_pid = false;	/* indicate this process has been
										 * attached to this token before */
	bool		is_other_pid = false;	/* indicate other process has been
										 * attached to this token before */
	bool		is_invalid_sendpid = false;
	pid_t		attached_pid = InvalidPid;

	if (EndpointCtl.Gp_endpoint_role != EPR_RECEIVER)
		ep_log(ERROR, "%s could not attach endpoint", endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

	if (my_shared_endpoint)
		ep_log(ERROR, "endpoint is already attached");

    if (SharedEndpoints == NULL) {
        ep_log(ERROR, "No endpoint exists.");
    }
	check_token_valid();

	LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].database_id == MyDatabaseId &&
			SharedEndpoints[i].token == EndpointCtl.Gp_token &&
			SharedEndpoints[i].user_id == GetUserId() &&
			!SharedEndpoints[i].empty)
		{
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

			if (SharedEndpoints[i].receiver_pid == MyProcPid)	/* already attached by
																 * this process before */
			{
				is_self_pid = true;
			}
			else if (SharedEndpoints[i].receiver_pid != InvalidPid)		/* already attached by
																		 * other process before */
			{
				is_other_pid = true;
				attached_pid = SharedEndpoints[i].receiver_pid;
				break;
			}
			else
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

	LWLockRelease(EndpointsDSMLWLock);

	if (is_invalid_sendpid)
	{
		ep_log(ERROR, "the PARALLEL CURSOR related to endpoint token %s is not EXECUTED",
			   printToken(EndpointCtl.Gp_token));
	}

	if (already_attached)
		ep_log(ERROR, "endpoint %s is been retrieved by receiver(pid: %d)",
			               printToken(EndpointCtl.Gp_token), attached_pid);

	if (is_other_pid)
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
			        errmsg("endpoint %s has been already attached by receiver(pid: %d)",
			               printToken(EndpointCtl.Gp_token), attached_pid),
			        errdetail("One endpoint only can be attached by one retrieve session "
					          "for each 'EXECUTE PARALLEL CURSOR'")));

	if (!my_shared_endpoint)
		ep_log(ERROR, "failed to attach non-existing endpoint of token %s", printToken(EndpointCtl.Gp_token));

	/*
	 * Search all tokens that retrieved in this session, set
	 * CurrentRetrieveToken to it's array index
	 */
	if (MsgQueueHTB == NULL) {
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
		currentMQEntry->retrieveStatus = RETRIEVE_STATUS_INVALID;
		currentMQEntry->retrieveTupleSlots = NULL;
	}
	if (!is_self_pid)
	{
		currentMQEntry->retrieveStatus = RETRIEVE_STATUS_INIT;
	}

}

static void
init_conn_for_receiver(void)
{
    check_token_valid();
    Assert(currentMQEntry);

    dsm_segment* dsm_seg;
    LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);
    if (currentMQEntry->mq_seg && dsm_segment_handle(currentMQEntry->mq_seg) == my_shared_endpoint->handle) {
        LWLockRelease(EndpointsDSMLWLock);
        return;
    }
    if (currentMQEntry->mq_seg) {
        dsm_detach(currentMQEntry->mq_seg);
    }
    dsm_seg = dsm_attach(my_shared_endpoint->handle);
    LWLockRelease(EndpointsDSMLWLock);
    if (dsm_seg == NULL) {
        close_endpoint_connection();
        ep_log(ERROR, "attach to shared message queue failed.");
    }
    dsm_pin_mapping(dsm_seg);
    shm_toc * toc = shm_toc_attach(EndpointCtl.Gp_token, dsm_segment_address(dsm_seg));
    shm_mq     *mq = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_QUEUE);
    shm_mq_set_receiver(mq, MyProc);
    currentMQEntry->mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
    currentMQEntry->mq_seg = dsm_seg;
}

static TupleDesc
read_tuple_desc_info(shm_toc *toc)
{
	int *tdlen_plen;

	char	   *tdlen_space;
	char       *tupdesc_space;

	tdlen_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC_LEN);
	tdlen_plen = (int *)tdlen_space;

	tupdesc_space = shm_toc_lookup(toc, ENDPOINT_KEY_TUPLE_DESC);

	TupleDescNode *tupdescnode = (TupleDescNode*) deserializeNode(tupdesc_space, *tdlen_plen);
	return tupdescnode->tuple;
}

/*
 * Return the tuple description for retrieve statement
 */
TupleDesc
TupleDescOfRetrieve(void)
{
	TupleDesc   td;
	MemoryContext oldcontext;

	Assert(currentMQEntry);
	if (currentMQEntry->retrieveStatus < RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/*
		 * Store the result slot all the retrieve mode QE life cycle, we only
		 * have one chance to built it.
		 */

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		init_conn_for_receiver();

		Assert(currentMQEntry->mq_handle);
		shm_toc * toc = shm_toc_attach(GpToken(), dsm_segment_address(currentMQEntry->mq_seg));
		td = read_tuple_desc_info(toc);
		currentMQEntry->tQReader = CreateTupleQueueReader(currentMQEntry->mq_handle, td);

		if (currentMQEntry->retrieveTupleSlots != NULL)
				ExecClearTuple(currentMQEntry->retrieveTupleSlots);
		currentMQEntry->retrieveTupleSlots = MakeTupleTableSlot();
		ExecSetSlotDescriptor(currentMQEntry->retrieveTupleSlots, td);
		currentMQEntry->retrieveStatus = RETRIEVE_STATUS_GET_TUPLEDSCR;

		MemoryContextSwitchTo(oldcontext);
	}

	Assert(currentMQEntry->retrieveTupleSlots);
	Assert(currentMQEntry->retrieveTupleSlots->tts_tupleDescriptor);

	return currentMQEntry->retrieveTupleSlots->tts_tupleDescriptor;
}

/*
 * Send the results to dest receiver of retrieve statement
 */
void
RetrieveResults(RetrieveStmt * stmt, DestReceiver *dest)
{
	TupleTableSlot *result;
	int64		retrieve_count;
	Assert(currentMQEntry);

	retrieve_count = stmt->count;
	if (retrieve_count <= 0 && !stmt->is_all)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("RETRIEVE statement only supports forward scan, count should not be: %ld", retrieve_count)));
	}

	if (currentMQEntry->retrieveStatus < RETRIEVE_STATUS_FINISH)
	{
		//init_endpoint_connection();

		while (retrieve_count > 0)
		{
			result = receive_tuple_slot();
			if (!result)
			{
				break;
			}
			(*dest->receiveSlot) (result, dest);
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
				(*dest->receiveSlot) (result, dest);
			}
		}

		finish_endpoint_connection();
	}

	DetachEndpoint(false);
	ClearEndpointRole();
	ClearGpToken();
}

static TupleTableSlot *
receive_tuple_slot(void)
{
	TupleTableSlot *result = NULL;
	HeapTuple	tup = NULL;
	bool		readerdone = false;

	CHECK_FOR_INTERRUPTS();

	Assert(currentMQEntry->tQReader != NULL);

	/* at the first time to retrieve data */
	if (currentMQEntry->retrieveStatus == RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/* try to receive data with nowait, so that empty result will not hang here */
		tup = TupleQueueReaderNext(currentMQEntry->tQReader, true, &readerdone);

		currentMQEntry->retrieveStatus = RETRIEVE_STATUS_GET_DATA;

		/* at the first time to retrieve data, tell sender not to wait at wait_receiver()*/
		ep_log(LOG, "receiver set latch in receive_tuple_slot() at the first time to retrieve data");
		SetLatch(&my_shared_endpoint->ack_done);
	}

	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR(FetchTuplesFromEndpoint);
	RESUME_INTERRUPTS();

	/* re retrieve data in wait mode
	 * if not the first time retrieve data
	 * or if the first time retrieve an invalid data, but not finish */
	if(readerdone==false && tup==NULL)
	{
		tup = TupleQueueReaderNext(currentMQEntry->tQReader, false, &readerdone);
	}

	/* readerdone returns true only after sender detach mq */
	if (readerdone)
	{
		Assert(!tup);
		DestroyTupleQueueReader(currentMQEntry->tQReader);
		currentMQEntry->tQReader = NULL;
		/* when finish retrieving data, tell sender not to wait at sender_finish()*/
		ep_log(LOG, "receiver set latch in receive_tuple_slot() when finish retrieving data");
		SetLatch(&my_shared_endpoint->ack_done);
		currentMQEntry->retrieveStatus = RETRIEVE_STATUS_FINISH;
		return NULL;
	}

	if (HeapTupleIsValid(tup))
	{
		Assert(currentMQEntry->mq_handle);
		Assert(currentMQEntry->retrieveTupleSlots);
		ExecClearTuple(currentMQEntry->retrieveTupleSlots);
		result = currentMQEntry->retrieveTupleSlots;
		ExecStoreHeapTuple(tup,		/* tuple to store */
						   result,	/* slot in which to store the tuple */
						   InvalidBuffer,	/* buffer associated with this tuple */
						   false);	/* slot should not pfree tuple */
		return result;
	}
	return result;
}

static void
receiver_finish(void)
{
/* for now, receiver does nothing after finished */
}

static void
receiver_mq_close(void)
{
	bool found;

	// If error happened, currentMQEntry could be none.
    if (currentMQEntry != NULL && currentMQEntry->mq_seg != NULL) {
        dsm_detach(currentMQEntry->mq_seg);
        currentMQEntry->mq_seg = NULL;
        currentMQEntry->mq_handle = NULL;
        currentMQEntry->retrieveStatus = RETRIEVE_STATUS_INVALID;
        if (currentMQEntry->retrieveTupleSlots != NULL)
            ExecDropSingleTupleTableSlot(currentMQEntry->retrieveTupleSlots);
        currentMQEntry->retrieveTupleSlots = NULL;
        currentMQEntry = (MsgQueueStatusEntry *) hash_search(
            MsgQueueHTB, &currentMQEntry->retrieveToken, HASH_REMOVE, &found);
        if (!currentMQEntry)
            elog(ERROR, "CDB_ENDPOINT: Message queue status element destroy failed.");
        currentMQEntry = NULL;
    }
}

/*
 * When detach endpoint, if this process have not yet finish this mq reading,
 * then don't reset it's pid, so that we can know the process is the first time
 * of attaching endpoint (need to re-read tuple descriptor).
 *
 * Note: don't drop the result slot, we only have one chance to built it.
 */
void
DetachEndpoint(bool reset_pid)
{
	if (EndpointCtl.Gp_endpoint_role != EPR_RECEIVER ||
		!my_shared_endpoint ||
		EndpointCtl.Gp_token == InvalidToken)
		return;

	if (EndpointCtl.Gp_endpoint_role != EPR_RECEIVER)
		ep_log(ERROR, "%s could not attach endpoint", endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

	check_token_valid();

	LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		if (my_shared_endpoint->token != EndpointCtl.Gp_token)
			ep_log(LOG, "unmatched token, expected %s but it's %s",
				   printToken(EndpointCtl.Gp_token), printToken(my_shared_endpoint->token));

		if (my_shared_endpoint->receiver_pid != MyProcPid)
			ep_log(ERROR, "unmatched pid, expected %d but it's %d",
				   MyProcPid, my_shared_endpoint->receiver_pid);
	}
	PG_CATCH();
	{
		LWLockRelease(EndpointsDSMLWLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (reset_pid)
	{
		my_shared_endpoint->receiver_pid = InvalidPid;
	}

	/* Don't set if Status_Finished */
	if (my_shared_endpoint->attach_status == Status_Attached)
	{
		my_shared_endpoint->attach_status = Status_Prepared;
	}
	LWLockRelease(EndpointsDSMLWLock);

    my_shared_endpoint = NULL;
    currentMQEntry = NULL;
}

static void
retrieve_cancel_action(int64 token)
{
    Assert(SharedEndpoints);
    if (EndpointCtl.Gp_endpoint_role != EPR_RECEIVER)
        ep_log(ERROR, "receiver cancel action is triggered by accident");

    LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

    for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
    {
        if (SharedEndpoints[i].token == token && SharedEndpoints[i].receiver_pid == MyProcPid)
        {
            SharedEndpoints[i].receiver_pid = InvalidPid;
            SharedEndpoints[i].attach_status = Status_NotAttached;
            elog(LOG, "CDB_ENDPOINT: pg_signal_backend");
            pg_signal_backend(SharedEndpoints[i].sender_pid, SIGINT, NULL);
            break;
        }
    }

    LWLockRelease(EndpointsDSMLWLock);
}

static void
unset_endpoint_receiver_pid(volatile EndpointDesc * endPointDesc)
{
    pid_t		receiver_pid;
    bool		is_attached;

    if (!endPointDesc && !endPointDesc->empty)
        return;

    while (true)
    {
        receiver_pid = InvalidPid;
        is_attached = false;

        LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

        receiver_pid = endPointDesc->receiver_pid;
        is_attached = endPointDesc->attach_status == Status_Attached;

        if (receiver_pid == MyProcPid)
        {
            endPointDesc->receiver_pid = InvalidPid;
            endPointDesc->attach_status = Status_NotAttached;
        }

        LWLockRelease(EndpointsDSMLWLock);
        if (receiver_pid != InvalidPid && is_attached && receiver_pid != MyProcPid)
        {
            if (kill(receiver_pid, SIGINT) < 0)
            {
                /* no permission or non-existing */
                if (errno == EPERM || errno == ESRCH)
                    break;
                else
                    elog(WARNING, "failed to kill sender process(pid: %d): %m", (int) receiver_pid);
            }
        }
        else
            break;
    }
}

/*
 * If retrieve role session do retrieve for more than one token.
 * On exit, we need to detach all message queue.
 **/
static void retrieve_exit_callback(int code, Datum arg) {
	HASH_SEQ_STATUS status;
	MsgQueueStatusEntry *entry;

	if (code != 0) {
        StatusInAbort = true;
        // TODO: The cancel here  should consider more than one sender.
        retrieve_cancel_action(EndpointCtl.Gp_token);
        DetachEndpoint(true);
        StatusInAbort = false;
	}
    ClearGpToken();
    ClearEndpointRole();

	/* Nothing to do if hashtable not set up */
	if (MsgQueueHTB == NULL)
		return;
	/* Detach all msg queue dsm*/
	hash_seq_init(&status, MsgQueueHTB);
	while ((entry = (MsgQueueStatusEntry *) hash_seq_search(&status)) != NULL) {
		dsm_detach(entry->mq_seg);
	}
	hash_destroy(MsgQueueHTB);
	MsgQueueHTB = NULL;
}

/*
 * If no PANIC exception happens, DetachEndpoint and retrieve_cancel_action will only
 * be called once in current function.
 *
 * Buf if the PANIC happens, proc exit, these two methods will be called twice. Since we
 * call these two methods during dsm detach.
 * */
static void retrieve_xact_abort_callback(XactEvent ev, void* vp) {
    elog(LOG, "retrieve_xact_abort_callback ---------------");
    if (ev == XACT_EVENT_ABORT) {
        StatusInAbort = true;
        if (EndpointCtl.Gp_endpoint_role == EPR_RECEIVER &&
            my_shared_endpoint != NULL &&
            EndpointCtl.Gp_token != InvalidToken) {
            retrieve_cancel_action(EndpointCtl.Gp_token);
            DetachEndpoint(true);
        }
        StatusInAbort = false;
        ClearGpToken();
        ClearEndpointRole();
    }
}

static void retrieve_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                      SubTransactionId parentSubid, void *arg) {
    if (event == SUBXACT_EVENT_ABORT_SUB) {
        retrieve_xact_abort_callback(XACT_EVENT_ABORT, NULL);
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

/*
 * Set the variable Gp_token
 */
void
SetGpToken(int64 token)
{
	if (EndpointCtl.Gp_token != InvalidToken)
		ep_log(ERROR, "endpoint token %s is already set", printToken(EndpointCtl.Gp_token));

	EndpointCtl.Gp_token = token;
}

/*
 * Clear the variable Gp_token
 */
void
ClearGpToken(void)
{
	// ep_log(DEBUG3, "endpoint token %s is unset", printToken(Gp_token));
	EndpointCtl.Gp_token = InvalidToken;
}

/*
 * Convert the string tk0123456789 to int 0123456789
 */
int64
parseToken(char *token)
{
	int64		token_id = InvalidToken;
	char* tokenFmtStr = get_token_name_format_str();

	if (token[0] == tokenFmtStr[0] && token[1] == tokenFmtStr[1])
	{
		token_id = atoll(token + 2);
	}
	else
	{
		ep_log(ERROR, "invalid token \"%s\"", token);
	}

	return token_id;
}

/*
 * Generate a string tk0123456789 from int 0123456789
 *
 * Note: need to pfree() the result
 */
char *
printToken(int64 token_id)
{
	Insist(token_id != InvalidToken);

	char	   *res = palloc(23);		/* length 13 = 2('tk') + 20(length of max int64 value) + 1('\0') */

	sprintf(res, get_token_name_format_str(), token_id);
	return res;
}

/*
 * Set the role of endpoint, sender or receiver
 */
void
SetEndpointRole(enum EndpointRole role)
{
	if (EndpointCtl.Gp_endpoint_role != EPR_NONE)
		ep_log(ERROR, "endpoint role %s is already set",
			   endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

	ep_log(DEBUG3, "set endpoint role to %s", endpoint_role_to_string(role));

	EndpointCtl.Gp_endpoint_role = role;
}

/*
 * Clear the role of endpoint
 */
void
ClearEndpointRole(void)
{
	ep_log(DEBUG3, "unset endpoint role %s", endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

	EndpointCtl.Gp_endpoint_role = EPR_NONE;
}

/*
 * Return the value of static variable Gp_endpoint_role
 */
enum EndpointRole
EndpointRole(void)
{
	return EndpointCtl.Gp_endpoint_role;
}

List *
GetContentIDsByToken(int64 token)
{
	List	   *l = NIL;
	Assert(SharedTokens);
	LWLockAcquire(TokensDSMLWLock, LW_SHARED);
	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedTokens[i].token == token)
		{
			if (SharedTokens[i].all_seg)
			{
				l = NIL;
				break;
			}
			else
			{
				int16		x = -1;

				while ((x = get_next_dbid_from_bitmap(SharedTokens[i].dbIds, x)) >= 0)
				{
					l = lappend_int(l, dbid_to_contentid(x));
				}
				Assert(l->length == SharedTokens[i].endpoint_cnt);
				break;
			}
		}
	}
	LWLockRelease(TokensDSMLWLock);
	return l;
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
static int
get_next_dbid_from_bitmap(int32 *bitmap, int prevbit)
{
    int			wordnum;
    uint32		mask;

    if (bitmap == NULL)
        elog(ERROR, "invalid dbid bitmap");

    prevbit++;
    mask = (~(uint32) 0) << BITNUM(prevbit);
    for (wordnum = WORDNUM(prevbit); wordnum < MAX_NWORDS; wordnum++)
    {
        uint32		w = bitmap[wordnum];

        /* ignore bits before prevbit */
        w &= mask;

        if (w != 0)
        {
            int			result;

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

/*
 * If the dbid is in this bitmap.
 */
static bool
dbid_in_bitmap(int32 *bitmap, int16 dbid)
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
static void
add_dbid_into_bitmap(int32 *bitmap, int16 dbid)
{
    if (dbid < 0 || dbid >= sizeof(int32) * 8 * MAX_NWORDS)
        elog(ERROR, "invalid dbid");
    if (bitmap == NULL)
        elog(ERROR, "invalid dbid bitmap");

    bitmap[WORDNUM(dbid)] |= ((uint32) 1 << BITNUM(dbid));
}

/*
 * End-points with same token can exist in some or all segments.
 * This function is to determine if the end-point exists in the segment(dbid).
 */
static bool
dbid_has_token(SharedToken token, int16 dbid)
{
    if (token->all_seg)
        return true;

    return dbid_in_bitmap(token->dbIds, dbid);
}

/*
 * Obtain the content-id of a segment by given dbid
 */
static int16
dbid_to_contentid(int16 dbid)
{
    int16		contentid = 0;
    Relation	rel;
    ScanKeyData scankey[1];
    SysScanDesc scan;
    HeapTuple	tup;

    /* Can only run on a master node. */
    if (!IS_QUERY_DISPATCHER())
        elog(ERROR, "dbid_to_contentid() should only execute on execution segments");

    rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

    /*
     * SELECT * FROM gp_segment_configuration WHERE dbid = :1
     */
    ScanKeyInit(&scankey[0],
                Anum_gp_segment_configuration_dbid,
                BTEqualStrategyNumber, F_INT2EQ,
                Int16GetDatum(dbid));

    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, 1, scankey);


    tup = systable_getnext(scan);
    if (HeapTupleIsValid(tup))
    {
        contentid = ((Form_gp_segment_configuration) GETSTRUCT(tup))->content;
        /* We expect a single result, assert this */
        Assert(systable_getnext(scan) == NULL); /* should be only 1 */
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return contentid;
}

static const char *
endpoint_role_to_string(enum EndpointRole role)
{
    switch (role)
    {
        case EPR_SENDER:
            return "[END POINT SENDER]";

        case EPR_RECEIVER:
            return "[END POINT RECEIVER]";

        case EPR_NONE:
            return "[END POINT NONE]";

        default:
            ep_log(ERROR, "unknown end point role %d", role);
            return NULL;
    }
}

static void
check_token_valid(void)
{
    if (Gp_role == GP_ROLE_EXECUTE && EndpointCtl.Gp_token == InvalidToken)
        ep_log(ERROR, "invalid endpoint token");
}

char *
get_token_name_format_str(void)
{
    static char tokenNameFmtStr[64]= "";
    if (strlen(tokenNameFmtStr)==0)
    {
        char *p = INT64_FORMAT;
        snprintf(tokenNameFmtStr, sizeof(tokenNameFmtStr), "tk%%020%s", p+1);
    }
    return tokenNameFmtStr;
}

static void
check_end_point_allocated(void)
{
    if (EndpointCtl.Gp_endpoint_role != EPR_SENDER)
        ep_log(ERROR, "%s could not check endpoint allocated status",
               endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

    if (!my_shared_endpoint)
        ep_log(ERROR, "endpoint for token %s is not allocated", printToken(EndpointCtl.Gp_token));

    check_token_valid();

    LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);
    if (my_shared_endpoint->token != EndpointCtl.Gp_token)
    {
        LWLockRelease(EndpointsDSMLWLock);
        ep_log(ERROR, "endpoint for token %s is not allocated", printToken(EndpointCtl.Gp_token));
    }
    LWLockRelease(EndpointsDSMLWLock);
}

static EndpointStatus *
find_endpoint_status(EndpointStatus * status_array, int number,
                     int64 token, int dbid)
{
    for (int i = 0; i < number; i++)
    {
        if (status_array[i].token == token
            && status_array[i].dbid == dbid)
        {
            return &status_array[i];
        }
    }
    return NULL;
}

static void
set_attach_status(AttachStatus status)
{
    if (EndpointCtl.Gp_endpoint_role != EPR_SENDER)
        ep_log(ERROR, "%s could not set endpoint", endpoint_role_to_string(EndpointCtl.Gp_endpoint_role));

    if (!my_shared_endpoint && !my_shared_endpoint->empty)
        ep_log(ERROR, "endpoint doesn't exist");

    LWLockAcquire(EndpointsDSMLWLock, LW_EXCLUSIVE);

    my_shared_endpoint->attach_status = status;

    LWLockRelease(EndpointsDSMLWLock);

    if (status == Status_Finished)
        my_shared_endpoint = NULL;
}

/*
 * Return true if this end-point exists on QD.
 */
static bool
endpoint_on_qd(SharedToken token)
{
    return (token->endpoint_cnt == 1) && (dbid_has_token(token, MASTER_DBID));
}

static volatile EndpointDesc *
find_endpoint_by_token(int64 token)
{
    EndpointDesc *res = NULL;

    LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);
    if (SharedEndpoints == NULL) {
        LWLockRelease(EndpointsDSMLWLock);
        return res;
    }

    for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
    {
        if (!SharedEndpoints[i].empty &&
            SharedEndpoints[i].token == token)
        {

            res = &SharedEndpoints[i];
            break;
        }
    }
    LWLockRelease(EndpointsDSMLWLock);
    return res;
}

static AttachStatus status_string_to_enum(char* status)
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
    else {
        ep_log(ERROR, "unknown end point status %s", status);
        return Status_NotAttached;
    }
}

/*
 * On QD, display all the endpoints information in shared memory
 */
Datum
gp_endpoints_info(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "gp_endpoints_info() only can be called on query dispatcher");
	// Attach to the endpoints and tokens dsm if in other sessions.
	AttachOrCreateEndpointDsm(true);
	AttachOrCreateTokenDsm(true);

	bool is_all = PG_GETARG_BOOL(0);
	FuncCallContext *funcctx;
	EndpointsInfo *mystatus;
	MemoryContext oldcontext;
	Datum		values[GP_ENDPOINTS_INFO_ATTRNUM];
	bool		nulls[GP_ENDPOINTS_INFO_ATTRNUM] = {true};
	HeapTuple	tuple;
	int			res_number = 0;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */


		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc	tupdesc = CreateTemplateTupleDesc(GP_ENDPOINTS_INFO_ATTRNUM, false);

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
		mystatus->seg_db_list = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID)->cdbs->segment_db_info;
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
			int			idx = 0;

			for (int i = 0; i < cdb_pgresults.numResults; i++)
			{
				struct pg_result *result = cdb_pgresults.pg_results[i];

				for (int j = 0; j < PQntuples(result); j++)
				{
					mystatus->status[idx].token = parseToken(PQgetvalue(result, j, 0));
					mystatus->status[idx].dbid = atoi(PQgetvalue(result, j, 1));
					mystatus->status[idx].attach_status = status_string_to_enum(PQgetvalue(result, j, 2));
					mystatus->status[idx].sender_pid = atoi(PQgetvalue(result, j, 3));
					idx++;
				}
			}
		}

		/* get end-point status on master */
		LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);
		int			cnt = 0;

		for (int i = 0; i < MAX_ENDPOINT_SIZE && SharedEndpoints != NULL; i++)
		{
			Endpoint	entry = &SharedEndpoints[i];

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
			int			idx = 0;

			for (int i = 0; i < MAX_ENDPOINT_SIZE && SharedEndpoints != NULL; i++)
			{
				Endpoint	entry = &SharedEndpoints[i];

				if (!entry->empty)
				{
					mystatus->status[mystatus->status_num - cnt + idx].token = entry->token;
					mystatus->status[mystatus->status_num - cnt + idx].dbid = MASTER_DBID;
					mystatus->status[mystatus->status_num - cnt + idx].attach_status = entry->attach_status;
					mystatus->status[mystatus->status_num - cnt + idx].sender_pid = entry->sender_pid;
					idx++;
				}
			}
		}
		LWLockRelease(EndpointsDSMLWLock);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;
	/*
	 * build detailed token information
	 */
	LWLockAcquire(TokensDSMLWLock, LW_SHARED);
	while (mystatus->curTokenIdx < MAX_ENDPOINT_SIZE && SharedTokens != NULL)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum		result;
		CdbComponentDatabaseInfo *dbinfo;

		SharedToken entry = &SharedTokens[mystatus->curTokenIdx];

		if (entry->token != InvalidToken
			&& (superuser() || entry->user_id == GetUserId()))
		{
			if (endpoint_on_qd(entry))
			{
				if (gp_session_id == entry->session_id || is_all)
				{
					/* one end-point on master */
					dbinfo = dbid_get_dbinfo(MASTER_DBID);

					char	   *token = printToken(entry->token);

					values[0] = CStringGetTextDatum(token);
					nulls[0]  = false;
					values[1] = CStringGetTextDatum(entry->cursor_name);
					nulls[1]  = false;
					values[2] = Int32GetDatum(entry->session_id);
					nulls[2]  = false;
					values[3] = CStringGetTextDatum(dbinfo->hostname);
					nulls[3]  = false;
					values[4] = Int32GetDatum(dbinfo->port);
					nulls[4]  = false;
					values[5] = Int32GetDatum(MASTER_DBID);
					nulls[5]  = false;
					values[6] = ObjectIdGetDatum(entry->user_id);
					nulls[6]  = false;

					/*
					 * find out the status of end-point
					 */
					EndpointStatus *ep_status = find_endpoint_status(mystatus->status, mystatus->status_num,
													  entry->token, MASTER_DBID);

					if (ep_status != NULL)
					{
						char	   *status = NULL;

						switch (ep_status->attach_status)
						{
							case Status_NotAttached:
								status = GP_ENDPOINT_STATUS_INIT;
								break;
							case Status_Prepared:
								status = GP_ENDPOINT_STATUS_READY;
								break;
							case Status_Attached:
								status = GP_ENDPOINT_STATUS_RETRIEVING;
								break;
							case Status_Finished:
								status = GP_ENDPOINT_STATUS_FINISH;
								break;
						}
						values[7] = CStringGetTextDatum(status);
						nulls[7] = false;
					}
					else
					{
						values[7] = CStringGetTextDatum(GP_ENDPOINT_STATUS_RELEASED);
						nulls[7] = false;
					}

					mystatus->curTokenIdx++;
					tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
					result = HeapTupleGetDatum(tuple);
					LWLockRelease(TokensDSMLWLock);
					SRF_RETURN_NEXT(funcctx, result);
					pfree(token);
				}
				else
				{
					mystatus->curTokenIdx++;
				}
			}
			else
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
				}
				else if (mystatus->seg_db_list[mystatus->curSegIdx].role == 'p'
						 && mystatus->curSegIdx < mystatus->segment_num)
				{
					if (gp_session_id == entry->session_id || is_all)
					{
						/* get a primary segment and return this token and segment */
						char	   *token = printToken(entry->token);

						values[0] = CStringGetTextDatum(token);
						nulls[0]  = false;
						values[1] = CStringGetTextDatum(entry->cursor_name);
						nulls[1]  = false;
						values[2] = Int32GetDatum(entry->session_id);
						nulls[2]  = false;
						values[3] = CStringGetTextDatum(mystatus->seg_db_list[mystatus->curSegIdx].hostname);
						nulls[3]  = false;
						values[4] = Int32GetDatum(mystatus->seg_db_list[mystatus->curSegIdx].port);
						nulls[4]  = false;
						values[5] = Int32GetDatum(mystatus->seg_db_list[mystatus->curSegIdx].dbid);
						nulls[5]  = false;
						values[6] = ObjectIdGetDatum(entry->user_id);
						nulls[6]  = false;

						/*
						 * find out the status of end-point
						 */
						EndpointStatus *qe_status = find_endpoint_status(mystatus->status,
															mystatus->status_num,
																	entry->token,
								mystatus->seg_db_list[mystatus->curSegIdx].dbid);

						if (qe_status != NULL)
						{
							char	   *status = NULL;

							switch (qe_status->attach_status)
							{
								case Status_NotAttached:
									status = GP_ENDPOINT_STATUS_INIT;
									break;
								case Status_Prepared:
									status = GP_ENDPOINT_STATUS_READY;
									break;
								case Status_Attached:
									status = GP_ENDPOINT_STATUS_RETRIEVING;
									break;
								case Status_Finished:
									status = GP_ENDPOINT_STATUS_FINISH;
									break;
							}
							values[7] = CStringGetTextDatum(status);
							nulls[7] = false;
						}
						else
						{
							values[7] = CStringGetTextDatum(GP_ENDPOINT_STATUS_RELEASED);
							nulls[7] = false;
						}

						mystatus->curSegIdx++;
						if (mystatus->curSegIdx == mystatus->segment_num)
						{
							mystatus->curTokenIdx++;
							mystatus->curSegIdx = 0;
						}

						tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
						result = HeapTupleGetDatum(tuple);
						LWLockRelease(TokensDSMLWLock);
						SRF_RETURN_NEXT(funcctx, result);
						pfree(token);
					}
					else
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
		}
		else
		{
			mystatus->curTokenIdx++;
		}
	}
	LWLockRelease(TokensDSMLWLock);
	SRF_RETURN_DONE(funcctx);
}

/*
 * Display the status of all valid EndpointDesc of current
 * backend in shared memory
 */
Datum
gp_endpoints_status_info(PG_FUNCTION_ARGS)
{
	// Attach to the token info dsm if in other sessions.
	AttachOrCreateEndpointDsm(true);

	FuncCallContext *funcctx;
	EndpointsStatusInfo *mystatus;
	MemoryContext oldcontext;
	Datum		values[8];
	bool		nulls[8] = {true};
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc	tupdesc = CreateTemplateTupleDesc(8, false);

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
	mystatus = funcctx->user_fctx;

	LWLockAcquire(EndpointsDSMLWLock, LW_SHARED);
	while (mystatus->current_idx < mystatus->endpoints_num && SharedEndpoints != NULL)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum		result;

		Endpoint	entry = &SharedEndpoints[mystatus->current_idx];

		if (!entry->empty && (superuser() || entry->user_id == GetUserId()))
		{
			char	   *status = NULL;
			char	   *token = printToken(entry->token);

			values[0] = CStringGetTextDatum(token);
			nulls[0] = false;
			values[1] = Int32GetDatum(entry->database_id);
			nulls[1] = false;
			values[2] = Int32GetDatum(entry->sender_pid);
			nulls[2] = false;
			values[3] = Int32GetDatum(entry->receiver_pid);
			nulls[3] = false;
			switch (entry->attach_status)
			{
				case Status_NotAttached:
					status = GP_ENDPOINT_STATUS_INIT;
					break;
				case Status_Prepared:
					status = GP_ENDPOINT_STATUS_READY;
					break;
				case Status_Attached:
					status = GP_ENDPOINT_STATUS_RETRIEVING;
					break;
				case Status_Finished:
					status = GP_ENDPOINT_STATUS_FINISH;
					break;
			}
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
			LWLockRelease(EndpointsDSMLWLock);
			SRF_RETURN_NEXT(funcctx, result);
			pfree(token);
		}
		mystatus->current_idx++;
	}
	LWLockRelease(EndpointsDSMLWLock);
	SRF_RETURN_DONE(funcctx);
}
