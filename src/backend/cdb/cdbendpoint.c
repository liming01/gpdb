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

#include "access/tupdesc.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
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

/*
 * Macros
 */

#define BITS_PER_BITMAPWORD 32
#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/*
 * Static variables
 */

static char tokenNameFmtStr[64]= "";

/* Cache tuple descriptors for all tokens which have been retrieved in this
 * retrieve session */
static MessageQueueData MessageQueues[MAX_ENDPOINT_SIZE];
static TupleTableSlot *RetrieveTupleSlots[MAX_ENDPOINT_SIZE] = {};
static int64 RetrieveTokens[MAX_ENDPOINT_SIZE];
static int RetrieveStatus[MAX_ENDPOINT_SIZE];

static SharedTokenDesc *SharedTokens = NULL;
static EndpointDesc *SharedEndpoints = NULL;
static List *TokensInXact = NIL;
static volatile EndpointDesc *my_shared_endpoint = NULL;

static slock_t *shared_tokens_lock; // TODO: Should use LWLock.
static slock_t *shared_end_points_lock;
static slock_t *shared_token_ctx_lock;

static int64 Gp_token = InvalidToken;
static enum EndpointRole Gp_endpoint_role = EPR_NONE;
static bool StatusInAbort = false;
static bool StatusNeedAck = false;
static int64 CurrentRetrieveToken = 0;

static dsm_segment* token_info_dsm_seg = NULL;
static dsm_segment* endpoint_info_dsm_seg = NULL;

/*
 * Static functions
 */

static const char *endpoint_role_to_string(enum EndpointRole role);
static void init_mq_conn_data(void);
static void check_token_valid(void);
static void check_end_point_allocated(void);
static void create_and_connect_mq(void);
static void init_conn_for_sender(void);
static void message_write(char *data, int len);
static void message_read(char *data, int len);
static void sender_finish(void);
static void receiver_finish(void);
static void init_conn_for_receiver(void);
static void sender_close(void);
static void receiver_close(void);
static EndpointStatus *find_endpoint_status(EndpointStatus * status_array, int number, int64 token, int dbid);
static bool dbid_in_bitmap(int32 *bitmap, int16 dbid);
static void add_dbid_into_bitmap(int32 *bitmap, int16 dbid);
static int get_next_dbid_from_bitmap(int32 *bitmap, int prevbit);
static bool dbid_has_token(SharedToken token, int16 dbid);
static int16 dbid_to_contentid(int16 dbid);
static void set_attach_status(AttachStatus status);
static void set_sender_pid(void);
static void init_endpoint_connection(void);
static void finish_endpoint_connection(void);
static void close_endpoint_connection(void);
static void startup_endpoint_mq(DestReceiver *self, int operation __attribute__((unused)), TupleDesc typeinfo);
static void send_slot_to_endpoint_receiver(TupleTableSlot *slot, DestReceiver *self);
static void shutdown_endpoint_mq(DestReceiver *self);
static void destroy_endpoint_mq(DestReceiver *self);
static void retrieve_cancel_action(void);
static bool endpoint_on_qd(SharedToken token);
static void unset_sender_pid(void);
static void unset_endpoint_receiver_pid(volatile EndpointDesc * endPointDesc);
static void unset_endpoint_sender_pid(volatile EndpointDesc * endPointDesc);
static void unset_endpoint(volatile EndpointDesc * endPointDesc);
static volatile EndpointDesc *find_endpoint_by_token(int64 token);
static void send_tuple_desc_to_mq(TupleDesc tupdesc);
static void send_tuple_slot(TupleTableSlot *slot);
static TupleTableSlot *receive_tuple_slot(void);

static dsm_segment * create_token_info_dsm();
static dsm_segment * create_endpoint_info_dsm();

static void detach_token_dsm_seg(void) {
	if (token_info_dsm_seg != NULL) {
		dsm_detach(token_info_dsm_seg);
		token_info_dsm_seg = NULL;
		SharedTokens = NULL;
		elog(LOG, "detach_token_dsm_seg +++++++++++++++++");
	}
}

static void detach_endpoint_dsm_seg(void) {
	if (endpoint_info_dsm_seg != NULL) {
		dsm_detach(endpoint_info_dsm_seg);
		endpoint_info_dsm_seg = NULL;
		SharedEndpoints = NULL;
        elog(LOG, "detach_endpoint_dsm_seg ++++++++++++++++++");
	}
}

static void on_endpoint_dsm_destroy_callback (dsm_segment* seg, Datum arg) {
	SpinLockAcquire(shared_token_ctx_lock);
	tokenDSMCtx->endpoint_info_handle = DSM_HANDLE_INVALID;
	SpinLockRelease(shared_token_ctx_lock);
}

static void on_token_dsm_destroy_callback (dsm_segment* seg, Datum arg) {
	SpinLockAcquire(shared_token_ctx_lock);
	tokenDSMCtx->token_info_handle = DSM_HANDLE_INVALID;
	SpinLockRelease(shared_token_ctx_lock);
}

void AttachOrCreateTokenInfoDSM(bool* is_token_attached, bool* is_endpoint_attached) {
	bool token_attached = false;
	bool endpoint_attached = false;
	bool token_attach_error = false;
	bool endpoint_attach_error = false;
    SpinLockAcquire(shared_token_ctx_lock);
	if (Gp_role == GP_ROLE_DISPATCH) {
        // Init token info dsm only on QD.
        dsm_handle token_info_handle = tokenDSMCtx->token_info_handle;
        if (token_info_dsm_seg == NULL) {
			if (token_info_handle == DSM_HANDLE_INVALID) {
				token_info_dsm_seg = create_token_info_dsm();
				if (token_info_dsm_seg == NULL) {
					token_attach_error = true;
				} else {
					tokenDSMCtx->token_info_handle = dsm_segment_handle(token_info_dsm_seg);
					SharedTokens = (SharedToken) dsm_segment_address(token_info_dsm_seg);
					token_attached = true;
					int			i;
					for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
					{
						SharedTokens[i].token = InvalidToken;
						memset(SharedTokens[i].cursor_name, 0, NAMEDATALEN);
						SharedTokens[i].session_id = InvalidSession;
						SharedTokens[i].user_id = InvalidOid;
					}
				}
			} else {
				// attach
				token_info_dsm_seg = dsm_attach(token_info_handle);
				if (token_info_dsm_seg == NULL) {
					token_attach_error = true;
				} else {
					token_attached = true;
				    dsm_pin_mapping(token_info_dsm_seg);
					SharedTokens = (SharedToken) dsm_segment_address(token_info_dsm_seg);
				}
			}
			elog(LOG, "attach to token_info_dsm_seg AAAAAAAAAAAAA");
        }
	}
    dsm_handle endpoint_info_handle = tokenDSMCtx->endpoint_info_handle;
	if (endpoint_info_dsm_seg == NULL) {
		if (endpoint_info_handle == DSM_HANDLE_INVALID) {
			endpoint_info_dsm_seg = create_endpoint_info_dsm();
			if (endpoint_info_dsm_seg == NULL) {
				endpoint_attach_error = true;
			} else {
				tokenDSMCtx->endpoint_info_handle = dsm_segment_handle(endpoint_info_dsm_seg);
				SharedEndpoints = (Endpoint) dsm_segment_address(endpoint_info_dsm_seg);
				endpoint_attached = true;
				int			i;
				for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
				{
					SharedEndpoints[i].database_id = InvalidOid;
					SharedEndpoints[i].sender_pid = InvalidPid;
					SharedEndpoints[i].receiver_pid = InvalidPid;
					SharedEndpoints[i].token = InvalidToken;
					SharedEndpoints[i].handle = DSM_HANDLE_INVALID;
					SharedEndpoints[i].session_id = InvalidSession;
					SharedEndpoints[i].user_id = InvalidOid;
					SharedEndpoints[i].attach_status = Status_NotAttached;
					SharedEndpoints[i].empty = true;
					InitSharedLatch(&SharedEndpoints[i].ack_done);
				}
			}
		} else {
			endpoint_info_dsm_seg = dsm_attach(endpoint_info_handle);
			if (endpoint_info_dsm_seg == NULL) {
				endpoint_attach_error = true;
			} else {
				endpoint_attached = true;
                dsm_pin_mapping(endpoint_info_dsm_seg);
				SharedEndpoints = (Endpoint) dsm_segment_address(endpoint_info_dsm_seg);
			}
		}
        elog(LOG, "attach to endpoint_info_dsm_seg AAAAAAAAAAAAA");
	}
    SpinLockRelease(shared_token_ctx_lock);
	if (token_attach_error) {
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("could not create / map dynamic shared memory segment")));
	}
	if (endpoint_attach_error) {
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not create / map dynamic shared memory segment")));
	}
	if (token_attached) {
		on_dsm_destroy(token_info_dsm_seg, on_token_dsm_destroy_callback, 0);
	}
	if (endpoint_attached) {
		on_dsm_destroy(endpoint_info_dsm_seg, on_endpoint_dsm_destroy_callback, 0);
	}
	if (is_token_attached != NULL) {
		*is_token_attached = token_attached;
	}
	if (is_endpoint_attached != NULL) {
		*is_endpoint_attached = endpoint_attached;
	}
}

void Token_DSM_CTX_ShmemInit(void) {
	bool is_shmem_ready;

	tokenDSMCtx = (TokenDSMCtx *)
		ShmemInitStruct(SHMEM_TOKENDSMCTX,
						sizeof(TokenDSMCtx),
						&is_shmem_ready);
	Assert(is_shmem_ready || !IsUnderPostmaster);

	if (!is_shmem_ready)
	{
		tokenDSMCtx->token_info_handle = DSM_HANDLE_INVALID;
		tokenDSMCtx->endpoint_info_handle = DSM_HANDLE_INVALID;
	}

	shared_token_ctx_lock = (slock_t *)
		ShmemInitStruct(SHMEM_TOKEN_CTX_SLOCK,
						sizeof(slock_t),
						&is_shmem_ready);

	Assert(is_shmem_ready || !IsUnderPostmaster);

	if (!is_shmem_ready)
		SpinLockInit(shared_token_ctx_lock);

    shared_tokens_lock = (slock_t *)
        ShmemInitStruct(SHMEM_TOKEN_SLOCK,
                        sizeof(slock_t),
                        &is_shmem_ready);

    Assert(is_shmem_ready || !IsUnderPostmaster);

    if (!is_shmem_ready)
        SpinLockInit(shared_tokens_lock);

    shared_end_points_lock = (slock_t *)
        ShmemInitStruct(SHMEM_END_POINT_SLOCK,
                        sizeof(slock_t),
                        &is_shmem_ready);

    Assert(is_shmem_ready || !IsUnderPostmaster);

    if (!is_shmem_ready)
        SpinLockInit(shared_end_points_lock);
}

static struct dsm_segment * create_token_info_dsm() {
	// Calculate size of the dsm
	Size		size;
	dsm_segment *dsm_seg = NULL;


	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(SharedTokenDesc));
	dsm_seg = dsm_create(size);
	if (dsm_seg != NULL) {
		dsm_pin_mapping(dsm_seg);
	}
	// on_dsm_detach if needed.
	return dsm_seg;
}

static struct dsm_segment * create_endpoint_info_dsm() {
	// Calculate size of the dsm
	Size		size;
    dsm_segment *dsm_seg = NULL;

	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc));
    dsm_seg = dsm_create(size);
	if (dsm_seg != NULL) {
		dsm_pin_mapping(dsm_seg);
	}
    // on_dsm_detach if needed.
    return dsm_seg;
}


/*
 * Extern functions
 */

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
init_mq_conn_data(void)
{
	MessageQueues[CurrentRetrieveToken].mq_seg = NULL;
	MessageQueues[CurrentRetrieveToken].mq_handle = NULL;
	MessageQueues[CurrentRetrieveToken].finished = false;
}

static void
check_token_valid(void)
{
	if (Gp_role == GP_ROLE_EXECUTE && Gp_token == InvalidToken)
		ep_log(ERROR, "invalid endpoint token");
}

static void
check_end_point_allocated(void)
{
	if (Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s could not check endpoint allocated status",
			   endpoint_role_to_string(Gp_endpoint_role));

	if (!my_shared_endpoint)
		ep_log(ERROR, "endpoint for token %s is not allocated", printToken(Gp_token));

	check_token_valid();

	SpinLockAcquire(shared_end_points_lock);

	if (my_shared_endpoint->token != Gp_token)
	{
		SpinLockRelease(shared_end_points_lock);
		ep_log(ERROR, "endpoint for token %s is not allocated", printToken(Gp_token));
	}

	SpinLockRelease(shared_end_points_lock);
}

static void
create_and_connect_mq(void)
{
	check_token_valid();

	if (MessageQueues[CurrentRetrieveToken].mq_handle != NULL)
		return;

	dsm_segment* dsm_seg;
	SpinLockAcquire(shared_end_points_lock);
	dsm_seg = dsm_create(160*1024);
	if (dsm_seg == NULL) {
		SpinLockRelease(shared_end_points_lock);
		close_endpoint_connection();
		ep_log(ERROR, "failed to create shared message queue for send tuples.");
	}
	my_shared_endpoint->handle = dsm_segment_handle(dsm_seg);
	SpinLockRelease(shared_end_points_lock);

	shm_toc    *toc;
	toc = shm_toc_create(my_shared_endpoint->token, dsm_segment_address(dsm_seg), 160*1024);
	shm_mq     *mq;
	mq = shm_mq_create(shm_toc_allocate(toc, (Size) 128*1024), (Size) 128*1024);
	shm_toc_insert(toc, 1, mq);
	shm_mq_set_sender(mq, MyProc);
	MessageQueues[CurrentRetrieveToken].mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
	set_attach_status(Status_Prepared);
	MessageQueues[CurrentRetrieveToken].mq_seg = dsm_seg;
}

static void
init_conn_for_sender(void)
{
	check_end_point_allocated();
	init_mq_conn_data();
	create_and_connect_mq();
}

static void
message_write(char *data, int len) {
	shm_mq_result res;

	/* Notice any interrupts that have occurred. */
	CHECK_FOR_INTERRUPTS();

	res = shm_mq_send(MessageQueues[CurrentRetrieveToken].mq_handle, len, data, false);
	if (res != SHM_MQ_SUCCESS) {
		close_endpoint_connection();
		ep_log(ERROR, "fail to send data to shared message queue, length:%d", len);
	}
}

static void
message_read(char *data, int len)
{
	shm_mq_result res;
	void *temp_data = NULL;

	CHECK_FOR_INTERRUPTS();

	Size actual_size;
	res = shm_mq_receive(MessageQueues[CurrentRetrieveToken].mq_handle, &actual_size, &temp_data, false);

	if (res != SHM_MQ_SUCCESS || actual_size != len) {
		close_endpoint_connection();
		ep_log(ERROR, "fail to receive data from shared message queue, length:%d", len);
	}

	memcpy(data, temp_data, actual_size);
}

static void
sender_finish(void)
{
	char		cmd = 'F';

	message_write(&cmd, 1);

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

		wr = WaitLatch(&my_shared_endpoint->ack_done,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   POLL_FIFO_TIMEOUT);

		if (wr & WL_TIMEOUT)
			continue;

		if (wr & WL_POSTMASTER_DEATH)
		{
			close_endpoint_connection();
			proc_exit(0);
		}

		Assert(wr & WL_LATCH_SET);
		break;
	}
}

static void
receiver_finish(void)
{
	/* for now, receiver does nothing after finished */
}

static void
init_conn_for_receiver(void)
{
	check_token_valid();
	init_mq_conn_data();
	if (MessageQueues[CurrentRetrieveToken].mq_handle)
		return;

	dsm_segment* dsm_seg;
	SpinLockAcquire(shared_end_points_lock);
	dsm_seg = dsm_attach(my_shared_endpoint->handle);
	SpinLockRelease(shared_end_points_lock);
	if (dsm_seg == NULL) {
		close_endpoint_connection();
		ep_log(ERROR, "attach to shared message queue failed.");
	}
	shm_toc * toc = shm_toc_attach(Gp_token, dsm_segment_address(dsm_seg));
	shm_mq     *mq = shm_toc_lookup(toc, 1);
	shm_mq_set_receiver(mq, MyProc);
	MessageQueues[CurrentRetrieveToken].mq_handle = shm_mq_attach(mq, dsm_seg, NULL);
	MessageQueues[CurrentRetrieveToken].mq_seg = dsm_seg;
}

static void
sender_close(void)
{
	if (MessageQueues[CurrentRetrieveToken].mq_seg != NULL) {
		dsm_detach(MessageQueues[CurrentRetrieveToken].mq_seg);
		MessageQueues[CurrentRetrieveToken].mq_seg = NULL;
		MessageQueues[CurrentRetrieveToken].mq_handle = NULL;
	}
}

static void
receiver_close(void)
{
	if (MessageQueues[CurrentRetrieveToken].mq_seg != NULL) {
		dsm_detach(MessageQueues[CurrentRetrieveToken].mq_seg);
		MessageQueues[CurrentRetrieveToken].mq_seg = NULL;
		MessageQueues[CurrentRetrieveToken].mq_handle = NULL;
	}
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

static void
set_attach_status(AttachStatus status)
{
	if (Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s could not set endpoint", endpoint_role_to_string(Gp_endpoint_role));

	if (!my_shared_endpoint && !my_shared_endpoint->empty)
		ep_log(ERROR, "endpoint doesn't exist");

	SpinLockAcquire(shared_end_points_lock);

	my_shared_endpoint->attach_status = status;

	SpinLockRelease(shared_end_points_lock);

	if (status == Status_Finished)
		my_shared_endpoint = NULL;
}

static void
set_sender_pid(void)
{
	int			i;
	int			found_idx = -1;

	Assert(SharedEndpoints);

	if (Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s could not allocate endpoint slot",
			   endpoint_role_to_string(Gp_endpoint_role));

	if (my_shared_endpoint && my_shared_endpoint->token != InvalidToken)
		ep_log(ERROR, "endpoint is already allocated");

	check_token_valid();

	SpinLockAcquire(shared_end_points_lock);

	/*
	 * Presume that for any token, only one parallel cursor is activated at
	 * that time.
	 */
	/* find the slot with the same token */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].token == Gp_token)
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
		SharedEndpoints[i].token = Gp_token;
		SharedEndpoints[i].session_id = gp_session_id;
		SharedEndpoints[i].user_id = GetUserId();
		SharedEndpoints[i].attach_status = Status_NotAttached;
		SharedEndpoints[i].empty = false;
		OwnLatch(&SharedEndpoints[i].ack_done);
	}

	my_shared_endpoint = &SharedEndpoints[i];

	SpinLockRelease(shared_end_points_lock);

	if (!my_shared_endpoint)
		ep_log(ERROR, "failed to allocate endpoint");
}

static void
init_endpoint_connection(void)
{
	switch (Gp_endpoint_role)
	{
		case EPR_SENDER:
			init_conn_for_sender();
			break;
		case EPR_RECEIVER:
			init_conn_for_receiver();
			break;
		default:
			ep_log(ERROR, "invalid endpoint role");
	}
}

static void
finish_endpoint_connection(void)
{
	switch (Gp_endpoint_role)
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
	MessageQueues[CurrentRetrieveToken].finished = true;
}

static void
close_endpoint_connection(void)
{

	if (!MessageQueues[CurrentRetrieveToken].finished)
		ep_log(ERROR, "data are finished reading");

	check_token_valid();

	switch (Gp_endpoint_role)
	{
		case EPR_SENDER:
			sender_close();
			break;
		case EPR_RECEIVER:
			receiver_close();
			break;
		default:
			ep_log(ERROR, "invalid endpoint role");
	}
	MessageQueues[CurrentRetrieveToken].finished = false;
}

static void
startup_endpoint_mq(DestReceiver *self, int operation __attribute__((unused)), TupleDesc typeinfo)
{
	set_sender_pid();
	init_endpoint_connection();
	send_tuple_desc_to_mq(typeinfo);
}

static void
send_slot_to_endpoint_receiver(TupleTableSlot *slot, DestReceiver *self)
{
	send_tuple_slot(slot);
}

static void
shutdown_endpoint_mq(DestReceiver *self)
{
	finish_endpoint_connection();
	close_endpoint_connection();
	set_attach_status(Status_Finished);
}

static void
destroy_endpoint_mq(DestReceiver *self)
{
	pfree(self);
}

static void
retrieve_cancel_action(void)
{
	Assert(SharedEndpoints);
	if (Gp_endpoint_role != EPR_RECEIVER)
		ep_log(ERROR, "receiver cancel action is triggered by accident");

	SpinLockAcquire(shared_end_points_lock);

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].token == Gp_token && SharedEndpoints[i].receiver_pid == MyProcPid)
		{
			SharedEndpoints[i].receiver_pid = InvalidPid;
			SharedEndpoints[i].attach_status = Status_NotAttached;
			pg_signal_backend(SharedEndpoints[i].sender_pid, SIGINT, NULL);
			break;
		}
	}

	SpinLockRelease(shared_end_points_lock);
}

/*
 * Return true if this end-point exists on QD.
 */
static bool
endpoint_on_qd(SharedToken token)
{
	return (token->endpoint_cnt == 1) && (dbid_has_token(token, MASTER_DBID));
}

static void
unset_sender_pid(void)
{
	if (Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s can not free endpoint", endpoint_role_to_string(Gp_endpoint_role));

	if (!my_shared_endpoint && !my_shared_endpoint->empty)
		ep_log(ERROR, "endpoint doesn't exist");

	check_token_valid();

	unset_endpoint_sender_pid(my_shared_endpoint);

	my_shared_endpoint = NULL;
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

		SpinLockAcquire(shared_end_points_lock);

		receiver_pid = endPointDesc->receiver_pid;
		is_attached = endPointDesc->attach_status == Status_Attached;

		if (receiver_pid == MyProcPid)
		{
			endPointDesc->receiver_pid = InvalidPid;
			endPointDesc->attach_status = Status_NotAttached;
		}

		SpinLockRelease(shared_end_points_lock);
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

		SpinLockAcquire(shared_end_points_lock);

		pid = endPointDesc->sender_pid;

		/*
		 * Only reset by this process itself, other process just send signal
		 * to sendpid
		 */
		if (pid == MyProcPid)
		{
			endPointDesc->sender_pid = InvalidPid;
			DisownLatch(&endPointDesc->ack_done);
		}

		SpinLockRelease(shared_end_points_lock);
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

static void
unset_endpoint(volatile EndpointDesc * endPointDesc)
{
	if (!endPointDesc && !endPointDesc->empty)
		ep_log(ERROR, "not an valid endpoint");

	unset_endpoint_sender_pid(endPointDesc);

	SpinLockAcquire(shared_end_points_lock);

	endPointDesc->database_id = InvalidOid;
	endPointDesc->token = InvalidToken;
	endPointDesc->handle = DSM_HANDLE_INVALID;
	endPointDesc->session_id = InvalidSession;
	endPointDesc->user_id = InvalidOid;
	endPointDesc->empty = true;

	SpinLockRelease(shared_end_points_lock);
}

static volatile EndpointDesc *
find_endpoint_by_token(int64 token)
{
	EndpointDesc *res = NULL;

	SpinLockAcquire(shared_end_points_lock);
	if (SharedEndpoints == NULL) {
		SpinLockRelease(shared_end_points_lock);
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
	SpinLockRelease(shared_end_points_lock);
	return res;
}

/*
 * Send the tuple description for retrieve statement to FIFO
 */
static void
send_tuple_desc_to_mq(TupleDesc tupdesc)
{
	Assert(MessageQueues[CurrentRetrieveToken].mq_handle);

	char		cmd = 'D';

	message_write(&cmd, 1);
	TupleDescNode *node = makeNode(TupleDescNode);

	node->natts = tupdesc->natts;
	node->tuple = tupdesc;
	int			tupdesc_len = 0;
	char	   *tupdesc_str = nodeToBinaryStringFast(node, &tupdesc_len);

	message_write((char *) &tupdesc_len, 4);
	message_write(tupdesc_str, tupdesc_len);
}

static void
send_tuple_slot(TupleTableSlot *slot)
{
	char		cmd = 'T';
	int			tupleSize = 0;

	if (Gp_endpoint_role != EPR_SENDER)
		ep_log(ERROR, "%s could not send tuple", endpoint_role_to_string(Gp_endpoint_role));

	MemTuple	mtup = ExecFetchSlotMemTuple(slot);

	tupleSize = memtuple_get_size(mtup);
	message_write(&cmd, 1);
	message_write((char *) &tupleSize, sizeof(int));
	message_write((char *) mtup, tupleSize);
}

static TupleTableSlot *
receive_tuple_slot(void)
{
	char		cmd;
	TupleTableSlot *slot;
	int			tupleSize = 0;
	MemTuple	mtup;

	Assert(MessageQueues[CurrentRetrieveToken].mq_handle);
	Assert(RetrieveTupleSlots[CurrentRetrieveToken]);

	ExecClearTuple(RetrieveTupleSlots[CurrentRetrieveToken]);

	slot = RetrieveTupleSlots[CurrentRetrieveToken];

	while (true)
	{
		message_read(&cmd, 1);

		if (cmd == 'F')
		{
			StatusNeedAck = true;
			return NULL;
		}

		Assert(cmd == 'T');
		message_read((char *) &tupleSize, sizeof(int));
		Assert(tupleSize > 0);

		HOLD_INTERRUPTS();
		SIMPLE_FAULT_INJECTOR(FetchTuplesFromEndpoint);
		RESUME_INTERRUPTS();

		mtup = palloc(tupleSize);
		message_read((char *) mtup, tupleSize);
		slot->PRIVATE_tts_memtuple = mtup;
		ExecStoreVirtualTuple(slot);

		return slot;
	}
}

/*
 * Generate an unique int64 token
 */
int64
GetUniqueGpToken(void)
{
	int64		token;
	Assert(SharedTokens);
	SpinLockAcquire(shared_tokens_lock);

	/* The random number sequences in the same second are the same */

	srand(time(NULL));

REGENERATE:
	token = llabs(((int64)rand() << 32) | rand());

	for (int i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (token == SharedTokens[i].token)
			goto REGENERATE;
	}

	SpinLockRelease(shared_tokens_lock);

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

	SpinLockAcquire(shared_tokens_lock);

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

	SpinLockRelease(shared_tokens_lock);

	/* no empty entry to save this token */
	if (i == MAX_ENDPOINT_SIZE)
	{
		ep_log(ERROR, "can't add a new token %s into shared memory", printToken(token));
	}

}

/*
 * Remove the target token information from token shared memory
 */
void
RemoveParallelCursorToken(int64 token)
{
	Assert(token != InvalidToken);
	bool		endpoint_on_QD = false,
				found = false;
	List	   *seg_list = NIL;
	Assert(SharedTokens);
	SpinLockAcquire(shared_tokens_lock);

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

			elog(LOG, "removed token: " INT64_FORMAT ", session id: %d, cursor name: %s from shared memory",
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

	SpinLockRelease(shared_tokens_lock);

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
	detach_token_dsm_seg();
}
char * 
getTokenNameFormatStr(void)
{
	if (strlen(tokenNameFmtStr)==0)
	{
		char *p = INT64_FORMAT;
		snprintf(tokenNameFmtStr, sizeof(tokenNameFmtStr), "tk%%020%s", p+1);
	}
	return tokenNameFmtStr;
}
/*
 * Convert the string tk0123456789 to int 0123456789
 */
int64
parseToken(char *token)
{
	int64		token_id = InvalidToken;
	char* tokenFmtStr = getTokenNameFormatStr();

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

	sprintf(res, getTokenNameFormatStr(), token_id);
	return res;
}

/*
 * Set the variable Gp_token
 */
void
SetGpToken(int64 token)
{
	if (Gp_token != InvalidToken)
		ep_log(ERROR, "endpoint token %s is already set", printToken(Gp_token));

	Gp_token = token;
}

/*
 * Clear the variable Gp_token
 */
void
ClearGpToken(void)
{
	// ep_log(DEBUG3, "endpoint token %s is unset", printToken(Gp_token));
	Gp_token = InvalidToken;
}

/*
 * Set the role of endpoint, sender or receiver
 */
void
SetEndpointRole(enum EndpointRole role)
{
	if (Gp_endpoint_role != EPR_NONE)
		ep_log(ERROR, "endpoint role %s is already set",
			   endpoint_role_to_string(Gp_endpoint_role));

	ep_log(DEBUG3, "set endpoint role to %s", endpoint_role_to_string(role));

	Gp_endpoint_role = role;
}

/*
 * Clear the role of endpoint
 */
void
ClearEndpointRole(void)
{
	ep_log(DEBUG3, "unset endpoint role %s", endpoint_role_to_string(Gp_endpoint_role));

	Gp_endpoint_role = EPR_NONE;
}

/*
 * Return the value of static variable Gp_token
 */
int64
GpToken(void)
{
	return Gp_token;
}

/*
 * Return the value of static variable Gp_endpoint_role
 */
enum EndpointRole
EndpointRole(void)
{
	return Gp_endpoint_role;
}

Size
Token_ShmemSize(void)
{
	Size		size;

	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(SharedTokenDesc));
	size = add_size(size, sizeof(slock_t));

	return size;
}

Size
Endpoint_ShmemSize(void)
{
	Size		size;

	size = mul_size(MAX_ENDPOINT_SIZE, sizeof(EndpointDesc));
	size = add_size(size, sizeof(slock_t));

	return size;
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
	SpinLockAcquire(shared_end_points_lock);

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
		TokensInXact = lappend(TokensInXact, token_str);
		MemoryContextSwitchTo(oldcontext);
	}

	SpinLockRelease(shared_end_points_lock);

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

	unset_endpoint(endPointDesc);
	detach_endpoint_dsm_seg();
}

/*
 * Return if the user has parallel cursor/endpoint of the token
 *
 * Used by retrieve role authentication
 */
bool
FindEndpointTokenByUser(Oid user_id, const char *token_str)
{
	bool		isFound = false;
    AttachOrCreateTokenInfoDSM(NULL, NULL);
	Assert(SharedEndpoints);
	SpinLockAcquire(shared_end_points_lock);

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

	SpinLockRelease(shared_end_points_lock);
	return isFound;
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

	if (Gp_endpoint_role != EPR_RECEIVER)
		ep_log(ERROR, "%s could not attach endpoint", endpoint_role_to_string(Gp_endpoint_role));

	if (my_shared_endpoint)
		ep_log(ERROR, "endpoint is already attached");

	check_token_valid();

	Assert(SharedEndpoints);
	SpinLockAcquire(shared_end_points_lock);

	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (SharedEndpoints[i].database_id == MyDatabaseId &&
			SharedEndpoints[i].token == Gp_token &&
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

	SpinLockRelease(shared_end_points_lock);

	if (is_invalid_sendpid)
	{
		ep_log(ERROR, "the PARALLEL CURSOR related to endpoint token %s is not EXECUTED",
			   printToken(Gp_token));
	}

	if (already_attached || is_other_pid)
		ep_log(ERROR, "endpoint %s is already attached by receiver(pid: %d)",
			   printToken(Gp_token), attached_pid);

	if (!my_shared_endpoint)
		ep_log(ERROR, "failed to attach non-existing endpoint of token %s", printToken(Gp_token));

	StatusNeedAck = false;

	/*
	 * Search all tokens that retrieved in this session, set
	 * CurrentRetrieveToken to it's array index
	 */
	for (i = 0; i < MAX_ENDPOINT_SIZE; ++i)
	{
		if (RetrieveTokens[i] == Gp_token)
		{
			isFound = true;
			CurrentRetrieveToken = i;
			break;
		}
	}
	if (!isFound)
	{
		CurrentRetrieveToken = 0;
		RetrieveTokens[0] = Gp_token;
	}
	if (!is_self_pid)
	{
		RetrieveStatus[CurrentRetrieveToken] = RETRIEVE_STATUS_INIT;
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
	volatile Latch *ack_done;

	if (Gp_endpoint_role != EPR_RECEIVER ||
		!my_shared_endpoint ||
		Gp_token == InvalidToken)
		return;

	if (Gp_endpoint_role != EPR_RECEIVER)
		ep_log(ERROR, "%s could not attach endpoint", endpoint_role_to_string(Gp_endpoint_role));

	check_token_valid();

	SpinLockAcquire(shared_end_points_lock);

	PG_TRY();
	{
		if (my_shared_endpoint->token != Gp_token)
			ep_log(LOG, "unmatched token, expected %s but it's %s",
				   printToken(Gp_token), printToken(my_shared_endpoint->token));

		if (my_shared_endpoint->receiver_pid != MyProcPid)
			ep_log(ERROR, "unmatched pid, expected %d but it's %d",
				   MyProcPid, my_shared_endpoint->receiver_pid);
	}
	PG_CATCH();
	{
		SpinLockRelease(shared_end_points_lock);
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
	ack_done = &my_shared_endpoint->ack_done;

	SpinLockRelease(shared_end_points_lock);

	my_shared_endpoint = NULL;

	if (StatusNeedAck)
		SetLatch(ack_done);

	StatusNeedAck = false;
}

/*
 * Return the tuple description for retrieve statement
 */
TupleDesc
TupleDescOfRetrieve(void)
{
	char		cmd;
	int			len;
	TupleDescNode *tupdescnode;
	MemoryContext oldcontext;

	if (RetrieveStatus[CurrentRetrieveToken] < RETRIEVE_STATUS_GET_TUPLEDSCR)
	{
		/*
		 * Store the result slot all the retrieve mode QE life cycle, we only
		 * have one chance to built it.
		 */

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		init_endpoint_connection();

		Assert(MessageQueues[CurrentRetrieveToken].mq_handle);
		message_read(&cmd, 1);
		if (cmd == 'D')
		{
			message_read((char *) &len, 4);

			char	   *tupdescnode_str = palloc(len);

			message_read(tupdescnode_str, len);

			tupdescnode = (TupleDescNode *) readNodeFromBinaryString(tupdescnode_str, len);
			if (RetrieveTupleSlots[CurrentRetrieveToken] != NULL)
				ExecClearTuple(RetrieveTupleSlots[CurrentRetrieveToken]);
			RetrieveTupleSlots[CurrentRetrieveToken] = MakeTupleTableSlot();
			ExecSetSlotDescriptor(RetrieveTupleSlots[CurrentRetrieveToken], tupdescnode->tuple);
			RetrieveStatus[CurrentRetrieveToken] = RETRIEVE_STATUS_GET_TUPLEDSCR;
		}
		MemoryContextSwitchTo(oldcontext);
	}

	Assert(RetrieveTupleSlots[CurrentRetrieveToken]);
	Assert(RetrieveTupleSlots[CurrentRetrieveToken]->tts_tupleDescriptor);

	return RetrieveTupleSlots[CurrentRetrieveToken]->tts_tupleDescriptor;
}

/*
 * The error handling of endpoint actions
 */
void
AbortEndpoint(void)
{
	ListCell   *l;

	StatusInAbort = true;

	switch (Gp_endpoint_role)
	{
		case EPR_SENDER:
			if (MessageQueues[CurrentRetrieveToken].mq_seg != NULL)
				sender_close();
			unset_sender_pid();
			break;
		case EPR_RECEIVER:
			if (MessageQueues[CurrentRetrieveToken].mq_seg != NULL)
				receiver_close();
			retrieve_cancel_action();
			DetachEndpoint(true);
			break;
		default:
			break;
	}

	/*
	 * Make sure all token running in this QE is freed, double free is allowed
	 * because free request may issued by the QD already.
	 */

	if (TokensInXact != NIL)
	{
		foreach(l, TokensInXact)
		{
			int64		token = atoll(lfirst(l));

			FreeEndpointOfToken(token);
			pfree(lfirst(l));
		}
		list_free(TokensInXact);
		TokensInXact = NIL;
	}

	StatusInAbort = false;
	Gp_token = InvalidToken;
	Gp_endpoint_role = EPR_NONE;
}

List *
GetContentIDsByToken(int64 token)
{
	List	   *l = NIL;
	Assert(SharedTokens);
	SpinLockAcquire(shared_tokens_lock);
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
	SpinLockRelease(shared_tokens_lock);
	return l;
}

/*
 * Send the results to dest receiver of retrieve statement
 */
void
RetrieveResults(RetrieveStmt * stmt, DestReceiver *dest)
{
	TupleTableSlot *result;
	int64		retrieve_count;

	retrieve_count = stmt->count;

	if (retrieve_count <= 0 && !stmt->is_all)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("RETRIEVE statement only supports forward scan, count should not be: %ld", retrieve_count)));
	}

	if (RetrieveStatus[CurrentRetrieveToken] < RETRIEVE_STATUS_FINISH)
	{
		//init_endpoint_connection();

		while (retrieve_count > 0)
		{
			result = receive_tuple_slot();
			if (!result)
			{
				RetrieveStatus[CurrentRetrieveToken] = RETRIEVE_STATUS_FINISH;
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
					RetrieveStatus[CurrentRetrieveToken] = RETRIEVE_STATUS_FINISH;
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

/*
 * Create the dest receiver of parallel cursor
 */
DestReceiver *
CreateEndpointReceiver(void)
{
	DR_mq_printtup *self = (DR_mq_printtup *) palloc0(sizeof(DR_mq_printtup));

	self->pub.receiveSlot = send_slot_to_endpoint_receiver;
	self->pub.rStartup = startup_endpoint_mq;
	self->pub.rShutdown = shutdown_endpoint_mq;
	self->pub.rDestroy = destroy_endpoint_mq;
	self->pub.mydest = DestEndpoint;

	return (DestReceiver *) self;
}

/*
 * On QD, display all the endpoints information in shared memory
 */
Datum
gp_endpoints_info(PG_FUNCTION_ARGS)
{
	// Attach to the token info dsm if in other sessions.
	bool is_token_attached, is_endpoint_attached;
	AttachOrCreateTokenInfoDSM(&is_token_attached, &is_endpoint_attached);

	FuncCallContext *funcctx;
	EndpointsInfo *mystatus;
	MemoryContext oldcontext;
	Datum		values[GP_ENDPOINTS_INFO_ATTRNUM];
	bool		nulls[GP_ENDPOINTS_INFO_ATTRNUM] = {true};
	HeapTuple	tuple;
	int			res_number = 0;

	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "gp_endpoints_info() only can be called on query dispatcher");

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

		CdbDispatchCommand("SELECT token,dbid,attach_status,senderpid FROM pg_catalog.gp_endpoints_status_info()",
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
					mystatus->status[idx].attach_status = atoi(PQgetvalue(result, j, 2));
					mystatus->status[idx].sender_pid = atoi(PQgetvalue(result, j, 3));
					idx++;
				}
			}
		}

		/* get end-point status on master */
		SpinLockAcquire(shared_end_points_lock);
		int			cnt = 0;

		for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
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

			for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
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
		SpinLockRelease(shared_end_points_lock);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;
	Assert(SharedTokens);
	/*
	 * build detailed token information
	 */
	SpinLockAcquire(shared_tokens_lock);
	while (mystatus->curTokenIdx < MAX_ENDPOINT_SIZE)
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
				/* one end-point on master */
				dbinfo = dbid_get_dbinfo(MASTER_DBID);

				char	   *token = printToken(entry->token);

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

				tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
				result = HeapTupleGetDatum(tuple);
				mystatus->curTokenIdx++;
				SpinLockRelease(shared_tokens_lock);
				SRF_RETURN_NEXT(funcctx, result);
				pfree(token);
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
					/* get a primary segment and return this token and segment */
					char	   *token = printToken(entry->token);

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
					tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
					if (mystatus->curSegIdx == mystatus->segment_num)
					{
						mystatus->curTokenIdx++;
						mystatus->curSegIdx = 0;
					}
					result = HeapTupleGetDatum(tuple);
					SpinLockRelease(shared_tokens_lock);
					SRF_RETURN_NEXT(funcctx, result);
					pfree(token);
				}
			}
		}
		else
		{
			mystatus->curTokenIdx++;
		}
	}
	SpinLockRelease(shared_tokens_lock);
    if (is_endpoint_attached) {
        detach_endpoint_dsm_seg();
    }
	if (is_token_attached) {
		detach_token_dsm_seg();
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
	// Attach to the token info dsm if in other sessions.
	bool is_token_attached, is_endpoint_attached;
	AttachOrCreateTokenInfoDSM(&is_token_attached, &is_endpoint_attached);

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

		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "attach_status",
						   INT4OID, -1, 0);

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

	SpinLockAcquire(shared_end_points_lock);
	while (mystatus->current_idx < mystatus->endpoints_num)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum		result;

		Endpoint	entry = &SharedEndpoints[mystatus->current_idx];

		if (!entry->empty && (superuser() || entry->user_id == GetUserId()))
		{
			char	   *token = printToken(entry->token);

			values[0] = CStringGetTextDatum(token);

			nulls[0] = false;
			values[1] = Int32GetDatum(entry->database_id);
			nulls[1] = false;
			values[2] = Int32GetDatum(entry->sender_pid);
			nulls[2] = false;
			values[3] = Int32GetDatum(entry->receiver_pid);
			nulls[3] = false;
			values[4] = Int32GetDatum(entry->attach_status);
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
			SpinLockRelease(shared_end_points_lock);
			SRF_RETURN_NEXT(funcctx, result);
			pfree(token);
		}
		mystatus->current_idx++;
	}
	SpinLockRelease(shared_end_points_lock);

	if (is_endpoint_attached) {
		detach_endpoint_dsm_seg();
	}
	if (is_token_attached) {
		detach_token_dsm_seg();
	}
	SRF_RETURN_DONE(funcctx);
}

/*
 * Endpoint actions, push, free or unset
 */
void
assign_gp_endpoints_token_operation(const char *newval, void *extra)
{

	const char *token = newval + 1;
	int64		tokenid = atoll(token);

	/*
	 * Maybe called in AtEOXact_GUC() to set to default value (i.e. empty
	 * string)
	 */

	if (newval == NULL || strlen(newval) == 0)
		return;

	if (tokenid != InvalidToken && Gp_role == GP_ROLE_EXECUTE && Gp_is_writer)
	{
        AttachOrCreateTokenInfoDSM(NULL, NULL);
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
