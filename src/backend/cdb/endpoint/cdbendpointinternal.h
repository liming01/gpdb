/*-------------------------------------------------------------------------
 *
 * cdbendpointinternal.h
 *	  Internal routines for parallel retrieve cursor.
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 * src/backend/cdb/endpoints/cdbendpointinternal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBENDPOINTINTERNAL_H
#define CDBENDPOINTINTERNAL_H

#define MAX_ENDPOINT_SIZE                1024
#define ENDPOINT_TOKEN_LEN               16
#define ENDPOINT_TOKEN_STR_LEN           (2 + ENDPOINT_TOKEN_LEN * 2) // "tk0A1B...4E5F"
#define InvalidSession                   (-1)

#define GP_ENDPOINT_STATUS_INIT          "INIT"
#define GP_ENDPOINT_STATUS_READY         "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING    "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH        "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED      "RELEASED"

#define ENDPOINT_KEY_TUPLE_DESC_LEN     1
#define ENDPOINT_KEY_TUPLE_DESC         2
#define ENDPOINT_KEY_TUPLE_QUEUE        3

/*
 * Naming rules for endpoint:
 * cursorname_sessionIdHex_segIndexHex
 */
#define ENDPOINT_NAME_LEN (NAMEDATALEN + 1 + 8 + 1 + 8)

/*
 * Endpoint attach status.
 */
enum AttachStatus
{
	Status_NotAttached = 0,
	Status_Prepared,
	Status_Attached,
	Status_Finished
} AttachStatus;

/*
 * Retrieve role status.
 */
enum RetrieveStatus
{
	RETRIEVE_STATUS_INVALID,
	RETRIEVE_STATUS_INIT,
	RETRIEVE_STATUS_GET_TUPLEDSCR,
	RETRIEVE_STATUS_GET_DATA,
	RETRIEVE_STATUS_FINISH,
};

/*
 * Endpoint Description, entries are maintained in shared memory.
 */
typedef struct EndpointDesc
{
	char name[ENDPOINT_NAME_LEN];      /* Endpoint name */
	char cursor_name[NAMEDATALEN];     /* Parallel cursor name */
	Oid database_id;                   /* Database OID */
	pid_t sender_pid;                  /* The PID of EPR_SENDER(endpoint), set before endpoint sends data */
	pid_t receiver_pid;                /* The retrieve role's PID that connect to current endpoint */
	dsm_handle handle;                 /* DSM handle, which contains shared message queue */
	Latch ack_done;                    /* Latch to sync EPR_SENDER and EPR_RECEIVER status */
	Latch check_wait_latch;            /* Latch to sync CHECK WAIT udf in write gang and ENDPOINT QE */
	enum AttachStatus attach_status;   /* The attach status of the endpoint */
	int session_id;                    /* Connection session id */
	Oid user_id;                       /* User ID of the current executed PARALLEL RETRIEVE CURSOR */
	bool empty;                        /* Whether current EndpointDesc slot in DSM is free */
} EndpointDesc;


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

/*
 * Local structure to record current PARALLEL RETRIEVE CURSOR token and other info.
 */
typedef struct EndpointControl
{
	enum ParallelRetrCursorExecRole Gp_prce_role;   /* Current PARALLEL RETRIEVE CURSOR role */
	char cursor_name[NAMEDATALEN];
	int  session_id;
} EndpointControl;

typedef EndpointDesc *Endpoint;
extern EndpointControl EndpointCtl;            /* Endpoint ctrl */

/* Endpoint shared memory utility functions in "cdbendpoint.c" */
extern EndpointDesc* get_endpointdesc_by_index(int index);
extern EndpointDesc *find_endpoint(const char *endpointName, int sessionID);
extern const int8 *get_token_by_session_id(int sessionId);
extern int get_session_id_by_token(const int8 *token);

/* utility functions in "cdbendpointutilities.c" */
extern const char *endpoint_role_to_string(enum ParallelRetrCursorExecRole role);
extern bool is_endpoint_token_valid(const int8 *token);
extern void invalidate_endpoint_name(char *endpointName /*out*/);
extern bool token_equals(const int8 *token1, const int8 *token2);
extern bool endpoint_name_equals(const char *name1, const char *name2);
extern uint64 create_magic_num_for_endpoint(const EndpointDesc *desc);

#endif   /* CDBENDPOINTINTERNAL_H */
