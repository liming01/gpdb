/*-------------------------------------------------------------------------
 * cdbendpoint.h
 *    Functions supporting the Greenplum Endpoint PARALLEL RETRIEVE CURSOR.
 *
 * The PARALLEL RETRIEVE CURSOR is introduced to reduce the heavy burdens of
 * master node. If possible it will not gather the result to master, and
 * redirect the result to segments. However some query may still need to
 * gather to the master. So the ENDPOINT is introduced to present these
 * node entities that when the PARALLEL RETRIEVE CURSOR executed, the query result
 * will be redirected to, not matter they are one master or some segments
 * or all segments.
 *
 * When the PARALLEL RETRIEVE CURSOR executed, user can setup retrieve mode connection
 * (in retrieve mode connection, the libpq authentication will not depends on
 * pg_hba) to all endpoints for retrieving result data parallelly. The RETRIEVE
 * statement behavior is similar to the "FETCH count" statement, while it only
 * can be executed in retrieve mode connection to endpoint.
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbendpoint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBENDPOINT_H
#define CDBENDPOINT_H

#include <inttypes.h>

#include "executor/tqueue.h"
#include "executor/execdesc.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "storage/lwlock.h"

#define MAX_NWORDS                       128
#define MAX_ENDPOINT_SIZE                1024
#define ENDPOINT_TOKEN_LEN               16
#define ENDPOINT_TOKEN_STR_LEN           (2 + ENDPOINT_TOKEN_LEN * 2) //"tk0A1B...4E5F"
#define InvalidSession                  (-1)

#define GP_ENDPOINT_STATUS_INIT          "INIT"
#define GP_ENDPOINT_STATUS_READY         "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING    "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH        "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED      "RELEASED"

/*
 * Roles that used in PARALLEL RETRIEVE CURSOR execution.
 *
 * EPR_SENDER(endpoint) behaviors like a store, the client could retrieve
 * results from it. The EPR_SENDER could be on master or some/all segments,
 * depending on the query of the PARALLEL RETRIEVE CURSOR.
 *
 * EPR_RECEIVER(retrieve role), connect to each EPR_SENDER(endpoint) via "retrieve"
 * mode to retrieve results.
 */
enum ParallelRetrCursorExecRole
{
	PRCER_SENDER = 1,
	PRCER_RECEIVER,
	PRCER_NONE
};

/*
 * Endpoint allocate positions.
 */
enum EndPointExecPosition
{
	ENDPOINT_POS_INVALID,
	ENDPOINT_ON_QD,
	ENDPOINT_ON_SINGLE_QE,
	ENDPOINT_ON_SOME_QE,
	ENDPOINT_ON_ALL_QE
};

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
 * ParallelCursorTokenDesc is a entry to store the information of a PARALLEL RETRIEVE CURSOR token.
 * These entries are maintained in shared memory on QD.
 */
typedef struct ParallelCursorTokenDesc
{
	int8 token[ENDPOINT_TOKEN_LEN];    /* Token */
	char cursor_name[NAMEDATALEN];     /* The PARALLEL RETRIEVE CURSOR's name */
	int session_id;                    /* Which session created this PARALLEL RETRIEVE CURSOR */
	int endpoint_cnt;                  /* How many endpoints are created */
	Oid user_id;                       /* User ID of the current executed PARALLEL RETRIEVE CURSOR */
	enum EndPointExecPosition endPointExecPosition;  /* Position: on QD, On all QE, On Some QEs */
	int32 dbIds[MAX_NWORDS];           /* A bitmap stores the dbids of every endpoint, size is 4906 bits(32X128) */
} ParallelCursorTokenDesc;


/*
 * Naming rules for endpoint:
 * cursorname_sessionIdHex_segIndexHex
 */
#define ENDPOINT_NAME_LEN (NAMEDATALEN + 1 + 8 + 1 + 8)

/*
 * Endpoint Description, entries are maintained in shared memory.
 */
typedef struct EndpointDesc
{
	char name[ENDPOINT_NAME_LEN];      /* Endpoint name */
	Oid database_id;                   /* Database OID */
	pid_t sender_pid;                  /* The PID of EPR_SENDER(endpoint), set before endpoint sends data */
	pid_t receiver_pid;                /* The retrieve role's PID that connect to current endpoint */
	int8 token[ENDPOINT_TOKEN_LEN];    /* The token of the endpoint's running PARALLEL RETRIEVE CURSOR */
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
 * For endpoint(on QD/QE), only keep one entry to track current message queue.
 */
typedef struct MsgQueueStatusEntry
{
	char endpoint_name[ENDPOINT_NAME_LEN];     /* The name of endpoint to be retrieved, also behave as hash key */
	int8 retrieve_token[ENDPOINT_TOKEN_LEN];   /* The PARALLEL RETRIEVE CURSOR token, also as the hash table entry key */
	dsm_segment *mq_seg;                       /* The dsm handle which contains shared memory message queue */
	shm_mq_handle *mq_handle;                  /* Shared memory message queue */
	TupleTableSlot *retrieve_ts;               /* tuple slot used for retrieve data */
	TupleQueueReader *tq_reader;               /* TupleQueueReader to read tuple from message queue */
	enum RetrieveStatus retrieve_status;       /* Track retrieve status for retrieve token entry */
} MsgQueueStatusEntry;

/*
 * Local structure to record current PARALLEL RETRIEVE CURSOR token and other info.
 */
typedef struct EndpointControl
{
	int8 Gp_token[ENDPOINT_TOKEN_LEN];         /* Current PARALLEL RETRIEVE CURSOR token */
	enum ParallelRetrCursorExecRole Gp_prce_role;   /* Current PARALLEL RETRIEVE CURSOR role */
	char cursor_name[NAMEDATALEN];
	int  session_id;
} EndpointControl;

typedef ParallelCursorTokenDesc *ParaCursorToken;
typedef EndpointDesc *Endpoint;

#define ENDPOINT_KEY_TUPLE_DESC_LEN     1
#define ENDPOINT_KEY_TUPLE_DESC         2
#define ENDPOINT_KEY_TUPLE_QUEUE        3

extern ParallelCursorTokenDesc *SharedTokens;  /* Point to ParallelCursorTokenDesc entries in shared memory */
extern EndpointDesc *SharedEndpoints;          /* Point to EndpointDesc entries in shared memory */
extern EndpointControl EndpointCtl;            /* Endpoint ctrl */

/* cbdendpoint.c */
/* Endpoint shared memory context init */
extern Size EndpointShmemSize(void);
extern void EndpointCTXShmemInit(void);

/*
 * Below functions should run on dispatcher.
 */
extern enum EndPointExecPosition GetParallelCursorEndpointPosition(
	const struct Plan *planTree);
extern List *ChooseEndpointContentIDForParallelCursor(
	const struct Plan *planTree, enum EndPointExecPosition *position);
extern void AddParallelCursorToken(int8 *token /*out*/, const char *name, int session_id,
								   Oid user_id, enum EndPointExecPosition endPointExecPosition, List *seg_list);
extern void WaitEndpointReady(const struct Plan *planTree, const char *cursorName);
extern bool CallEndpointUDFOnQD(const struct Plan *planTree, const char *cursorName, const char operator);
/* Called during EXECUTE CURSOR stage on QD. */
extern bool CheckParallelCursorPrivilege(const int8 *token);
/* Remove PARALLEL RETRIEVE CURSOR during cursor portal drop/abort, on QD */
extern void DestroyParallelCursor(const char *cursorName);

extern bool CheckParallelCursorErrors(QueryDesc *queryDesc, bool isWait);

/*
 * Below functions should run on Endpoints(QE/QD).
 */
/* Functions used in CHECK PARALLEL RETRIEVE CURSOR stage, on Endpoints(QE/QD) */
extern DestReceiver *CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc, const char* cursorName);
extern void DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest);
/* Endpoint backend register/free, execute on Endpoints(QE/QD) */
extern void AllocEndpointOfToken(const char *cursorName);

/* UDFs for endpoints operation */
extern Datum gp_operate_endpoints_token(PG_FUNCTION_ARGS);
extern Datum gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
extern Datum gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
/* cdbendpointretrieve.c */
/*
 * Below functions should run on retrieve role backend.
 */
extern bool FindEndpointTokenByUser(Oid user_id, const char *token_str);
extern void AttachEndpoint(const char *endpoint_name);
extern TupleDesc TupleDescOfRetrieve(void);
extern void RetrieveResults(RetrieveStmt *stmt, DestReceiver *dest);
extern void DetachEndpoint(bool reset_pid);


/* cdbendpointutils.c */
/* Utility functions */
extern int64 GpToken(void);
extern void CheckTokenValid(void);
extern bool IsGpTokenValid(void);
extern void SetGpToken(const int8 *token);
extern void ClearGpToken(void);
extern void ParseToken(int8 *token /*out*/, const char *token_str);
extern char *PrintToken(const int8 *token); /* Need to pfree() the result */
extern void SetParallelCursorExecRole(enum ParallelRetrCursorExecRole role);
extern void ClearParallelCursorExecRole(void);
extern enum ParallelRetrCursorExecRole GetParallelCursorExecRole(void);
extern const char *EndpointRoleToString(enum ParallelRetrCursorExecRole role);
extern bool IsEndpointTokenValid(const int8 *token);
extern void InvalidateEndpointToken(int8 *token /*out*/);
extern bool IsEndpointNameValid(const char *endpoint_name);
extern void InvalidateEndpointName(char *endpoint_name /*out*/);

/* Utility functions to handle tokens and endpoints in shared memory */
extern bool endpoint_on_qd(ParaCursorToken para_cursor_token);
extern bool seg_dbid_has_token(ParaCursorToken para_cursor_token, int16 dbid);
extern bool master_dbid_has_token(ParaCursorToken para_cursor_token, int16 dbid);
extern bool dbid_in_bitmap(int32 *bitmap, int16 dbid);
extern void add_dbid_into_bitmap(int32 *bitmap, int16 dbid);
extern int get_next_dbid_from_bitmap(int32 *bitmap, int prevbit);
extern EndpointDesc *find_endpoint(const char *endpoint_name, int session_id);
extern int get_session_id_by_token(const int8 *token);

/* UDFs for endpoints info*/
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);

#endif   /* CDBENDPOINT_H */
