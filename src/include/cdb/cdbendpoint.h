/*-------------------------------------------------------------------------
 * cdbendpoint.h
 *    Functions supporting the Greenplum Endpoint Parallel Cursor.
 *
 * The PARALLEL CURSOR is introduced to reduce the heavy burdens of
 * master node. If possible it will not gather the result to master, and
 * redirect the result to segments. However some query may still need to
 * gather to the master. So the ENDPOINT is introduced to present these
 * node entities that when the parallel cursor executed, the query result
 * will be redirected to, not matter they are one master or some segments
 * or all segments.
 *
 * When the parallel cursor executed, user can setup retrieve mode connection
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
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "storage/lwlock.h"

#define InvalidToken        (-1)
#define InvalidSession      (-1)

#define MAX_NWORDS                       128
#define MAX_ENDPOINT_SIZE                1024

#define GP_ENDPOINT_STATUS_INIT          "INIT"
#define GP_ENDPOINT_STATUS_READY         "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING    "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH        "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED      "RELEASED"

/*
 * Roles that used in parallel cursor execution.
 *
 * EPR_SENDER(endpoint) behaviors like a store, the client could retrieve
 * results from it. The EPR_SENDER could be on master or some/all segments,
 * depending on the query of the parallel cursor.
 *
 * EPR_RECEIVER(retrieve role), connect to each EPR_SENDER(endpoint) via "retrieve"
 * mode to retrieve results.
 */
enum ParallelCursorExecRole
{
	PCER_SENDER = 1,
	PCER_RECEIVER,
	PCER_NONE
};

/*
 * Endpoint allocate positions.
 */
enum EndPointExecPosition
{
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
 * ParallelCursorTokenDesc is a entry to store the information of a parallel cursor token.
 * These entries are maintained in shared memory on QD.
 */
typedef struct ParallelCursorTokenDesc
{
	int64 token;                       /* Token number */
	char cursor_name[NAMEDATALEN];     /* The parallel cursor's name */
	int session_id;                    /* Which session created this parallel cursor */
	int endpoint_cnt;                  /* How many endpoints are created */
	Oid user_id;                       /* User ID of the current executed parallel cursor */
	bool all_seg;                      /* A flag to indicate if the endpoints are on all segments */
	int32 dbIds[MAX_NWORDS];           /* A bitmap stores the dbids of every endpoint, size is 4906 bits(32X128) */
} ParallelCursorTokenDesc;

/*
 * Endpoint Description, entries are maintained in shared memory.
 */
typedef struct EndpointDesc
{
	Oid database_id;                   /* Database OID */
	pid_t sender_pid;                  /* The PID of EPR_SENDER(endpoint), set before endpoint sends data */
	pid_t receiver_pid;                /* The retrieve role's PID that connect to current endpoint */
	int64 token;                       /* The token of the endpoint's running parallel cursor */
	dsm_handle handle;                 /* DSM handle, which contains shared message queue */
	Latch ack_done;                    /* Latch to sync EPR_SENDER and EPR_RECEIVER status */
	enum AttachStatus attach_status;   /* The attach status of the endpoint */
	int session_id;                    /* Connection session id */
	Oid user_id;                       /* User ID of the current executed parallel cursor */
	bool empty;                        /* Whether current EndpointDesc slot in DSM is free */
} EndpointDesc;

/*
 * Shared memory structure LWLocks for ParallelCursorTokenDesc entries
 * and EndpointDesc entries.
 */
typedef struct EndpointSharedCTX
{
	int tranche_id;                    /* Tranche id for parallel cursor endpoint lwlocks.
                                          Read only, don't need acquire lock*/
	LWLockTranche tranche;
	LWLockPadded *endpointLWLocks;     /* LWLocks to protect ParallelCursorTokenDesc entries
                                          and EndpointDesc entries */
} EndpointSharedCTX;

/*
 * For receiver, we have a hash table to store connected endpoint's shared message queue.
 * So that we can retrieve from different endpoints in the same retriever and switch
 * between different endpoints.
 */
typedef struct MsgQueueStatusEntry
{
	int64 retrieve_token;              /* The parallel cursor token, also as the hash table entry key */
	dsm_segment *mq_seg;               /* The dsm handle which contains shared memory message queue */
	shm_mq_handle *mq_handle;          /* Shared memory message queue */
	TupleTableSlot *retrieve_ts;       /* tuple slot */
	TupleQueueReader *tq_reader;
	enum RetrieveStatus retrieve_status;
} MsgQueueStatusEntry;

/*
 * Local structure to record current parallel cursor token and other info.
 */
typedef struct EndpointControl
{
	int64 Gp_token;                            /* Current parallel cursor token */
	enum ParallelCursorExecRole Gp_pce_role;   /* Current parallel cursor role */
	List *Endpoint_tokens;                     /* Tokens in current xact of current endpoint,
                                                  we can clean them in case of exception */
	List *Cursor_tokens;                       /* Tokens in current xact of QD, same purpose with Endpoint_tokens */
} EndpointControl;

typedef ParallelCursorTokenDesc *ParaCursorToken;
typedef EndpointDesc *Endpoint;

#define ENDPOINT_KEY_TUPLE_DESC_LEN     1
#define ENDPOINT_KEY_TUPLE_DESC         2
#define ENDPOINT_KEY_TUPLE_QUEUE        3

extern EndpointSharedCTX *endpointSC;          /* Shared memory context with LWLocks */
extern ParallelCursorTokenDesc *SharedTokens;  /* Point to ParallelCursorTokenDesc entries in shared memory */
extern EndpointDesc *SharedEndpoints;          /* Point to EndpointDesc entries in shared memory */
extern EndpointControl EndpointCtl;            /* Endpoint ctrl */

#define EndpointsLWLock (LWLock*) endpointSC->endpointLWLocks    /* LWLocks to protect EndpointDesc entries */
#define TokensLWLock (LWLock*)(endpointSC->endpointLWLocks + 1)  /* LWLocks to protect ParallelCursorTokenDesc entries */

/* cbdendpoint.c */
/* Endpoint shared memory context init */
extern Size EndpointShmemSize(void);
extern void EndpointCTXShmemInit(void);

/*
 * Below functions should run on dispatcher.
 */
/* Functions used in declare parallel cursor stage on QD. */
extern int64 GetUniqueGpToken(void);
extern enum EndPointExecPosition GetParallelCursorEndpointPosition(
	const struct Plan *planTree);
extern List *ChooseEndpointContentIDForParallelCursor(
	const struct Plan *planTree, enum EndPointExecPosition *position);
extern void AddParallelCursorToken(int64 token, const char *name, int session_id,
								   Oid user_id, bool all_seg, List *seg_list);
/* Called during EXECUTE CURSOR stage on QD. */
extern bool CheckParallelCursorPrivilege(int64 token);
/* Get Content ID for Endpoints in execute parallel cursor finish stage on QD*/
extern List *GetContentIDsByToken(int64 token);
/* Remove parallel cursor during cursor portal drop/abort, on QD */
extern void DestroyParallelCursor(int64 token);

/*
 * Below functions should run on Endpoints(QE/QD).
 */
/* Functions used in execute parallel cursor stage, on Endpoints(QE/QD) */
extern DestReceiver *CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc);
extern void DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest);
/* Function used in execute parallel cursor finish stage, on Endpoints(QE/QD) */
extern void UnsetSenderPidOfToken(int64 token);
/* Endpoint backend register/free, execute on Endpoints(QE/QD) */
extern void AllocEndpointOfToken(int64 token);
extern void FreeEndpointOfToken(int64 token);

/* UDFs for endpoints operation */
extern Datum gp_operate_endpoints_token(PG_FUNCTION_ARGS);


/* cdbendpointretrieve.c */
/*
 * Below functions should run on retrieve role backend.
 */
extern bool FindEndpointTokenByUser(Oid user_id, const char *token_str);
extern void AttachEndpoint(void);
extern TupleDesc TupleDescOfRetrieve(void);
extern void RetrieveResults(RetrieveStmt *stmt, DestReceiver *dest);
extern void DetachEndpoint(bool reset_pid);


/* cdbendpointutils.c */
/* Utility functions */
extern int64 GpToken(void);
extern void CheckTokenValid(void);
extern void SetGpToken(int64 token);
extern void ClearGpToken(void);
extern int64 ParseToken(char *token);
extern char *PrintToken(int64 token_id); /* Need to pfree() the result */
extern void SetParallelCursorExecRole(enum ParallelCursorExecRole role);
extern void ClearParallelCursorExecRole(void);
extern enum ParallelCursorExecRole GetParallelCursorExecRole(void);
extern const char *EndpointRoleToString(enum ParallelCursorExecRole role);

/* Utility functions to handle tokens and endpoints in shared memory */
extern bool endpoint_on_qd(ParaCursorToken para_cursor_token);
extern bool dbid_has_token(ParaCursorToken para_cursor_token, int16 dbid);
extern bool dbid_in_bitmap(int32 *bitmap, int16 dbid);
extern void add_dbid_into_bitmap(int32 *bitmap, int16 dbid);
extern int get_next_dbid_from_bitmap(int32 *bitmap, int prevbit);
extern volatile EndpointDesc *find_endpoint_by_token(int64 token);

/* UDFs for endpoints info*/
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);

#endif   /* CDBENDPOINT_H */
