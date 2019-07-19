/*
 * cdbendpoint.h
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 */

#ifndef CDBENDPOINT_H
#define CDBENDPOINT_H

#include <inttypes.h>

#include "cdb/cdbutil.h"
#include "nodes/parsenodes.h"
#include "storage/latch.h"
#include "tcop/dest.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"

#define InvalidToken        (-1)
#define InvalidSession      (-1)

#define GP_ENDPOINT_STATUS_INIT		  "INIT"
#define GP_ENDPOINT_STATUS_READY	  "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH	  "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED   "RELEASED"

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
 * Endpoint attach status.
 */
typedef enum AttachStatus
{
	Status_NotAttached = 0,
	Status_Prepared,
	Status_Attached,
	Status_Finished
}	AttachStatus;

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

/* Endpoint shared memory context init and dsm create/attach */
extern Size Endpoint_ShmemSize(void);
extern void Endpoint_CTX_ShmemInit(void);
extern void AttachOrCreateEndpointAndTokenDSM(void);
extern bool AttachOrCreateEndpointDsm(bool attachOnly);
extern bool AttachOrCreateTokenDsm(bool attachOnly);

/* Declare parallel cursor stage */
extern int64 GetUniqueGpToken(void);
extern void AddParallelCursorToken(int64 token, const char *name, int session_id,
                                   Oid user_id, bool all_seg, List *seg_list);

/* Execute parallel cursor stage, start sender job */
extern DestReceiver *CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc);
extern void DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest);

/* Execute parallel cursor finish stage, unset sender pid and exit retrieve if needed */
extern void UnsetSenderPidOfToken(int64 token);

/* Remove parallel cursor during cursor portal drop/abort */
extern void RemoveParallelCursorToken(int64 token);

/* Endpoint backend register/free, execute on endpoints(QE or QD) */
extern void AllocEndpointOfToken(int64 token);
extern void FreeEndpointOfToken(int64 token);

// "gp_endpoints_token_operation" GUC hook.
extern void assign_gp_endpoints_token_operation(const char *newval, void *extra);

/* Retrieve role auth */
extern bool FindEndpointTokenByUser(Oid user_id, const char *token_str);

/* For retrieve role. Must have endpoint allocated */
extern void AttachEndpoint(void);
extern TupleDesc TupleDescOfRetrieve(void);
extern void RetrieveResults(RetrieveStmt * stmt, DestReceiver *dest);
extern void DetachEndpoint(bool reset_pid);


/* Utilities */
extern int64 GpToken(void);
extern void SetGpToken(int64 token);
extern void ClearGpToken(void);
extern int64 parseToken(char *token);
extern char* printToken(int64 token_id); /* Need to pfree() the result */

extern void SetParallelCursorExecRole(enum ParallelCursorExecRole role);
extern void ClearParallelCursorExecRole(void);
extern enum ParallelCursorExecRole GetParallelCursorExecRole(void);

extern List *GetContentIDsByToken(int64 token);


/* UDFs for endpoint */
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);

#endif   /* CDBENDPOINT_H */
