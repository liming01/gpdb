/*-------------------------------------------------------------------------
 * cdbendpoint.h
 *	  Functions supporting the Greenplum Endpoint PARALLEL RETRIEVE CURSOR.
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
 * #NOTE: Orca is not support PARALLEL RETRIEVE CURSOR for now. It should fall back
 * to postgres optimizer.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *
 * IDENTIFICATION
 *		src/include/cdb/cdbendpoint.h
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
	ENDPOINT_ON_Entry_DB,
	ENDPOINT_ON_SINGLE_QE,
	ENDPOINT_ON_SOME_QE,
	ENDPOINT_ON_ALL_QE
};

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
extern void WaitEndpointReady(const struct Plan *planTree, const char *cursorName);

/*
 * Below functions should run on Endpoints(QE/Entry DB).
 */
extern DestReceiver *CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc, const char *cursorName);
extern void DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest);

/* UDFs for endpoints internal operation */
extern Datum gp_operate_endpoints_token(PG_FUNCTION_ARGS);
extern Datum gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
extern Datum gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS);


/* cdbendpointretrieve.c */
/*
 * Below functions should run on retrieve role backend.
 */
extern bool AuthEndpoint(Oid userID, const char *tokenStr);
extern TupleDesc GetRetrieveStmtTupleDesc(const RetrieveStmt *stmt);
extern void ExecRetrieveStmt(const RetrieveStmt *stmt, DestReceiver *dest);


/* cdbendpointutils.c */
/* Utility functions */
extern void SetParallelCursorExecRole(enum ParallelRetrCursorExecRole role);
extern void ClearParallelCursorExecRole(void);
extern enum ParallelRetrCursorExecRole GetParallelCursorExecRole(void);

/* UDFs for endpoints info*/
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);

#endif   /* CDBENDPOINT_H */
