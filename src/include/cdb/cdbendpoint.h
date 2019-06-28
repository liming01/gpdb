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

#define InvalidToken (-1)
#define InvalidTokenIndex (-1)
#define InvalidSession (-1)
#define DummyToken			(0) /* For fault injection */

#define MAX_ENDPOINT_SIZE	1024
#define MAX_FIFO_NAME_SIZE	100
#define POLL_FIFO_TIMEOUT	50
#define FIFO_NAME_PATTERN "gp2gp_%d_"INT64_FORMAT
#define SHMEM_TOKEN "SharedMemoryToken"
#define SHMEM_TOKEN_SLOCK "SharedMemoryTokenSlock"
#define SHMEM_END_POINT "SharedMemoryEndpoint"
#define SHMEM_END_POINT_SLOCK "SharedMemoryEndpointSlock"

#define GP_ENDPOINT_STATUS_INIT		  "INIT"
#define GP_ENDPOINT_STATUS_READY	  "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH	  "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED   "RELEASED"

#define GP_ENDPOINTS_INFO_ATTRNUM 8

#define ep_log(level, ...) \
	do { \
		if (!StatusInAbort) \
			elog(level, __VA_ARGS__); \
	} \
	while (0)

enum EndpointRole
{
	EPR_SENDER = 1,
	EPR_RECEIVER,
	EPR_NONE
};

enum RetrieveStatus
{
	RETRIEVE_STATUS_INIT,
	RETRIEVE_STATUS_GET_TUPLEDSCR,
	RETRIEVE_STATUS_GET_DATA,
	RETRIEVE_STATUS_FINISH,
};

typedef enum AttachStatus
{
	Status_NotAttached = 0,
	Status_Prepared,
	Status_Attached,
	Status_Finished
}	AttachStatus;

typedef struct EndpointDesc
{
	Oid			database_id;
	pid_t		sender_pid;
	pid_t		receiver_pid;
	int64		token;
	dsm_handle	handle;
	Latch		ack_done;
	AttachStatus attach_status;
	int			session_id;
	Oid			user_id;
	bool		empty;
}	EndpointDesc;

typedef EndpointDesc *Endpoint;

/*
 * SharedTokenDesc is a entry to store the information of a token, includes:
 * token: token number
 * cursor_name: the parallel cursor's name
 * session_id: which session created this parallel cursor
 * endpoint_cnt: how many endpoints are created.
 * all_seg: a flag to indicate if the endpoints are on all segments.
 * dbIds: a bitmap stores the dbids of every endpoint, size is 4906 bits(32X128).
 */
#define MAX_NWORDS 128
typedef struct sharedtokendesc
{
	int64		token;
	char		cursor_name[NAMEDATALEN];
	int			session_id;
	int			endpoint_cnt;
	Oid			user_id;
	bool		all_seg;
	int32		dbIds[MAX_NWORDS];
}	SharedTokenDesc;

typedef SharedTokenDesc *SharedToken;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
}	DR_mq_printtup;

typedef struct MessageQueueData
{
	dsm_segment*   mq_seg;
	shm_mq_handle* mq_handle;
	bool		   finished;
}	MessageQueueData;

typedef struct
{
	int64		token;
	int			dbid;
	AttachStatus attach_status;
	pid_t		sender_pid;
}	EndpointStatus;

typedef struct
{
	int			curTokenIdx;
	/* current index in shared token list. */
	CdbComponentDatabaseInfo *seg_db_list;
	int			segment_num;
	/* number of segments */
	int			curSegIdx;
	/* current index of segment id */
	EndpointStatus *status;
	int			status_num;
}	EndpointsInfo;

typedef struct
{
	int			endpoints_num;
	/* number of endpointdesc in the list */
	int			current_idx;
	/* current index of endpointdesc in the list */
}	EndpointsStatusInfo;

extern int64 GetUniqueGpToken(void);
extern void AddParallelCursorToken(int64 token, const char *name, int session_id, Oid user_id, bool all_seg, List *seg_list);
extern void RemoveParallelCursorToken(int64 token);
extern int64 parseToken(char *token);
extern char* getTokenNameFormatStr(void);

/* Need to pfree() the result */
extern char* printToken(int64 token_id);
extern void SetGpToken(int64 token);
extern void ClearGpToken(void);
extern void SetEndpointRole(enum EndpointRole role);
extern void ClearEndpointRole(void);
extern int64 GpToken(void);
extern enum EndpointRole EndpointRole(void);
extern Size Token_ShmemSize(void);
extern Size Endpoint_ShmemSize(void);
extern void AllocEndpointOfToken(int64 token);
extern void FreeEndpointOfToken(int64 token);
extern bool FindEndpointTokenByUser(Oid user_id, const char *token_str);
extern void UnsetSenderPidOfToken(int64 token);
extern void AttachEndpoint(void);
extern void DetachEndpoint(bool reset_pid);
extern TupleDesc TupleDescOfRetrieve(void);
extern void AbortEndpoint(void);
extern List *GetContentIDsByToken(int64 token);
extern void RetrieveResults(RetrieveStmt * stmt, DestReceiver *dest);
extern DestReceiver *CreateEndpointReceiver(void);
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);
extern void assign_gp_endpoints_token_operation(const char *newval, void *extra);

#define SHMEM_TOKENDSMCTX "ShareTokenDSMCTX"
#define SHMEM_TOKEN_CTX_SLOCK "SharedMemoryTokenCTXSlock"

typedef struct TokenDSMCtx {
	dsm_handle token_info_handle;
	dsm_handle endpoint_info_handle;
} TokenDSMCtx;

static TokenDSMCtx* tokenDSMCtx;

extern void Token_DSM_CTX_ShmemInit(void);
extern void AttachOrCreateTokenInfoDSM(bool* is_token_attached, bool* is_endpoint_attached);

#endif   /* CDBENDPOINT_H */
