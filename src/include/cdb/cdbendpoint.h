/*-------------------------------------------------------------------------
 * cdbendpoint.h
 *	  Fifo used for inter process communication.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBENDPOINT_H
#define CDBENDPOINT_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"
#include "storage/latch.h"
#include "tcop/dest.h"


enum EndPointRole
{
	EPR_SENDER = 1,
	EPR_RECEIVER,
	EPR_NONE
};

typedef struct attrdesc
{
	NameData	attname;
	Oid			atttypid;
}	AttrDesc;

#define InvalidToken		(-1)

typedef enum AttachStatus
{
	Status_NotAttached = 0,
	Status_Attached,
	Status_Finished
}	AttachStatus;

typedef struct sendpointdesc
{
	Oid			database_id;
	pid_t		sender_pid;
	pid_t		receiver_pid;
	int32		token;
	Latch		ack_done;
	AttachStatus attached;
	int			session_id;
	Oid			user_id;
	bool		empty;
}	EndPointDesc;

typedef struct token
{
	int32		token;
	int			session_id;
	Oid			user_id;
}	Token;


#define INVALID_SESSION_ID -1

#define SHAREDTOKEN_DBID_NUM 64
#define TOKEN_NAME_FORMAT_STR "TK%010d"
/*
 * SharedTokenDesc is a entry to store the information of a token, includes:
 * token: token number
 * cursor_name: the parallel cursor's name
 * session_id: which session created this parallel cursor
 * endpoint_cnt: how many endpoints are created.
 * all_seg: a flag to indicate if the endpoints are on all segments.
 * dbIds is a int16 array, It stores the dbids of every endpoint
 */
typedef struct sharedtokendesc
{
	int32		token;
	char		cursor_name[NAMEDATALEN];
	int			session_id;
	int			endpoint_cnt;
	Oid			user_id;
	bool		all_seg;
	int16		dbIds[SHAREDTOKEN_DBID_NUM];
}	SharedTokenDesc;

typedef EndPointDesc *EndPoint;

typedef SharedTokenDesc *SharedToken;

extern Size EndPoint_ShmemSize(void);
extern void EndPoint_ShmemInit(void);

extern void Token_ShmemInit(void);

extern int32 GetUniqueGpToken(void);
extern int32 parseToken(char *token);
extern char* printToken(int32 token_id);
extern void SetGpToken(int32, int, Oid);
extern void ClearGpToken(void);
extern void DismissGpToken(void);
extern void AddParallelCursorToken(int32, const char *, int, Oid, bool, List *);
extern void ClearParallelCursorToken(int32);
extern int32 GpToken(void);

extern void SetEndPointRole(enum EndPointRole role);
extern void ClearEndPointRole(void);
extern enum EndPointRole EndPointRole(void);
extern void SetSendPid4EndPoint(void);
extern void UnSetSendPid4MyEndPoint(void);
extern bool isEndPointOnQD(SharedToken);
extern List* getContentidListByToken(int);
extern void AllocEndPoint4token(int token);
extern void FreeEndPoint4token(int token);
extern void UnSetSendPid4EndPoint(int token);
extern void SetAttachStatus4MyEndPoint(AttachStatus status);
extern void ResetEndPointRecvPid(volatile EndPointDesc * endPointDesc);
extern void ResetEndPointSendPid(volatile EndPointDesc * endPointDesc);
extern void ResetEndPointToken(volatile EndPointDesc * endPointDesc);
extern bool FindEndPointTokenByUser(Oid user_id, const char *token_str);
extern volatile EndPointDesc *FindEndPointByToken(int token);

extern void AttachEndPoint(void);
extern void DetachEndPoint(bool reset_pid);
extern TupleDesc ResultTupleDesc(void);

extern void InitConn(void);
extern void SendTupdescToFIFO(TupleDesc tupdesc);

extern void SendTupleSlot(TupleTableSlot *slot);
extern TupleTableSlot *RecvTupleSlot(void);

extern void FinishConn(void);
extern void CloseConn(void);

extern void AbortEndPoint(void);

extern void RetrieveResults(RetrieveStmt * stmt, DestReceiver *dest);

extern DestReceiver *CreateEndpointReceiver(void);

#endif   /* CDBENDPOINT_H */
