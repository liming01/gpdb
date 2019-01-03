/*-------------------------------------------------------------------------
 * cdbfifo.h
 *	  Fifo used for inter process communication.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBFIFO_H
#define CDBFIFO_H

#include "postgres.h"
#include "storage/latch.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"

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
} AttrDesc;

#define ENDPOINT_MAX_ATT_NUM 1000
#define InvalidToken		(-1)

typedef struct sendpointdesc
{
	Oid			database_id;
	pid_t		sender_pid;
	pid_t		receiver_pid;
	int32		token;
	Latch		ack_done;
	Size		num_attributes;
	AttrDesc	attdesc[ENDPOINT_MAX_ATT_NUM];
	bool		attached;
	bool		empty;
} EndPointDesc;

#define INVALID_SESSION_ID -1

#define SHAREDTOKEN_DBID_NUM 64
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
	int32	token;
	char	cursor_name[NAMEDATALEN];
	int		session_id;
	int		endpoint_cnt;
	bool	all_seg;
	int16	dbIds[SHAREDTOKEN_DBID_NUM];
} SharedTokenDesc;

typedef EndPointDesc *EndPoint;

typedef SharedTokenDesc *SharedToken;

extern Size EndPoint_ShmemSize(void);
extern void EndPoint_ShmemInit(void);

extern void Token_ShmemInit(void);

extern int32 GetUniqueGpToken(void);
extern void SetGpToken(int32 token);
extern void ClearGpToken(void);
extern void DismissGpToken(void);
extern void AddParallelCursorToken(int32, const char*, int, bool, List*);
extern void ClearParallelCursorToken(int32);
extern int32 GpToken(void);

extern void SetEndPointRole(enum EndPointRole role);
extern void ClearEndPointRole(void);
extern enum EndPointRole EndPointRole(void);

extern void AllocEndPoint(TupleDesc tupdesc);
extern void FreeEndPoint(void);

extern void AttachEndPoint(void);
extern void DetachEndPoint(void);
extern TupleDesc ResultTupleDesc(void);

extern void InitConn(void);

extern void SendTupleSlot(TupleTableSlot *slot);
extern TupleTableSlot* RecvTupleSlot(void);

extern void FinishConn(void);
extern void CloseConn(void);

extern void AbortEndPoint(void);

#endif   /* CDBFIFO_H */