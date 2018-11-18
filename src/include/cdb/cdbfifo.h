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
	int32      	token;
	Latch		ack_done;
	Size		num_attributes;
	AttrDesc	attdesc[ENDPOINT_MAX_ATT_NUM];
	bool		attached;
	bool		empty;
} EndPointDesc;

/* token and segid */
typedef struct sharedtokendesc
{
	int32	token;
	int16	dbid;
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
extern void AddParallelCursorToken(int32, int16);
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
