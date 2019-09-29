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

#define MAX_ENDPOINT_SIZE				 1024
#define ENDPOINT_TOKEN_LEN				 16
#define ENDPOINT_TOKEN_STR_LEN			 (2 + ENDPOINT_TOKEN_LEN * 2) // "tk0A1B...4E5F"
#define InvalidSession					 (-1)

#define GP_ENDPOINT_STATUS_READY		 "READY"
#define GP_ENDPOINT_STATUS_RETRIEVING	 "RETRIEVING"
#define GP_ENDPOINT_STATUS_FINISH		 "FINISH"
#define GP_ENDPOINT_STATUS_RELEASED		 "RELEASED"

#define ENDPOINT_KEY_TUPLE_DESC_LEN		1
#define ENDPOINT_KEY_TUPLE_DESC			2
#define ENDPOINT_KEY_TUPLE_QUEUE		3

#define ENDPOINT_MSG_QUEUE_MAGIC		0x1949100119980802U

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
	Status_Invalid = 0,
	Status_Prepared,
	Status_Attached,
	Status_Finished,
	Status_Released
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

/*
 * Endpoint Description, entries are maintained in shared memory.
 */
typedef struct EndpointDesc
{
	char		name[ENDPOINT_NAME_LEN];		/* Endpoint name */
	char		cursorName[NAMEDATALEN];		/* Parallel cursor name */
	Oid			databaseID;		/* Database OID */
	pid_t		senderPid;		/* The PID of EPR_SENDER(endpoint), set before
								 * endpoint sends data */
	pid_t		receiverPid;	/* The retrieve role's PID that connect to
								 * current endpoint */
	dsm_handle	mqDsmHandle;	/* DSM handle, which contains shared message
								 * queue */
	Latch		ackDone;		/* Latch to sync EPR_SENDER and EPR_RECEIVER
								 * status */
	enum AttachStatus attachStatus;		/* The attach status of the endpoint */
	int			sessionID;		/* Connection session id */
	Oid			userID;			/* User ID of the current executed PARALLEL
								 * RETRIEVE CURSOR */
	bool		empty;			/* Whether current EndpointDesc slot in DSM is
								 * free */
}	EndpointDesc;


/*
 * Local structure to record current PARALLEL RETRIEVE CURSOR token and other info.
 */
typedef struct EndpointControl
{
	/* Current PARALLEL RETRIEVE CURSOR role */
	enum ParallelRetrCursorExecRole GpPrceRole;

	/*
	 * Which session that the endpoint is created in. For senders, this is the
	 * same with gp_session_id. For receivers, this is decided by the auth
	 * token.
	 */
	int			sessionID;
}	EndpointControl;

typedef EndpointDesc *Endpoint;
extern EndpointControl EndpointCtl;		/* Endpoint ctrl */

/* Endpoint shared memory utility functions in "cdbendpoint.c" */
extern EndpointDesc *get_endpointdesc_by_index(int index);
extern EndpointDesc *find_endpoint(const char *endpointName, int sessionID);
extern void get_token_by_session_id(int sessionId, Oid userID, int8 *token /* out */ );
extern int	get_session_id_for_auth(Oid userID, const int8 *token);

/* utility functions in "cdbendpointutilities.c" */
extern const char *endpoint_role_to_string(enum ParallelRetrCursorExecRole role);
extern bool token_equals(const int8 *token1, const int8 *token2);
extern bool endpoint_name_equals(const char *name1, const char *name2);
extern void parse_token(int8 *token /* out */ , const char *tokenStr);
extern char *print_token(const int8 *token);	/* Need to pfree() the result */

#endif   /* CDBENDPOINTINTERNAL_H */
