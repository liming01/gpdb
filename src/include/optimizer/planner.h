/*-------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/optimizer/planner.h,v 1.45 2008/10/04 21:56:55 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"


/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
													   int cursorOptions,
												  ParamListInfo boundParams);
extern PGDLLIMPORT planner_hook_type planner_hook;

extern ParamListInfo PlannerBoundParamList;	 /* current boundParams */

extern PlannedStmt *planner(Query *parse, int cursorOptions,
		ParamListInfo boundParams);
extern PlannedStmt *standard_planner(Query *parse, int cursorOptions,
				 ParamListInfo boundParams);

extern Plan *subquery_planner(PlannerGlobal *glob, Query *parse,
				 PlannerInfo *parent_root,
				 bool hasRecursion,
				 double tuple_fraction,
				 PlannerInfo **subroot,
				 PlannerConfig *config);

extern bool choose_hashed_grouping(PlannerInfo *root,
								   double tuple_fraction, double limit_tuples,
								   Path *cheapest_path,
								   Path *sorted_path,
								   Oid *groupOperators, int numGroupOps,
								   double dNumGroups,
								   AggClauseCounts *agg_counts);

#endif   /* PLANNER_H */
