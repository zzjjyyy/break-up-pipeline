/*-------------------------------------------------------------------------
 *
 * 
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/ZJY.c
 *
 *-------------------------------------------------------------------------
 */
#include "parser/ZJY.h"
#include "fe_utils/simple_list.h"
#include "commands/event_trigger.h"
#include "commands/portalcmds.h"
#include "utils/relmapper.h"
#include "commands/vacuum.h"

#define NEWBETTER 1
#define OLDBETTER 2

//which plan is better?
static int compare_cost(PlannedStmt* new, PlannedStmt* old);
//Create a local query
static Query* createQuery(const Query* querytree, CommandDest dest, List* rtable, Index* transfer_array, int length);
//change the RangeTblEntry relid to the new one
static void dochange(RangeTblEntry* rte, Relation relation, Oid relid);
//check whether this NullTest clause should be remained
static bool doNullTest(NullTest* expr, Index* transfer_array);
//change the NullTest clause args to the new one
static bool doNullTestTransfor(NullTest* expr, Index* transfer_array);
//check whether this OpExpr clause should be remained
static bool doOpExpr(OpExpr* expr, Index* transfer_array);
//change the OpExpr clause args to the new one
static bool doOpExprTransfor(OpExpr* expr, Index* transfer_array);
//check whether this ScalarArrayOpExpr clause should be remained
static bool doScalarArrayOpExpr(ScalarArrayOpExpr* expr, Index* transfer_array);
//change the ScalarArrayOpExpr clause args to the new one
static bool doScalarArrayOpExprTransfor(ScalarArrayOpExpr* expr, Index* transfer_array);
//Get the var will link to unlocal table
static List* findvarlist(List* joinlist, Index* transfer_array, int length);
//from postgres.c
extern void finish_xact_command();
//Get local rtable
static List* getRT(List* global_rtable, bool* graph, int length, int i, Index* transfer_array);
//Get local rtables' foreign keys
static List* grFK(List* rtable);
//is this subquery is the last ?
static int hasNext(bool* graph, int length);
// Is this expr refer to two relationship table ?
static bool is_2relationship(OpExpr * opexpr, bool* is_relationship, int length);
//Is this a Entity-to-Relationship Join ?
static bool is_ER(OpExpr* opexpr, bool* is_relationship, int length);
//Is a foreign key join ?
static bool is_FK(OpExpr* opexpr, List* fklist);
//Is a restrict clause ?
static bool is_RC(Expr* expr);
//Transefer jointree to graph
static bool* List2Graph(bool* is_relationship, List* joinlist, int length);
//Make a aggregation function as result
List* makeAggref(List* targetList);
//give the new value to some var, prepare for the next subquery
static void Prepare4Next(Query* global_query, Index* transfer_array, DR_intorel* receiver, PlannedStmt* plannedstmt);
//remove redundant join
static void rRj(Query* querytree);
//Transfer fromlist to the local
static List* setfromlist(List* fromlist, Index* transfer_array, int length);
//Transfer global var to local
static List* setjoinlist(List* rclist, CommandDest dest, Index* transfer_array, int length);
//Make a local target list
static List* settargetlist(const List* global_rtable, List* local_rtable, CommandDest dest, List* varlist, List* targetlist, Index* transfer_array, int length);
//Remove used jointree
static List* simplifyjoinlist(List* list, CommandDest dest, Index* transfer_array, int length);
//Split Query by Foreign Key
static void spqFK(char* query_string, char* commandTag, Node* pstmt, Query* querytree, char* completionTag);
//from postgres.c
extern void start_xact_command();
//Execute the local query
static void ZJYExecutor(char* query_string, const char* commandTag, Node* pstmt, PlannedStmt* query, CommandDest dest, char* relname, char* completionTag, Query* querytree, Index* transfer_array);
//find the subquery with lowest cost to be executed
static PlannedStmt* ZJYOptimizer(Query* global_query, bool* graph, Index* transfer_array, int length);

//indicate the varno to the temporary relid
Oid* no2relid = NULL;
//the number of subquery
static int queryId = 0;
//where to send the result, to the client end or temporary table
CommandDest mydest;
bool* is_relationship = NULL;
Index* transfer_array = NULL;

//The interface
void doZJYparse(char* query_string, const char* commandTag, Node* pstmt, Query* querytree, char* completionTag)
{
	if (querytree->commandType != CMD_UTILITY)
	{
		//remove Redundant Join
		rRj(querytree);
	}
	//split parent query by foreign key
	spqFK(query_string, commandTag, pstmt, querytree, completionTag);
	return;
}

//remove Redundant Join
static void rRj(Query* querytree)
{
	//get all the foreign key
	List* FKlist = grFK(querytree->rtable);
	int length = querytree->rtable->length;
	is_relationship = (bool*)palloc(length * sizeof(bool));
	memset(is_relationship, false, length * sizeof(bool));
	ListCell* lc;
	//若一个表的主键不被其他表的外键引用，且引用了其他表的主键作为外键
	foreach(lc, FKlist)
	{
		ForeignKeyOptInfo* fkOptInfo = (ForeignKeyOptInfo*)lc->data.ptr_value;
		int x = fkOptInfo->con_relid - 1;
		is_relationship[x] = true;
	}
	//这个循环用来解决title和kind_type之间的bug
	foreach(lc, FKlist)
	{
		ForeignKeyOptInfo* fkOptInfo = (ForeignKeyOptInfo*)lc->data.ptr_value;
		int x = fkOptInfo->ref_relid - 1;
		is_relationship[x] = false;
	}
	//SQL where clause
	switch (querytree->jointree->quals->type)
	{
		case T_BoolExpr:
		{	
			BoolExpr* expr = (BoolExpr*)querytree->jointree->quals;
			if (expr == NULL)
				return;
			//expression list in the SQL where clause
			List* where = expr->args;
			//remove the redundant expression in expression list
			foreach(lc, where)
			{
				//is this expression a filter clause ?
				if (is_RC(lc->data.ptr_value))
				{
					continue;
				}
				//is this expression contian two relationship table ?
				if (is_2relationship(lc->data.ptr_value, is_relationship, length))
				{
					//if yes, remove it from expression list
					where = list_delete(where, lfirst(lc));
					continue;
				}
				//at和t和关系表的循环调用
				if (is_ER(lc->data.ptr_value, is_relationship, length) && !is_FK(lc->data.ptr_value, FKlist))
				{
					where = list_delete(where, lfirst(lc));
					continue;
				}
			}
			break;
		}
		case T_OpExpr:
			break;
	}
	return;
}

//split parent query by foreign key
static void spqFK(char* query_string, char* commandTag, Node* pstmt, Query* ori_query, char* completionTag)
{
	Query* global_query = copyObjectImpl(ori_query);
	PlannedStmt* plannedstmt = NULL;
	if (global_query->commandType == CMD_UTILITY)
	{
		plannedstmt = ZJYOptimizer(global_query, NULL, NULL, 0);
		ZJYExecutor(query_string, commandTag, pstmt, plannedstmt, DestRemote, NULL, completionTag, NULL, NULL);
		return;
	}
	int length = global_query->rtable->length;
	if (length == 1)
	{
		plannedstmt = ZJYOptimizer(global_query, NULL, NULL, length);
		ZJYExecutor(query_string, commandTag, pstmt, plannedstmt, DestRemote, NULL, completionTag, NULL, NULL);
		return;
	}
	no2relid = (int*)palloc(length * sizeof(Oid));
	for (int i = 0; i < length; i++)
		no2relid[i] = 0;
	List* RClist = NIL;
	List* Joinlist = NIL;
	List* WhereClause = NIL;
	switch (global_query->jointree->quals->type)
	{
		case T_BoolExpr:
		{
			BoolExpr* expr = (BoolExpr*)global_query->jointree->quals;
			WhereClause = expr->args;
			break;
		}
		case T_OpExpr:
		{
			WhereClause = lappend(WhereClause, global_query->jointree->quals);
			break;
		}
	}
	ListCell* lc;
	foreach(lc, WhereClause)
	{
		if (is_RC(lc->data.ptr_value))
			RClist = lappend(RClist, lc->data.ptr_value);
		else
			Joinlist = lappend(Joinlist, lc->data.ptr_value);
	}
	//transfer join list to join graph
	bool* graph = List2Graph(is_relationship, Joinlist, length);
	//value start from 1, index start from 0
	transfer_array = (Index*)palloc(length * sizeof(Index));
	while(plannedstmt = ZJYOptimizer(global_query, graph, transfer_array, length))
	{
		if (plannedstmt)
		{
			char* relname = NULL;
			//Should we output the result or save it as a temporary table
			if (mydest == DestIntoRel)
			{
				relname = (char*)palloc(7 * sizeof(char));
				sprintf(relname, "temp%d", queryId++);
			}
			//Execute the subquery and do some change for next subquery creation
			ZJYExecutor(query_string, commandTag, pstmt, plannedstmt, mydest, relname, completionTag, global_query, transfer_array);
		}
		else
		{
			break;
		}
	}
	pfree(transfer_array);
	pfree(graph);
	transfer_array = NULL;
	graph = NULL;
	return;
}

//Planner
static PlannedStmt* ZJYOptimizer(Query* global_query, bool* graph, Index* transfer_array, int length)
{
	PlannedStmt* result = NULL;
	start_xact_command();
	if (global_query->commandType == CMD_UTILITY)
	{
		result = makeNode(PlannedStmt);
		result->commandType = CMD_UTILITY;
		result->canSetTag = global_query->canSetTag;
		result->utilityStmt = global_query->utilityStmt;
		result->stmt_location = global_query->stmt_location;
		result->stmt_len = global_query->stmt_len;
	}
	else if (length == 1)
	{
		return planner(global_query, CURSOR_OPT_PARALLEL_OK, NULL);
	}
	else
	{
		Cost lowest_startup_cost = 0;
		Cost lowest_total_cost = 0;
		int X = 0;
		for (int i = 0; i < length; i++)
		{
			if (hasNext(graph, length) > 1)
				mydest = DestIntoRel;
			else if (hasNext(graph, length) == 1)
				mydest = DestRemote;
			else if (hasNext(graph, length) == 0)
				return NULL;
			for (int j = 0; j < length; j++)
				transfer_array[j] = 0;
			//Get the rang table list for this subgraph
			List* rtable = getRT(global_query->rtable, graph, length, i, transfer_array);
			//Can this subgraph make a join ?
			if (rtable->length < 2)
			{
				continue;
			}
			char* relname = NULL;
			//If so, create a subquery
			Query* local_query = createQuery(global_query, mydest, rtable, transfer_array, length);
			PlannedStmt* candidate_result = planner(local_query, CURSOR_OPT_PARALLEL_OK, NULL);
			if (lowest_startup_cost == 0 && lowest_total_cost == 0)
			{
				if (candidate_result->planTree->plan_rows < 10000000)
				{
					lowest_startup_cost = candidate_result->planTree->startup_cost;
					lowest_total_cost = candidate_result->planTree->total_cost;
					result = copyObjectImpl(candidate_result);
					X = i;
				}
			}
			else if (compare_cost(candidate_result, result) == NEWBETTER)
			{
				lowest_startup_cost = candidate_result->planTree->startup_cost;
				lowest_total_cost = candidate_result->planTree->total_cost;
				pfree(result);
				result = copyObjectImpl(candidate_result);
				X = i;
			}
			pfree(candidate_result);
			candidate_result = NULL;
		}
		Index index = 1;
		for (int j = 0; j < length; j++)
		{
			//graph[x][y]
			if (graph[X * length + j] == true)
			{
				transfer_array[j] = index++;
			}
			else if (X == j)
			{
				transfer_array[j] = index++;
			}
			else
			{
				transfer_array[j] = 0;
			}
		}
		switch (global_query->jointree->quals->type)
		{
			case T_BoolExpr:
			{
				((BoolExpr*)global_query->jointree->quals)->args = simplifyjoinlist(((BoolExpr*)global_query->jointree->quals)->args, mydest, transfer_array, length);
				break;
			}
			case T_OpExpr:
			{
				global_query->jointree->quals = NULL;
				break;
			}
		}
		for (int j = 0; j < length; j++)
		{
			if (graph[X * length + j] == true)
			{
				graph[X * length + j] = false;
				graph[j * length + X] = false;
			}
		}
	}
	return result;
}

//Executor
static void ZJYExecutor(char* query_string, const char* commandTag, Node* pstmt, PlannedStmt* plannedstmt, CommandDest dest, char* relname, char* completionTag, Query* querytree, Index* transfer_array)
{
	int16 format;
	Portal portal;
	List* plantree_list;
	DestReceiver* receiver = NULL;
	bool is_parallel_worker = false;
	BeginCommand(commandTag, dest);
	plantree_list = lappend(NIL, plannedstmt);
	CHECK_FOR_INTERRUPTS();
	portal = CreatePortal("", true, true);
	portal->visible = false;
	PortalDefineQuery(portal, NULL, query_string, commandTag, plantree_list, NULL);
	PortalStart(portal, NULL, 0, InvalidSnapshot);
	format = 0;
	PortalSetResultFormat(portal, 1, &format);
	if (dest == DestRemote)
	{
		receiver = CreateDestReceiver(dest);
		SetRemoteDestReceiverParams(receiver, portal);
	}
	if (dest == DestIntoRel)
	{
		IntoClause* into = makeNode(IntoClause);
		into->rel = makeRangeVar(NULL, relname, plannedstmt->stmt_location);
		into->rel->relpersistence = RELPERSISTENCE_TEMP;
		into->onCommit = ONCOMMIT_NOOP;
		into->rel->inh = false;
		into->skipData = false;
		into->viewQuery = NULL;
		receiver = CreateIntoRelDestReceiver(into);
	}
	//Executor
	(void)PortalRun(portal, FETCH_ALL, true, true, receiver, receiver, completionTag);
	//do some change
	if (dest == DestIntoRel)
		Prepare4Next(querytree, transfer_array, (DR_intorel*)receiver, plannedstmt);
	receiver->rDestroy(receiver);
	PortalDrop(portal, false);
	finish_xact_command();
	EndCommand(completionTag, dest);
}

//transfer joinlist to join graph
static bool* List2Graph(bool* is_relationship, List* joinlist, int length)
{
	bool* graph = (bool*)palloc(length * length * sizeof(bool));
	memset(graph, false, length * length * sizeof(bool));
	ListCell* lc1;
	bool flag = false;
	foreach(lc1, joinlist)
	{
		Var* var1 = (Var*)lfirst(((OpExpr*)lfirst(lc1))->args->head);
		Var* var2 = (Var*)lfirst(((OpExpr*)lfirst(lc1))->args->head->next);
		//有向graph[x][y], x->y
		if ((is_relationship[var1->varno - 1] == true) && (is_relationship[var2->varno - 1] == false))
		{
			graph[(var1->varno - 1) * length + var2->varno - 1] = true;
		}
		//graph[x * length + y] = 有向graph[x][y] = x->y
		else if ((is_relationship[var1->varno - 1] == false) && (is_relationship[var2->varno - 1] == true))
		{
			graph[(var2->varno - 1) * length + var1->varno - 1] = true;
		}
		else if ((is_relationship[var1->varno - 1] == false) && (is_relationship[var2->varno - 1] == false))
		{
			graph[(var1->varno - 1) * length + var2->varno - 1] = true;
			graph[(var2->varno - 1) * length + var1->varno - 1] = true;
		}
		//两个关系的join应该都删掉了
		//Never go to here
		else
			Assert(false);
	}
	return graph;
}

static bool is_ER(OpExpr* opexpr, bool* is_relationship, int length)
{
	Var* var1 = (Var*)lfirst(opexpr->args->head);
	Var* var2 = (Var*)lfirst(opexpr->args->head->next);
	if (is_relationship[var1->varno - 1] && is_relationship[var2->varno - 1])
		return false;
	else if (is_relationship[var1->varno - 1] || is_relationship[var2->varno - 1])
		return true;
	else
		return false;
}

static bool is_FK(OpExpr* opexpr, List* fklist)
{
	ListCell* lc = NULL;
	Var* var1 = (Var*)lfirst(opexpr->args->head);
	Var* var2 = (Var*)lfirst(opexpr->args->head->next);
	foreach(lc, fklist)
	{
		ForeignKeyOptInfo* fkOptInfo = (ForeignKeyOptInfo*)lfirst(lc);
		if (var1->varno == fkOptInfo->con_relid && var2->varno == fkOptInfo->ref_relid)
			return true;
		else if (var2->varno == fkOptInfo->con_relid && var1->varno == fkOptInfo->ref_relid)
			return true;
	}
	return false;
}

static bool is_2relationship(OpExpr* opexpr, bool* is_relationship, int length)
{
	Var* var1 = (Var*)lfirst(opexpr->args->head);
	Var* var2 = (Var*)lfirst(opexpr->args->head->next);
	if (is_relationship[var1->varno - 1] && is_relationship[var2->varno - 1])
		return true;
	return false;
}

//Expr is a filter clause?
static bool is_RC(Expr* expr)
{
	if (expr->type != T_OpExpr)
		return true;
	OpExpr* opexpr = (OpExpr*)expr;
	return (((Node*)lfirst(opexpr->args->head->next))->type == T_Const);
}

//get rtable
static List* getRT(List* prtable, bool* graph, int length, int i, Index* transfer_array)
{
	Index index = 1;
	List* rtable = NIL;
	for (int j = 0; j < length; j++)
	{
		//graph[x][y]
		if (graph[i * length + j] == true)
		{
			RangeTblEntry* rte = copyObjectImpl(list_nth(prtable, j));
			rtable = lappend(rtable, rte);
			transfer_array[j] = index++;
		}
		else if (i == j)
		{
			RangeTblEntry* rte = copyObjectImpl(list_nth(prtable, i));
			rtable = lappend(rtable, rte);
			transfer_array[j] = index++;
		}
	}
	return rtable;
}

//找到global出口
static List* findvarlist(List* joinlist, Index* transfer_array, int length)
{
	ListCell* lc;
	List* reslist = NIL;
	foreach(lc, joinlist)
	{
		Expr* expr = (Expr*)lfirst(lc);
		if (!is_RC(expr))
		{
			OpExpr* opexpr = (OpExpr*)expr;
			NodeTag type = ((Node*)lfirst(opexpr->args->head))->type;
			Var* var1 = lfirst(opexpr->args->head);
			Var* var2 = (Var*)lfirst(opexpr->args->head->next);
			//当前query到外围
			if (transfer_array[var1->varno - 1] != 0 && transfer_array[var2->varno - 1] == 0)
			{
				ListCell* lc1;
				bool append = true;
				foreach(lc1, reslist)
				{
					Var* var = (Var*)lfirst(lc1);
					if (var->varattno == var1->varattno && var->varno == var1->varno)
					{
						append = false;
						break;
					}
				}
				if (append)
				{
					Var* var = copyObjectImpl(var1);
					reslist = lappend(reslist, var);
				}
			}
			else if (transfer_array[var1->varno - 1] == 0 && transfer_array[var2->varno - 1] != 0)
			{
				ListCell* lc1;
				bool append = true;
				foreach(lc1, reslist)
				{
					Var* var = (Var*)lfirst(lc1);
					if (var->varattno == var2->varattno && var->varno == var2->varno)
					{
						append = false;
						break;
					}
				}
				if (append)
				{
					Var* var = copyObjectImpl(var2);
					reslist = lappend(reslist, var);
				}
			}
			//外围到外围
			else if (transfer_array[var1->varno - 1] == 0 && transfer_array[var2->varno - 1] == 0)
			{
				//虽然var1->varno不在当前query中，但是var1->varno指向一个临时表
				if (no2relid[var1->varno - 1] != 0)
				{
					for (int i = 0; i < length; i++)
					{
						//且当前query中的表rtable[i]和var1->varno指向同一个临时表
						if ((transfer_array[i] != 0) && (no2relid[i] == no2relid[var1->varno - 1]))
						{
							ListCell* lc1;
							bool append = true;
							foreach(lc1, reslist)
							{
								Var* var = (Var*)lfirst(lc1);
								if (var1->varattno == var->varattno && var1->varno == var->varno)
								{
									append = false;
									break;
								}
							}
							if (append)
							{
								Var* var = copyObjectImpl(var1);
								reslist = lappend(reslist, var);
								break;
							}
						}
					}
				}
				if (no2relid[var2->varno - 1] != 0)
				{
					for (int i = 0; i < length; i++)
					{
						if ((transfer_array[i] != 0) && (no2relid[i] == no2relid[var2->varno - 1]))
						{
							ListCell* lc1;
							bool append = true;
							foreach(lc1, reslist)
							{
								Var* var = (Var*)lfirst(lc1);
								if (var2->varattno == var->varattno && var2->varno == var->varno)
								{
									append = false;
									break;
								}
							}
							if (append)
							{
								Var* var = copyObjectImpl(var2);
								reslist = lappend(reslist, var);
								break;
							}
						}
					}
				}
			}
		}
	}
	return reslist;
}

static Query* createQuery(const Query* global_query, CommandDest dest, List* rtable, Index* transfer_array, int length)
{
	Query* query;
	query = makeNode(Query);
	query = copyObjectImpl(global_query);
	query->rtable = copyObjectImpl(rtable);
	query->jointree->fromlist = setfromlist(query->jointree->fromlist, transfer_array, length);
	List* varlist = NIL;
	switch (query->jointree->quals->type)
	{
		case T_BoolExpr:
		{
			varlist = findvarlist(((BoolExpr*)query->jointree->quals)->args, transfer_array, length);
			break;
		}
		case T_OpExpr:
		{
			List* temp = lappend(NIL, query->jointree->quals);
			varlist = findvarlist(temp, transfer_array, length);
			break;
		}
	}
	query->targetList = settargetlist(global_query->rtable, rtable, dest, varlist, query->targetList, transfer_array, length);
	switch (query->jointree->quals->type)
	{
		case T_BoolExpr:
		{
			((BoolExpr*)query->jointree->quals)->args = setjoinlist(((BoolExpr*)query->jointree->quals)->args, dest, transfer_array, length);
			break;
		}
		case T_OpExpr:
			break;
	}
	if (dest == DestRemote)
	{
		query->hasAggs = true;
	}
	return query;
}

static bool doNullTest(NullTest* expr, Index* transfer_array)
{
	NodeTag type = nodeTag(expr->arg);
	Var* var = NULL;
	if (type == T_Var)
		var = (Var*)expr->arg;
	else if (type == T_RelabelType)
		var = (Var*)((RelabelType*)expr->arg)->arg;
	if (no2relid[var->varno - 1] != 0)
		return false;
	if (transfer_array[var->varno - 1] == 0)
		return false;
	return true;
}

static bool doNullTestTransfor(NullTest* expr, Index* transfer_array)
{
	NodeTag type = nodeTag(expr->arg);
	Var* var = NULL;
	if (type == T_Var)
		var = (Var*)expr->arg;
	else if (type == T_RelabelType)
		var = (Var*)((RelabelType*)expr->arg)->arg;
	if (no2relid[var->varno - 1] != 0)
		return false;
	if (transfer_array[var->varno - 1] == 0)
		return false;
	var->varno = transfer_array[var->varno - 1];
	var->varnoold = var->varno;
	return true;
}

static bool doOpExpr(OpExpr* expr, Index* transfer_array)
{
	NodeTag type1 = ((Node*)lfirst(expr->args->head))->type;
	Var* var1 = NULL;
	Var* var2 = NULL;
	if (type1 == T_Var)
	{
		var1 = (Var*)lfirst(expr->args->head);
	}
	else if (type1 == T_RelabelType)
	{
		var1 = (Var*)((RelabelType*)lfirst(expr->args->head))->arg;
	}
	NodeTag type2 = ((Node*)lfirst(expr->args->head->next))->type;
	if (type2 == T_Var)
	{
		var2 = (Var*)lfirst(expr->args->head->next);
	}
	else if (type2 == T_RelabelType)
	{
		var2 = (Var*)((RelabelType*)lfirst(expr->args->head->next))->arg;
	}
	if (var1 && type2 == T_Const)
	{
		//临时表没有filter
		if (var1 && transfer_array[var1->varno - 1] != 0)
			return true;
		else
			return false;
	}
	//如果有一个var是非当前query，就保留
	if (var1 && transfer_array[var1->varno - 1] == 0)
	{
		return false;
	}
	if (var2 && transfer_array[var2->varno - 1] == 0)
	{
		return false;
	}
	return true;
}

static bool doOpExprTransfor(OpExpr* expr, Index* transfer_array)
{
	bool flag = true;
	NodeTag type1 = ((Node*)lfirst(expr->args->head))->type;
	Var* var1 = NULL;
	Var* var2 = NULL;
	if (type1 == T_Var)
	{
		var1 = (Var*)lfirst(expr->args->head);
	}
	else if (type1 == T_RelabelType)
	{
		var1 = (Var*)((RelabelType*)lfirst(expr->args->head))->arg;
	}
	NodeTag type2 = ((Node*)lfirst(expr->args->head->next))->type;
	if (type2 == T_Var)
	{
		var2 = (Var*)lfirst(expr->args->head->next);
	}
	else if (type2 == T_RelabelType)
	{
		var2 = (Var*)((RelabelType*)lfirst(expr->args->head->next))->arg;
	}
	else if (type2 == T_Const)
	{
		if (var1 && no2relid[var1->varno - 1] != 0)
		{
			flag = false;
		}
	}
	if (var1 && transfer_array[var1->varno - 1] == 0)
	{
		flag = false;
	}
	else if (var1)
	{
		var1->varno = transfer_array[var1->varno - 1];
		var1->varnoold = var1->varno;
	}
	if (var2 && transfer_array[var2->varno - 1] == 0)
	{
		flag = false;
	}
	else if (var2)
	{
		var2->varno = transfer_array[var2->varno - 1];
		var2->varnoold = var2->varno;
	}
	return flag;
}

static bool doScalarArrayOpExpr(ScalarArrayOpExpr* expr, Index* transfer_array)
{
	NodeTag type = nodeTag(lfirst(expr->args->head));
	Var* var = NULL;
	if (type == T_Var)
		var = (Var*)lfirst(expr->args->head);
	else if (type == T_RelabelType)
		var = (Var*)((RelabelType*)lfirst(expr->args->head))->arg;
	if (no2relid[var->varno - 1] != 0)
		return false;
	if (transfer_array[var->varno - 1] == 0)
		return false;
	return true;
}

static bool doScalarArrayOpExprTransfor(ScalarArrayOpExpr* expr, Index* transfer_array)
{
	NodeTag type = nodeTag(lfirst(expr->args->head));
	Var* var = NULL;
	if(type == T_Var)
		var = (Var*)lfirst(expr->args->head);
	else if(type == T_RelabelType)
		var = (Var*)((RelabelType*)lfirst(expr->args->head))->arg;
	if (no2relid[var->varno - 1] != 0)
		return false;
	if (transfer_array[var->varno - 1] == 0)
		return false;
	var->varno = transfer_array[var->varno - 1];
	var->varnoold = var->varno;
	return true;
}

static List* setjoinlist(List* qualslist, CommandDest dest, Index* transfer_array, int length)
{
	ListCell* lc;
	foreach(lc, qualslist)
	{
		Expr* expr = (Expr*)lfirst(lc);
		bool flag = true;
		switch (expr->type)
		{
			case T_NullTest:
			{
				NullTest* nulltest = (NullTest*)expr;
				flag = doNullTestTransfor(nulltest, transfer_array);
				break;
			}
			case T_OpExpr:
			{
				OpExpr* opexpr = (OpExpr*)expr;
				flag = doOpExprTransfor(opexpr, transfer_array);
				break;
			}
			case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr* scalararrayopexpr = (ScalarArrayOpExpr*)expr;
				flag = doScalarArrayOpExprTransfor(scalararrayopexpr, transfer_array);
				break;
			}
			case T_BoolExpr:
			{
				BoolExpr* boolexpr = (BoolExpr*)expr;
				boolexpr->args = setjoinlist(boolexpr->args, dest, transfer_array, length);
				if (boolexpr->args == NULL)
					flag = false;
				break;
			}
		}
		if (!flag)
		{
			qualslist = list_delete(qualslist, lfirst(lc));
		}
	}
	return qualslist;
}

static List* simplifyjoinlist(List* list, CommandDest dest, Index* transfer_array, int length)
{
	ListCell* lc;
	foreach(lc, list)
	{
		Expr* expr = (Expr*)lfirst(lc);
		bool flag = false;
		switch (expr->type)
		{
			case T_NullTest:
			{
				NullTest* nulltest = (NullTest*)expr;
				flag = doNullTest(nulltest, transfer_array);
				break;
			}
			case T_OpExpr:
			{
				OpExpr* opexpr = (OpExpr*)expr;
				flag = doOpExpr(opexpr, transfer_array);
				break;
			}
			case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr* scalararrayopexpr = (ScalarArrayOpExpr*)expr;
				flag = doScalarArrayOpExpr(scalararrayopexpr, transfer_array);
				break;
			}
			case T_BoolExpr:
			{
				BoolExpr* boolexpr = (BoolExpr*)expr;
				boolexpr->args = simplifyjoinlist(boolexpr->args, dest, transfer_array, length);
				if (boolexpr->args == NULL)
					flag = true;
				break;
			}
		}
		if (flag)
		{
			list = list_delete(list, lfirst(lc));
		}
	}
	return list;
}

static List* setfromlist(List* fromlist, Index* transfer_array, int length)
{
	ListCell* lc;
	foreach(lc, fromlist)
	{
		RangeTblRef* ref = (RangeTblRef*)lfirst(lc);
		if (transfer_array[ref->rtindex - 1] == 0)
		{
			fromlist = list_delete(fromlist, lfirst(lc));
			continue;
		}
		ref->rtindex = transfer_array[ref->rtindex - 1];
	}
	return fromlist;
}

//varlist - global, targetlist - global
static List* settargetlist(const List* global_rtable, List* local_rtable, CommandDest dest, List* varlist, List* targetlist, Index* transfer_array, int length)
{
	ListCell* lc;
	foreach(lc, targetlist)
	{
		bool reserved = false;
		TargetEntry* tar = (TargetEntry*)lfirst(lc);
		ListCell* lc1;
		if (tar->expr->type != T_Var)
		{
			continue;
		}
		foreach(lc1, local_rtable)
		{
			RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc1);
			if (rte->relid == tar->resorigtbl)
			{
				Var* vtar = (Var*)tar->expr;
				//将global的varno指向改为local
				if (no2relid[vtar->varno - 1] != 0)
				{
					ListCell* lc2;
					int cnt = 1;
					foreach(lc2, local_rtable)
					{
						RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc2);
						if (rte->relid == no2relid[vtar->varno - 1])
						{
							vtar->varno = cnt;
							vtar->varnoold = vtar->varno;
							break;
						}
						cnt++;
					}
				}
				else
				{
					vtar->varno = transfer_array[vtar->varno - 1];
					vtar->varnoold = vtar->varno;
				}
				reserved = true;
				break;
			}
		}
		if (reserved)
			continue;
		targetlist = list_delete(targetlist, lfirst(lc));
	}
	if (dest == DestRemote)
	{
		List* re_targetlist = targetlist;
		re_targetlist = makeAggref(targetlist);
		return re_targetlist;
	}
	foreach(lc, varlist)
	{
		Var* var = (Var*)lfirst(lc);
		if (var != NULL)
		{
			TargetEntry* tar = makeNode(TargetEntry);
			RangeTblEntry* rte = (RangeTblEntry*)list_nth(global_rtable, var->varno - 1);
			tar->resorigtbl = rte->relid;
			int len = strlen(rte->eref->aliasname) + strlen(strVal(list_nth(rte->eref->colnames, var->varattno - 1))) + 2;
			tar->resname = (char*)palloc(len * sizeof(char));
			sprintf(tar->resname, "%s_%s", rte->eref->aliasname, strVal(list_nth(rte->eref->colnames, var->varattno - 1)));
			tar->resorigcol = var->varattno;
			if(targetlist)
				tar->resno = targetlist->length + 1;
			else
				tar->resno = 1;
			//该变量所在的表直接参与此次join
			if (transfer_array[var->varno - 1] != 0)
			{
				var->varno = transfer_array[var->varno - 1];
				var->varnoold = var->varno;
			}
			//该变量所在的表间接参与此次join
			else
			{
				for (int i = 0; i < length; i++)
				{
					if ((transfer_array[i] != 0) && (no2relid[i] == no2relid[var->varno - 1]))
					{
						var->varno = transfer_array[i];
						var->varnoold = var->varno;
						break;
					}
				}
			}
			tar->expr = copyObjectImpl(var);
			targetlist = lappend(targetlist, tar);
		}
	}
	return targetlist;
}

//get relation foreign key
static List* grFK(List* rtable)
{
	ListCell* lc;
	List* fkey_list = NIL;
	Index relid = 0;
	foreach(lc, rtable)
	{
		relid++;
		RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
		Relation relation;
		relation = table_open(rte->relid, NoLock);
		List* cachedfkeys;
		ListCell* lc1;
		cachedfkeys = RelationGetFKeyList(relation);
		foreach(lc1, cachedfkeys)
		{
			ForeignKeyCacheInfo* cachedfk = (ForeignKeyCacheInfo*)lfirst(lc1);
			Index rti;
			ListCell* lc2;
			Assert(cachedfk->conrelid == RelationGetRelid(relation));
			rti = 0;
			foreach(lc2, rtable)
			{
				RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc2);
				ForeignKeyOptInfo* info;
				rti++;
				if (rte->rtekind != RTE_RELATION || rte->relid != cachedfk->confrelid)
					continue;
				if (rti == relid)
					continue;
				/* OK, let's make an entry */
				info = makeNode(ForeignKeyOptInfo);
				info->con_relid = relid;
				info->ref_relid = rti;
				info->nkeys = cachedfk->nkeys;
				memcpy(info->conkey, cachedfk->conkey, sizeof(info->conkey));
				memcpy(info->confkey, cachedfk->confkey, sizeof(info->confkey));
				memcpy(info->conpfeqop, cachedfk->conpfeqop, sizeof(info->conpfeqop));
				/* zero out fields to be filled by match_foreign_keys_to_quals */
				info->nmatched_ec = 0;
				info->nmatched_rcols = 0;
				info->nmatched_ri = 0;
				memset(info->eclass, 0, sizeof(info->eclass));
				memset(info->rinfos, 0, sizeof(info->rinfos));
				fkey_list = lappend(fkey_list, info);
			}
		}
		table_close(relation, NoLock);
	}
	return fkey_list;
}

//Is this local query the last one ?
int hasNext(bool* graph, int length)
{
	bool* temp_graph = (bool*)palloc(length * length * sizeof(bool));
	for (int i = 0; i < length * length; i++)
	{
		temp_graph[i] = graph[i];
	}
	int total_cnt = 0;
	for (int i = 0; i < length; i++)
	{
		int cnt = 0;
		for (int j = 0; j < length; j++)
		{
			if (temp_graph[i * length + j] == true)
			{
				temp_graph[i * length + j] = false;
				temp_graph[j * length + i] = false;
				cnt++;
			}
		}
		if (cnt > 0)
		{
			total_cnt++;
		}
	}
	pfree(temp_graph);
	temp_graph = NULL;
	return total_cnt;
}

//Change the rte's relid and name
void dochange(RangeTblEntry* rte, Relation relation, Oid relid)
{
	rte->relid = relid;
	list_free(rte->eref->colnames);
	rte->eref->colnames = NIL;
	for (int i = 0; i < relation->rd_att->natts; i++)
	{
		char* str = (char*)palloc((strlen(relation->rd_att->attrs[i].attname.data) + 1) * sizeof(char));
		strcpy(str, relation->rd_att->attrs[i].attname.data);
		rte->eref->colnames = lappend(rte->eref->colnames, makeString(str));
	}
	return;
}

static void Prepare4Next(Query* global_query, Index* transfer_array, DR_intorel* receiver, PlannedStmt* plannedstmt)
{
	int length = global_query->rtable->length;
	Oid relid = RangeVarGetRelid(receiver->into->rel, NoLock, true);
	Relation relation = table_open(relid, NoLock);
	List* varlist = pull_var_clause((Node*)global_query->jointree, 0);
	ListCell* lc;
	foreach(lc, varlist)
	{
		Var* var = (Var*)lfirst(lc);
		RangeTblEntry* rte = (RangeTblEntry*)list_nth(global_query->rtable, var->varno - 1);
		int len = strlen(rte->eref->aliasname) + strlen(strVal(list_nth(rte->eref->colnames, var->varattno - 1))) + 2;
		char* attrname = (char*)palloc(len * sizeof(char));
		sprintf(attrname, "%s_%s", rte->eref->aliasname, strVal(list_nth(rte->eref->colnames, var->varattno - 1)));
		for (int i = 0; i < relation->rd_att->natts; i++)
		{
			if (strcmp(attrname, relation->rd_att->attrs[i].attname.data) == 0)
			{
				var->varattno = i + 1;
				break;
			}
		}
		pfree(attrname);
		attrname = NULL;
	}
	int cnt = 0;
	ListCell* lc1;
	//子查询涉及的全局relation
	foreach(lc, global_query->rtable)
	{
		if (transfer_array[cnt] != 0)
		{
			//子查询中的一个表rte
			RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
			//找到之前和rte结合过的其他表
			if (no2relid[cnt] != 0)
			{
				for (int i = 0; i < length; i++)
				{
					//Exclude self-join
					if (i == cnt)
						continue;
					//如果这个表和rte之前结合过，则将其指向新的临时表
					if (no2relid[i] == no2relid[cnt])
					{
						RangeTblEntry* r = (RangeTblEntry*)list_nth(global_query->rtable, i);
						dochange(r, relation, relid);
						no2relid[i] = relid;
					}
				}
			}
			dochange(rte, relation, relid);
			no2relid[cnt] = relid;
		}
		cnt++;
	}
	foreach(lc, global_query->targetList)
	{
		foreach(lc1, plannedstmt->relationOids)
		{
			TargetEntry* tar = (TargetEntry*)lfirst(lc);
			if (tar->resorigtbl == lc1->data.oid_value)
			{
				tar->resorigtbl = relid;
				for (int i = 0; i < relation->rd_att->natts; i++)
				{
					if (strcmp(tar->resname, relation->rd_att->attrs[i].attname.data) == 0)
					{
						tar->resorigcol = i + 1;
						((Var*)tar->expr)->varattno = i + 1;
					}
				}
				break;
			}
		}
	}
	table_close(relation, NoLock);
}

static int compare_cost(PlannedStmt* new, PlannedStmt* old)
{
	if (new->planTree->plan_rows  > 10000000)
	{
		return OLDBETTER;
	}
	else
	{

		double fac_old, fac_new;
		if (new->planTree->plan_rows > 1)
		{
			//fac_new = log(new->planTree->plan_rows) / log(2);
			//fac_new = new->planTree->plan_rows;
			fac_new = sqrt(new->planTree->plan_rows);
		}
		else
		{
			fac_new = 1;
		}
		if (old->planTree->plan_rows > 1)
		{
			//fac_old = log(old->planTree->plan_rows) / log(2);
			//fac_old = old->planTree->plan_rows;
			fac_old = sqrt(old->planTree->plan_rows);
		}
		else
		{
			fac_old = 1;
		}
		if (fac_new / fac_old > old->planTree->total_cost / new->planTree->total_cost)
		{
			return OLDBETTER;
		}
		else
		{
			return NEWBETTER;
		}

		/*
		if (old->planTree->total_cost < new->planTree->total_cost)
		{
			return OLDBETTER;
		}
		else
		{
			return NEWBETTER;
		}
		*/
	}
}

List* makeAggref(List* targetList)
{
	List* resList = NIL;
	ListCell* lc;
	foreach(lc, targetList)
	{
		TargetEntry* old_tar = (TargetEntry*)lfirst(lc);
		Oid old_vartype = ((Var*)old_tar->expr)->vartype;
		TargetEntry* tar = makeNode(TargetEntry);
		tar->resjunk = false;
		tar->resname = old_tar->resname;
		old_tar->resname = NULL;
		tar->resno = old_tar->resno;
		tar->resorigcol = 0;
		tar->resorigtbl = 0;
		tar->ressortgroupref = 0;
		Aggref* aggref = makeNode(Aggref);
		aggref->aggargtypes = lappend_oid(NIL, old_vartype);
		aggref->aggdirectargs = NULL;
		aggref->aggdistinct = NULL;
		aggref->aggfilter = NULL;
		switch (old_vartype)
		{
			case 23:
			{
				aggref->aggfnoid = 2132;
				aggref->inputcollid = 0;
				aggref->aggcollid = 0;
				aggref->aggtype = 23;
				break;
			}
			case 25:
			{
				aggref->aggfnoid = 2145;
				aggref->inputcollid = 100;
				aggref->aggcollid = 100;
				aggref->aggtype = 25;
				break;
			}
			default:
			{
				aggref->aggfnoid = 2145;
				aggref->inputcollid = 100;
				aggref->aggcollid = 100;
				aggref->aggtype = 25;
			}
		}
		aggref->aggkind = 'n';
		aggref->agglevelsup = 0;
		aggref->aggorder = NULL;
		aggref->aggsplit = AGGSPLIT_SIMPLE;
		aggref->aggstar = false;
		aggref->aggtranstype = 0;
		aggref->aggvariadic = false;
		aggref->args = lappend(NIL, old_tar);
		aggref->location = -1;
		tar->expr = aggref;
		resList = lappend(resList, tar);
	}
	return resList;
}