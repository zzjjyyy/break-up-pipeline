#include "parser/ZJY.h"
#include "fe_utils/simple_list.h"
#include "commands/event_trigger.h"
#include "commands/portalcmds.h"
#include "utils/relmapper.h"
#include "commands/vacuum.h"

#define NEWBETTER 1
#define OLDBETTER 2

//check whether this NullTest clause should be remained
static bool doNullTest(NullTest* expr, Index* transfer_array);
//check whether this OpExpr clause should be remained
static bool doOpExpr(OpExpr* expr, Index* transfer_array);
//check whether this ScalarArrayOpExpr clause should be remained
static bool doScalarArrayOpExpr(ScalarArrayOpExpr* expr, Index* transfer_array);
static Plan* find_node_with_nleaf_recursive(Plan* plan, int nleaf, int* leaf_has);
//from postgres.c
extern void finish_xact_command();
static Plan* get_deep_leaf(Plan* plan, int* depth);
//Get local rtables' foreign keys
static List* grFK(List* rtable);
// Is this expr refer to two relationship table ?
static bool is_2relationship(OpExpr* opexpr, bool* is_relationship, int length);
//Is this a Entity-to-Relationship Join ?
static bool is_ER(OpExpr* opexpr, bool* is_relationship, int length);
//Is a foreign key join ?
static bool is_FK(OpExpr* opexpr, List* fklist);
//Is a restrict clause ?
static bool is_RC(Expr* expr);
//Make a aggregation function as result
static List* makeAggref(List* targetList);
static bool not_in_targetList(TargetEntry* tar, Query* query);
//give the new value to some var, prepare for the next subquery
static void Prepare4Next(Query* global_query, DR_intorel* receiver, PlannedStmt* plannedstmt);
//Remove used jointree
static List* simplifyjoinlist(List* list,Index* rel);
//from postgres.c
extern void start_xact_command();
static void walk_plantree(Plan* plan, Index* rel);
//Execute the local query
static void ZJYExecutor(char* query_string, const char* commandTag, Node* pstmt, PlannedStmt* query, CommandDest dest, char* relname, char* completionTag, Query* querytree);
//find the subquery with lowest cost to be executed
static PlannedStmt* ZJYOptimizer(Query* global_query);

//the number of subquery
static int queryId = 0;
//where to send the result, to the client end or temporary table
CommandDest mydest;

//The interface
void doZJYparse(char* query_string, const char* commandTag, Node* pstmt, Query* querytree, char* completionTag)
{
	PlannedStmt* plannedstmt = NULL;
	while (plannedstmt = ZJYOptimizer(querytree))
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
			ZJYExecutor(query_string, commandTag, pstmt, plannedstmt, mydest, relname, completionTag, querytree);
			if (mydest == DestRemote)
			{
				break;
			}
		}
		else
		{
			break;
		}
	}
	return;
}

static int hasNext(Oid* no2relid, int length)
{
	int cnt = 0;
	for (int i = 0; i < length; i++)
	{
		bool has_same = false;
		int target = no2relid[i];
		for (int j = i - 1; j  >= 0; j--)
		{
			if (target == no2relid[j])
			{
				has_same = true;
				break;
			}
		}
		if (!has_same)
			cnt++;
	}
	return cnt;
}

//Planner
static PlannedStmt* ZJYOptimizer(Query* query)
{
	PlannedStmt* result = NULL;
	if (query->rtable->length == 2)
		mydest = DestRemote;
	else
		mydest = DestIntoRel;
	start_xact_command();
	if (query->commandType == CMD_UTILITY)
	{
		result = makeNode(PlannedStmt);
		result->commandType = CMD_UTILITY;
		result->canSetTag = query->canSetTag;
		result->utilityStmt = query->utilityStmt;
		result->stmt_location = query->stmt_location;
		result->stmt_len = query->stmt_len;
	}
	else if(mydest == DestRemote)
	{
		query->targetList = makeAggref(query->targetList);
		query->hasAggs = true;
		result = planner(query, CURSOR_OPT_PARALLEL_OK, NULL);
	}
	else
	{
		result = planner(copyObjectImpl(query), CURSOR_OPT_PARALLEL_OK, NULL);
		Plan* plan = result->planTree;
		int depth = 0;
		Plan* temp = find_node_with_nleaf_recursive(plan, 2, &depth);
		//if(temp == NULL)
		//	temp = find_node_with_nleaf_recursive(plan, 2);
		result->planTree = temp;
		ListCell* lc;
		if (mydest == DestIntoRel)
		{
			foreach(lc, result->planTree->targetlist)
			{
				TargetEntry* tar = (TargetEntry*)lfirst(lc);
				Var* var = (Var*)tar->expr;
				RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, var->varnoold - 1);
				char* relname = rte->eref->aliasname;
				char* attrname = ((Value*)list_nth(rte->eref->colnames, var->varoattno - 1))->val.str;
				char* str = (char*)palloc((strlen(relname) + strlen(attrname) + 2)* sizeof(char));
				sprintf(str, "%s_%s", relname, attrname);
				tar->resname = str;
			}
		}
	}
	return result;
}

//Executor
static void ZJYExecutor(char* query_string, const char* commandTag, Node* pstmt, PlannedStmt* plannedstmt, CommandDest dest, char* relname, char* completionTag, Query* querytree)
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
		Prepare4Next(querytree, (DR_intorel*)receiver, plannedstmt);
	receiver->rDestroy(receiver);
	PortalDrop(portal, false);
	finish_xact_command();
	EndCommand(completionTag, dest);
}

//Expr is a filter clause?
static bool is_RC(Expr* expr)
{
	if (expr->type != T_OpExpr)
		return true;
	OpExpr* opexpr = (OpExpr*)expr;
	return (((Node*)lfirst(opexpr->args->head->next))->type == T_Const);
}

static bool doNullTest(NullTest* expr, Index* rel)
{
	NodeTag type = nodeTag(expr->arg);
	Var* var = NULL;
	if (type == T_Var)
		var = (Var*)expr->arg;
	else if (type == T_RelabelType)
		var = (Var*)((RelabelType*)expr->arg)->arg;
	if(var->varno == rel[0])
		return true;
	else if (var->varno == rel[1])
		return true;
	return false;
}

static bool doOpExpr(OpExpr* expr, Index* rel)
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
		if (var1->varno == rel[0])
			return true;
		else if (var1->varno == rel[1])
			return true;
		return false;
	}
	else
	{
		if (var1->varno == rel[0] && var2->varno == rel[1])
		{
			return true;
		}
		else if (var1->varno == rel[1] && var2->varno == rel[0])
		{
			return true;
		}
		return false;
	}
}

static bool doScalarArrayOpExpr(ScalarArrayOpExpr* expr, Index* rel)
{
	NodeTag type = nodeTag(lfirst(expr->args->head));
	Var* var = NULL;
	if (type == T_Var)
		var = (Var*)lfirst(expr->args->head);
	else if (type == T_RelabelType)
		var = (Var*)((RelabelType*)lfirst(expr->args->head))->arg;
	if (var->varno == rel[0])
		return true;
	else if (var->varno == rel[1])
		return true;
	return false;
}

static List* simplifyjoinlist(List* list, Index* rel)
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
			flag = doNullTest(nulltest, rel);
			break;
		}
		case T_OpExpr:
		{
			OpExpr* opexpr = (OpExpr*)expr;
			flag = doOpExpr(opexpr, rel);
			break;
		}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr* scalararrayopexpr = (ScalarArrayOpExpr*)expr;
			flag = doScalarArrayOpExpr(scalararrayopexpr, rel);
			break;
		}
		case T_BoolExpr:
		{
			BoolExpr* boolexpr = (BoolExpr*)expr;
			boolexpr->args = simplifyjoinlist(boolexpr->args, rel);
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

static void change_rtable(List* rtables, PlannedStmt* plannedstmt, Index merge, Oid new_id)
{
	RangeTblEntry* rte = (RangeTblEntry*)list_nth(rtables, merge - 1);
	rte->relid = new_id;
	ListCell* lc;
	list_free(rte->eref->colnames);
	rte->eref->colnames = NIL;
	foreach(lc, plannedstmt->planTree->targetlist)
	{
		TargetEntry* tar = (TargetEntry*)lfirst(lc);
		Value* value = makeString(tar->resname);
		rte->eref->colnames = lappend(rte->eref->colnames, value);
	}
	return;
}

static void delete_jointree(Query* query, Index* rel)
{
	switch (query->jointree->quals->type)
	{
	case T_List:
	{
		(List*)query->jointree->quals = simplifyjoinlist((List*)query->jointree->quals, rel);
		break;
	}
	case T_BoolExpr:
	{
		((BoolExpr*)query->jointree->quals)->args = simplifyjoinlist(((BoolExpr*)query->jointree->quals)->args, rel);
		break;
	}
	case T_OpExpr:
		return;
	}
}

static void remove_redundant_join(Query* query)
{
	List* list = NIL;
	switch (query->jointree->quals->type)
	{
		case T_List:
		{
			list = (List*)query->jointree->quals;
			break;
		}
		case T_BoolExpr:
		{
			list = ((BoolExpr*)query->jointree->quals)->args;
			break;
		}
		case T_OpExpr:
			return;
	}
	Assert(list != NULL);
	for (int i = 0; i < list->length - 1; i++)
	{
		Expr* expr1 = (Expr*)list_nth(list, i);
		if (is_RC(expr1))
			continue;
		OpExpr* opexpr = ((OpExpr*)expr1);
		int left_varno, right_varno;
		NodeTag type1 = ((Node*)lfirst(opexpr->args->head))->type;
		Var* var1 = NULL;
		Var* var2 = NULL;
		if (type1 == T_Var)
		{
			var1 = (Var*)lfirst(opexpr->args->head);
		}
		else if (type1 == T_RelabelType)
		{
			var1 = (Var*)((RelabelType*)lfirst(opexpr->args->head))->arg;
		}
		NodeTag type2 = ((Node*)lfirst(opexpr->args->head->next))->type;
		if (type2 == T_Var)
		{
			var2 = (Var*)lfirst(opexpr->args->head->next);
		}
		else if (type2 == T_RelabelType)
		{
			var2 = (Var*)((RelabelType*)lfirst(opexpr->args->head->next))->arg;
		}
		left_varno = var1->varno;
		right_varno = var2->varno;
		for (int j = i + 1; j < list->length; j++)
		{
			Expr* expr2 = (Expr*)list_nth(list, j);
			if (is_RC(expr2))
				continue;
			opexpr = ((OpExpr*)expr2);
			type1 = ((Node*)lfirst(opexpr->args->head))->type;
			if (type1 == T_Var)
			{
				var1 = (Var*)lfirst(opexpr->args->head);
			}
			else if (type1 == T_RelabelType)
			{
				var1 = (Var*)((RelabelType*)lfirst(opexpr->args->head))->arg;
			}
			type2 = ((Node*)lfirst(opexpr->args->head->next))->type;
			if (type2 == T_Var)
			{
				var2 = (Var*)lfirst(opexpr->args->head->next);
			}
			else if (type2 == T_RelabelType)
			{
				var2 = (Var*)((RelabelType*)lfirst(opexpr->args->head->next))->arg;
			}
			if (left_varno == var1->varno && right_varno == var2->varno)
			{
				list = list_delete(list, expr2);
			}
			else if (left_varno == var2->varno && right_varno == var1->varno)
			{
				list = list_delete(list, expr2);
			}
		}
	}
	return;
}

static void merge_rtable(Query* query, PlannedStmt* plannedstmt, Index merge, Index delete)
{
	query->rtable = list_delete(query->rtable, list_nth(query->rtable, delete - 1));
	query->jointree->fromlist = list_delete(query->jointree->fromlist, list_nth(query->jointree->fromlist, delete - 1));
	ListCell* lc;
	foreach(lc, query->jointree->fromlist)
	{
		RangeTblRef* ref = (RangeTblRef*)lfirst(lc);
		if (ref->rtindex > delete)
		{
			ref->rtindex = ref->rtindex - 1;
		}
		else if (ref->rtindex == delete)
		{
			ref->rtindex = merge;
		}
	}
	List* varlist = pull_var_clause((Node*)query->jointree, 0);
	foreach(lc, varlist)
	{
		Var* var = (Var*)lfirst(lc);
		if (var->varno > delete)
		{
			var->varno = var->varno - 1;
			var->varnoold = var->varno;
		}
		else if (var->varno == delete)
		{
			var->varno = merge;
			var->varnoold = var->varno;
			ListCell* lc1;
			int cnt = 0;
			foreach(lc1, plannedstmt->planTree->targetlist)
			{
				cnt++;
				TargetEntry* tar = (TargetEntry*)lfirst(lc1);
				Var* tVar = (Var*)tar->expr;
				Index varno = tVar->varnoold;
				Index varattno = tVar->varoattno;
				if (varno == delete && varattno == var->varoattno)
				{
					var->varattno = cnt;
					var->varoattno = var->varattno;
					break;
				}
			}
		}
		else if(var->varno == merge)
		{
			var->varnoold = var->varno;
			ListCell* lc1;
			int cnt = 0;
			foreach(lc1, plannedstmt->planTree->targetlist)
			{
				cnt++;
				TargetEntry* tar = (TargetEntry*)lfirst(lc1);
				Var* tVar = (Var*)tar->expr;
				Index varno = tVar->varnoold;
				Index varattno = tVar->varoattno;
				if (varno == merge && varattno == var->varoattno)
				{
					var->varattno = cnt;
					var->varoattno = var->varattno;
					break;
				}
			}
		}
	}
}

static void change_targetList(Query* query, PlannedStmt* plannedstmt, Index merge, Index delete, Oid relid)
{
	ListCell* lc;
	foreach(lc, query->targetList)
	{
		TargetEntry* tar1 = (TargetEntry*)lfirst(lc);
		Var* tVar1 = (Var*)tar1->expr;
		if (tVar1->varno == merge)
		{
			tVar1->varnoold = tVar1->varno;
			tar1->resorigtbl = relid;
			int cnt = 0;
			ListCell* lc1;
			foreach(lc1, plannedstmt->planTree->targetlist)
			{
				cnt++;
				TargetEntry* tar = (TargetEntry*)lfirst(lc1);
				Var* tVar = (Var*)tar->expr;
				Index varno = tVar->varnoold;
				Index varattno = tVar->varoattno;
				if (varno == merge && varattno == tVar1->varoattno)
				{
					tVar1->varattno = cnt;
					tVar1->varoattno = tVar1->varattno;
					break;
				}
			}
		}
		else if (tVar1->varno == delete)
		{
			tar1->resorigtbl = relid;
			tVar1->varno = merge;
			tVar1->varnoold = tVar1->varno;
			int cnt = 0;
			ListCell* lc1;
			foreach(lc1, plannedstmt->planTree->targetlist)
			{
				cnt++;
				TargetEntry* tar = (TargetEntry*)lfirst(lc1);
				Var* tVar = (Var*)tar->expr;
				Index varno = tVar->varnoold;
				Index varattno = tVar->varoattno;
				if (varno == delete && varattno == tVar1->varoattno)
				{
					tVar1->varattno = cnt;
					tVar1->varoattno = tVar1->varattno;
					break;
				}
			}
		}
		else if (tVar1->varno > delete)
		{
			tVar1->varno = tVar1->varno - 1;
			tVar1->varnoold = tVar1->varno;
		}
	}
	return;
}

static void Prepare4Next(Query* query,  DR_intorel* receiver, PlannedStmt* plannedstmt)
{
	Index rel[2] = { 0, 0 };
	walk_plantree(plannedstmt->planTree, rel);
	delete_jointree(query, rel);
	Index merge_info[2];
	if (rel[0] > rel[1])
	{
		merge_info[0] = rel[1];
		merge_info[1] = rel[0];
	}
	else
	{
		merge_info[0] = rel[0];
		merge_info[1] = rel[1];
	}
	merge_rtable(query, plannedstmt, merge_info[0], merge_info[1]);
	Oid relid = RangeVarGetRelid(receiver->into->rel, NoLock, true);
	change_rtable(query->rtable, plannedstmt, merge_info[0], relid);
	change_targetList(query, plannedstmt, merge_info[0], merge_info[1], relid);
	remove_redundant_join(query);
}

static List* makeAggref(List* targetList)
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

static void walk_plantree(Plan* plan, Index* rel)
{
	Index res = 0;
	if (plan->lefttree == NULL)
	{
		res = ((Scan*)plan)->scanrelid;
		if (rel[0] == 0)
			rel[0] = res;
		else
			rel[1] = res;
	}
	if (plan->lefttree != NULL)
		walk_plantree(plan->lefttree, rel);
	if(plan->righttree != NULL)
		walk_plantree(plan->righttree, rel);
	return;
}

static Plan* find_node_with_nleaf_recursive(Plan* plan, int nleaf, int* leaf_has)
{
	if (plan->lefttree == NULL)
	{
		*leaf_has = 1;
		return NULL;
	}
	int left_leaf = 0;
	int right_leaf = 0;
	Plan* res = NULL;
	res = find_node_with_nleaf_recursive(plan->lefttree, nleaf, &left_leaf);
	if (res)
		return res;
	if (plan->righttree)
		res = find_node_with_nleaf_recursive(plan->righttree, nleaf, &right_leaf);
	if (res)
		return res;
	*leaf_has = left_leaf + right_leaf;
	if (*leaf_has == nleaf)
		return plan;
	return NULL;
}

static bool not_in_targetList(TargetEntry* tar, Query* query)
{
	ListCell* lc;
	Var* vtar = (Var*)tar->expr;
	foreach(lc, query->targetList)
	{
		TargetEntry* ori_tar = (TargetEntry*)lfirst(lc);
		Var* ori_vtar = (Var*)ori_tar->expr;
		if (vtar->varnoold == ori_vtar->varno && vtar->varattno == ori_vtar->varattno)
		{
			tar->resname = (char*)palloc((strlen(ori_tar->resname) + 1) * sizeof(char));
			strcpy(tar->resname, ori_tar->resname);
			return false;
		}
	}
	return true;
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

static bool is_2relationship(OpExpr* opexpr, bool* is_relationship, int length)
{
	Var* var1 = (Var*)lfirst(opexpr->args->head);
	Var* var2 = (Var*)lfirst(opexpr->args->head->next);
	if (is_relationship[var1->varno - 1] && is_relationship[var2->varno - 1])
		return true;
	return false;
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