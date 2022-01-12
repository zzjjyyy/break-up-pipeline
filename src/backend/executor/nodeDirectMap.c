/*-------------------------------------------------------------------------
 *
 * nodeDirectMap.c
 *	  Routines to handle direct map nodes
 *
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodDirectMap.c
 *
 */

#include "postgres.h"

#include "access/heapam.h"
#include "optimizer/optimizer.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/tupdesc.h"
#include "access/valid.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "executor/hashjoin.h"
#include "executor/nodeDirectMap.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/itemid.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"

#define DM_BUILD_MAP 1
#define DM_NEED_NEW_OUTER 2
#define DM_SCAN_MAP 3
#define DM_SCAN_MAP_NEXT 4

#define OCCUPIED_BIT 0
#define PREV_BIT 1
#define NEXT_BIT 2
#define HEADER 3

#define ATT_IS_PACKABLE(att) ((att)->attlen == -1 && (att)->attstorage != 'p')
#define VARLENA_ATT_IS_PACKABLE(att) ((att)->attstorage != 'p')

#define UNKNOWN 0xcdcdcdcd

#define ALLOCSET_MAP_SIZES ALLOCSET_DEFAULT_MINSIZE, 1024 * 1024, (1024 * 1024 * 1024)

void fillMap(DirectMapState* node, Form_pg_attribute att, Datum* MapP, uint16* infomask, Datum datum, bool is_null);
static bool DirectFetchInnerTuple(DirectMapState* node, Datum* map, ExprContext* econtext, unsigned int* offsete);
static Datum* ExecMapCreate(DirectMapState* node, PlanState* innerPlan, ExprContext* econtext, Bitmapset* inner_attr);

static pg_attribute_always_inline TupleTableSlot* ExecDirectMap(PlanState* pstate)
{
	DirectMapState* node = castNode(DirectMapState, pstate);
	DirectMap* dm;
	PlanState* innerPlan;
	PlanState* outerPlan;
	TupleTableSlot* outerTupleSlot;
	ExprState* joinqual;
	ExprState* otherqual;
	ExprContext* econtext;
	CHECK_FOR_INTERRUPTS();
	dm = (DirectMap*)node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	Datum* map = node->dm_Map;
	//elog(ERROR, "Stop here.");
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		ResetExprContext(econtext);
		switch (node->dm_JoinState)
		{
			case DM_BUILD_MAP:
			{	
				Assert(map == NULL);
				ExprEvalStep* op = joinqual->steps;
				for(int i = 0;i < joinqual->steps_len; i++)
				{
					if (op[i].opcode == EEOP_INNER_VAR)
					{
						node->dm_innerattr = op[i].d.var.attnum + 1;
					}
					else if (op[i].opcode == EEOP_OUTER_VAR)
					{
						node->dm_outerattr = op[i].d.var.attnum + 1;
					}
				}
				ExprState pi_state = node->js.ps.ps_ProjInfo->pi_state;
				op = pi_state.steps;
				Bitmapset* inner_attrs = bms_add_member(NULL, node->dm_innerattr);
				for (int i = 0; i < pi_state.steps_len; i++)
				{
					if (op[i].opcode == EEOP_ASSIGN_INNER_VAR)
					{
						inner_attrs = bms_add_member(inner_attrs, op[i].d.assign_var.attnum + 1);
					}
				}
				//node->mapCxt = AllocSetContextCreate(CurrentMemoryContext, "MapContext", ALLOCSET_DEFAULT_SIZES);
				node->mapCxt = AllocSetContextCreate(CurrentMemoryContext, "MapContext", ALLOCSET_MAP_SIZES);
				//node->mapStrCxt = AllocSetContextCreate(CurrentMemoryContext, "MapStrContext", ALLOCSET_DEFAULT_SIZES);
				map = ExecMapCreate(node, innerPlan, econtext, inner_attrs);
				node->dm_Map = map;
				node->dm_JoinState = DM_NEED_NEW_OUTER;
			}
			case DM_NEED_NEW_OUTER:
			{	
				outerTupleSlot = ExecProcNode(outerPlan);
				if (TupIsNull(outerTupleSlot))
				{
					return NULL;
				}
				econtext->ecxt_outertuple = outerTupleSlot;
				if (outerTupleSlot->tts_tableOid != 0)
				{
					outerTupleSlot->tts_ops->getsomeattrs(outerTupleSlot, outerTupleSlot->tts_tupleDescriptor->natts);
				}
				node->offset = outerTupleSlot->tts_values[node->dm_outerattr - 1];
				if (node->offset >= node->mapsize)
				{
					continue;
				}
				node->dm_JoinState = DM_SCAN_MAP;
			}
			case DM_SCAN_MAP:
			{
				if (!DirectFetchInnerTuple(node, map, econtext, &node->offset))
				{
					ResetExprContext(econtext);
					continue;
				}
				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			case DM_SCAN_MAP_NEXT:
			{
				if (!DirectFetchInnerTuple(node, map, econtext, &node->offset))
				{
					ResetExprContext(econtext);
					continue;
				}
				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			default:
			{
				elog(ERROR, "In ExecDirectMap: unrecognized directmap state: %d", (int)node->dm_JoinState);
			}
		}
	}
}
/* ----------------------------------------------------------------
 *		ExecInitDirectMap
 * ----------------------------------------------------------------
 */
DirectMapState* ExecInitDirectMap(DirectMap* node, EState* estate, int eflags)
{
	DirectMapState* dmstate;
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	dmstate = makeNode(DirectMapState);
	dmstate->js.ps.plan = (Plan*)node;
	dmstate->js.ps.state = estate;
	dmstate->js.ps.ExecProcNode = ExecDirectMap;
	ExecAssignExprContext(estate, &dmstate->js.ps);
	outerPlanState(dmstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->directParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(dmstate) = ExecInitNode(innerPlan(node), estate, eflags);
	TupleDesc innerDesc = ExecGetResultType(innerPlanState(dmstate));
	const TupleTableSlotOps* ops = ExecGetResultSlotOps(innerPlanState(dmstate), NULL);
	ExecInitResultTupleSlotTL(&dmstate->js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&dmstate->js.ps, NULL);
	dmstate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState*)dmstate);
	dmstate->js.jointype = node->join.jointype;
	dmstate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState*)dmstate);
	dmstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);
	dmstate->dm_JoinState = DM_BUILD_MAP;
	switch (node->join.jointype)
	{
	case JOIN_INNER:
	case JOIN_SEMI:
		break;
	case JOIN_LEFT:
	case JOIN_ANTI:
		dmstate->dm_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(dmstate)), &TTSOpsVirtual);
		break;
	default:
		elog(ERROR, "In ExecInitDirectMap: unrecognized join type: %d", (int)node->join.jointype);
	}
	dmstate->dm_outerattr = 0;
	dmstate->dm_innerattr = 0;
	dmstate->dm_Map = NULL;
	dmstate->mapsize = 0;
	dmstate->nDatumperTuple = 0;
	return dmstate;
}
/* ----------------------------------------------------------------
 *		ExecEndDirectMap
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndDirectMap(DirectMapState* node)
{
	ExecFreeExprContext(&node->js.ps);
	pfree(node->dm_Map);
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
	MemoryContextDelete(node->mapCxt);
	//MemoryContextDelete(node->mapStrCxt);
}
/* ----------------------------------------------------------------
 *		ExecReScanDirectMap
 * ----------------------------------------------------------------
 */
void ExecReScanDirectMap(DirectMapState* node)
{
	if (node->dm_Map != NULL)
	{
		node->dm_JoinState = DM_NEED_NEW_OUTER;
	}
	PlanState* outerPlan = outerPlanState(node);
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

//offset start from 1
static bool DirectFetchInnerTuple(DirectMapState* node, Datum* map, ExprContext* econtext, unsigned int* offset)
{
	//map: qual value[1] ... value[n]
	Datum* mslot = map + *offset * node->nDatumperTuple;
	if (mslot[OCCUPIED_BIT] != 1)
	{
		node->dm_JoinState = DM_NEED_NEW_OUTER;
		return false;
	}
	if (mslot[PREV_BIT] != UNKNOWN && node->dm_JoinState == DM_SCAN_MAP)
	{
		node->dm_JoinState = DM_NEED_NEW_OUTER;
		return false;
	}
	if (mslot[NEXT_BIT] != UNKNOWN)
	{
		//Have next tuple
		*offset = mslot[NEXT_BIT];
		node->dm_JoinState = DM_SCAN_MAP_NEXT;
	}
	else if(mslot[OCCUPIED_BIT] == 1)
	{
		//Have no next tuple
		node->dm_JoinState = DM_NEED_NEW_OUTER;
	}
	else
	{
		//No tuple here
		node->dm_JoinState = DM_NEED_NEW_OUTER;
		return false;
	}
	econtext->ecxt_innertuple->tts_flags = 16;
	econtext->ecxt_innertuple->tts_nvalid = node->nDatumperTuple - HEADER;
	econtext->ecxt_innertuple->tts_values = mslot + NEXT_BIT + 1;
	return true;
}

static Datum* ExecMapCreate(DirectMapState* node, PlanState* planstate, ExprContext* econtext, Bitmapset* inner_attrs)
{
	TupleTableSlot* slot;
	node->mapsize = planstate->plan->plan_rows * 1.1;
	unsigned int num;
	slot = ExecProcNode(planstate);
	planstate->ps_ExprContext->ecxt_scantuple = slot;
	econtext->ecxt_innertuple = slot;
	//1 for valid, 1 for prev, 1 for next, other for attr value
	node->nDatumperTuple = HEADER + slot->tts_tupleDescriptor->natts;
	MemoryContext oldcxt = MemoryContextSwitchTo(node->mapCxt);
	/* The stucture for the mapline | occupied | prev | next | value[] | */
	Datum* res = palloc(node->mapsize * node->nDatumperTuple * sizeof(Datum));
	MemoryContextSwitchTo(oldcxt);
	unsigned int guest_pos = 0;
	for (int i = 1;;i++)
	{
		if (TupIsNull(slot))
			break;
		if (slot->tts_tableOid != 0)
		{
			slot->tts_ops->getsomeattrs(slot, slot->tts_tupleDescriptor->natts);
		}
		else
		{
			num = 0;
		}
		num = slot->tts_values[node->dm_innerattr - 1];
		if (num >= node->mapsize)
		{
			node->mapsize = num * 1.5;//1.5 for 23, 24, 30 for 1.5
			oldcxt = MemoryContextSwitchTo(node->mapCxt);
			res = repalloc(res, node->mapsize * node->nDatumperTuple * sizeof(Datum));
			MemoryContextSwitchTo(oldcxt);
		}
		//find the first address for the mapline
		Datum* ptr = res + num * node->nDatumperTuple;
		CHECK_FOR_INTERRUPTS();
		//Check the occupied bit
		if (ptr[OCCUPIED_BIT] == 1)
		{
			/* The mapline is occupied */
			if (ptr[PREV_BIT] == UNKNOWN)
			{
				//The mapline is occupied by host
				Datum* prev_ptr = ptr;
				unsigned int prev_pos = num;
				//Check the 'next' bit
				while (prev_ptr[NEXT_BIT] != UNKNOWN)
				{
					prev_pos = prev_ptr[NEXT_BIT];
					prev_ptr = res + prev_pos * node->nDatumperTuple;
				}
				//Find the guest position
				do
				{
					ptr = res + guest_pos * node->nDatumperTuple;
					guest_pos++;
					if (guest_pos >= node->mapsize)
					{
						node->mapsize = guest_pos * 1.1;
						oldcxt = MemoryContextSwitchTo(node->mapCxt);
						res = repalloc(res, node->mapsize * node->nDatumperTuple * sizeof(Datum));
						MemoryContextSwitchTo(oldcxt);
					}
				} while (ptr[OCCUPIED_BIT] == 1);
				prev_ptr[NEXT_BIT] = guest_pos - 1;
				ptr[OCCUPIED_BIT] = 1;
				ptr[PREV_BIT] = prev_pos;
				ptr[NEXT_BIT] = UNKNOWN;
			}
			else
			{
				//The mapline is occupied by guest
				unsigned int prev_pos = ptr[PREV_BIT];
				Datum* prev_ptr = res + prev_pos * node->nDatumperTuple;
				Datum* guest_ptr = ptr;
				do
				{
					guest_ptr = res + guest_pos * node->nDatumperTuple;
					guest_pos++;
					if (guest_pos >= node->mapsize)
					{
						node->mapsize = guest_pos * 1.1;
						oldcxt = MemoryContextSwitchTo(node->mapCxt);
						res = repalloc(res, node->mapsize * node->nDatumperTuple * sizeof(Datum));
						MemoryContextSwitchTo(oldcxt);
					}
				} while (guest_ptr[OCCUPIED_BIT] == 1);
				memcpy(guest_ptr, ptr, node->nDatumperTuple * sizeof(Datum));
				prev_ptr[NEXT_BIT] = guest_pos - 1;
				ptr[OCCUPIED_BIT] = 1;
				ptr[PREV_BIT] = UNKNOWN;
				ptr[NEXT_BIT] = UNKNOWN;
			}
		}
		else
		{
			ptr[OCCUPIED_BIT] = 1;
			ptr[PREV_BIT] = UNKNOWN;
			ptr[NEXT_BIT] = UNKNOWN;
		}
		for (int j = 1; j <= node->nDatumperTuple - HEADER; j++)
		{
			if (!bms_is_member(j, inner_attrs))
			{
				ptr[NEXT_BIT + j] = 0;
				continue;
			}
			else
			{
				oldcxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
				uint16 infomask = ~(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);
				fillMap(node, &slot->tts_tupleDescriptor->attrs[j - 1], ptr + NEXT_BIT + j, &infomask, slot->tts_values[j - 1], slot->tts_isnull[j - 1]);
				MemoryContextSwitchTo(oldcxt);
			}
		}
		slot = ExecProcNode(planstate);
	}
	return res;
}

void fillMap(DirectMapState* node, Form_pg_attribute att, Datum* MapP, uint16* infomask, Datum datum, bool is_null)
{
	Size data_length;
	if (is_null)
	{
		*MapP = NULL;
		return;
	}
	if (att->attbyval)
	{
		switch (att->attlen)
		{
			case sizeof(char):
			{
				*MapP = DatumGetChar(datum);
				break;
			}
			case sizeof(int16):
			{	
				*MapP = DatumGetInt16(datum);
				break;
			}
			case sizeof(int32):
			{
				*MapP = DatumGetInt32(datum);
				break;
			}
			default:
			{
				elog(ERROR, "unsupported byval length: %d", (int)(att->attlen));
				break;
			}
		}
		data_length = att->attlen;
		return;
	}
	else if (att->attlen == -1)
	{
		Pointer val = DatumGetPointer(datum);
		if (VARATT_IS_EXTERNAL(val))
		{
			if (VARATT_IS_EXTERNAL_EXPANDED(val))
			{
				ExpandedObjectHeader* eoh = DatumGetEOHP(datum);
				data_length = EOH_get_flat_size(eoh);
				//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
				char* data = (char*)palloc0(data_length * (sizeof(char)));
				//MemoryContextSwitchTo(oldcxt);
				EOH_flatten_into(eoh, data, data_length);
				*MapP = data;
				return;
			}
			else
			{
				*infomask |= HEAP_HASEXTERNAL;
				data_length = VARSIZE_EXTERNAL(val);
				//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
				char* data = (char*)palloc0((data_length + 2) * (sizeof(char)));
				//MemoryContextSwitchTo(oldcxt);
				memcpy(data, val, data_length + 2);
				*MapP = data;
				return;
			}
		}
		else if (VARATT_IS_SHORT(val))
		{
			data_length = VARSIZE_SHORT(val);
			//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
			char* data = (char*)palloc0((data_length + 2)* (sizeof(char)));
			//MemoryContextSwitchTo(oldcxt);
			memcpy(data, val, (data_length + 2));
			*MapP = data;
			return;
		}
		else if (VARLENA_ATT_IS_PACKABLE(att) && VARATT_CAN_MAKE_SHORT(val))
		{
			data_length = VARATT_CONVERTED_SHORT_SIZE(val);
			//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
			char* data = (char*)palloc0(data_length * (sizeof(char)));
			//MemoryContextSwitchTo(oldcxt);
			SET_VARSIZE_SHORT(data, data_length);
			memcpy(data + 1, VARDATA(val), data_length - 1);
			*MapP = data;
			return;
		}
		else
		{
			data_length = VARSIZE(val);
			Assert(data_length < 300);
			//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
			char* data = (char*)palloc0((data_length + 2)* (sizeof(char)));
			//MemoryContextSwitchTo(oldcxt);
			memcpy(data, val, data_length + 2);
			*MapP = data;
			return;
		}
	}
	else if (att->attlen == -2)
	{
		*infomask |= HEAP_HASVARWIDTH;
		Assert(att->attalign == 'c');
		data_length = strlen(DatumGetCString(datum)) + 1;
		//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
		char* data = (char*)palloc0(data_length * (sizeof(char)));
		//MemoryContextSwitchTo(oldcxt);
		memcpy(data, DatumGetPointer(datum), data_length);
		*MapP = data;
		return;
	}
	else
	{
		Assert(att->attlen > 0);
		data_length = att->attlen;
		//MemoryContext oldcxt = MemoryContextSwitchTo(node->mapStrCxt);
		char* data = (char*)palloc0(data_length * (sizeof(char)));
		//MemoryContextSwitchTo(oldcxt);
		memcpy(data, DatumGetPointer(datum), data_length);
		*MapP = data;
		return;
	}
	return;
}

