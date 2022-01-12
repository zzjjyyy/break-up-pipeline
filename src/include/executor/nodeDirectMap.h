/*-------------------------------------------------------------------------
 *
 * nodeDirectMap.h
 *
 *
 *
 *
 * src/include/executor/nodeDirectMap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEDIRECTMAP_H
#define NODEDIRECTMAP_H

#include "nodes/execnodes.h"

extern DirectMapState* ExecInitDirectMap(DirectMap* node, EState* estate, int eflags);
extern void ExecEndDirectMap(DirectMapState* node);
extern void ExecReScanDirectMap(DirectMapState* node);

#endif							/* NODEDIRECTMAP_H */