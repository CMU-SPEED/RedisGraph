/*
* Copyright 2018-2021 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "RG.h"
#include "../../query_ctx.h"
#include "../ops/op_node_by_label_scan.h"
#include "../ops/op_conditional_traverse.h"
#include "../execution_plan_build/execution_plan_modify.h"
#include "../../arithmetic/algebraic_expression/utils.h"

// this optimization scans through each label-scan operation
// in case the node being scaned is associated with multiple labels
// e.g. MATCH (n:A:B) RETURN n
// we will prefer to scan through the label with the least number of nodes
// for the above example if NNZ(A) < NNZ(B) we will want to iterate over A
//
// in-case this optimization changed the label scanned e.g. from A to B
// it will have to patch the following conditional traversal removing B operand
// and adding A back
//
// consider MATCH (n:A:B)-[:R]->(m) RETURN m
// 
// Scan(A)
// Traverse B*R
//
// if we switch from Scan(A) to Scan(B)
// we will have to update the traversal pattern from B*R to A*R
//
// Scan(B)
// Traverse A*R

static void _optimizeLabelScan(NodeByLabelScan *scan) {
	ASSERT(scan != NULL);	

	OpBase      *op  =  (OpBase*)scan;
	Graph       *g   =  QueryCtx_GetGraph();
	QueryGraph  *qg  =  op->plan->query_graph;

	// see if scanned node has multiple labels
	const char *node_alias = scan->n.alias;
	QGNode *qn = QueryGraph_GetNodeByAlias(qg, node_alias);
	ASSERT(qn != NULL);

	// return if node has only one label
	uint label_count = QGNode_LabelCount(qn);
	ASSERT(label_count >= 1);
	if(label_count == 1) return;

	// node has multiple labels
	// find label with minimum entities
	uint64_t    min_nnz = UINT64_MAX;  // tracks min entries
	int         min_label_id;          // tracks min label ID
	const char *min_label_str = NULL;  // tracks min label name

	for(uint i = 0; i < label_count; i++) {
		uint64_t nnz;
		int label_id = QGNode_GetLabelID(qn, i);
		nnz = Graph_LabeledNodeCount(g, label_id);
		if(min_nnz > nnz) {
			min_nnz = nnz;
			min_label_id = label_id;
			min_label_str = QGNode_GetLabel(qn, i);
		}
	}

	// scanned label has the minimum number of entries, no switching required
	if(min_label_id == scan->n.label_id) return;

	// swap current label with minimum label
	scan->n.label = min_label_str;
	scan->n.label_id = min_label_id;

	// patch following traversal, skip filters
	OpBase *parent = op->parent;
	while(OpBase_Type(parent) == OPType_FILTER) parent = parent->parent;
	if(OpBase_Type(op->parent) != OPType_CONDITIONAL_TRAVERSE) return;

	OpCondTraverse *op_traverse = (OpCondTraverse*)op->parent;
	AlgebraicExpression *ae = op_traverse->ae;
	AlgebraicExpression *operand;

	const char *row_domain = scan->n.alias;
	const char *column_domain = scan->n.alias;

	bool found = AlgebraicExpression_LocateOperand(ae, &operand, NULL,
			row_domain, column_domain, NULL, min_label_str);
	ASSERT(found == true);

	AlgebraicExpression *replacement = AlgebraicExpression_NewOperand(NULL,
			true, AlgebraicExpression_Source(operand),
			AlgebraicExpression_Destination(operand), NULL, min_label_str);

	_AlgebraicExpression_InplaceRepurpose(operand, replacement);
}

void optimizeLabelScan(ExecutionPlan *plan) {
	ASSERT(plan != NULL);

	// collect all label scan operations
	OPType t = OPType_NODE_BY_LABEL_SCAN;
	OpBase **label_scan_ops = ExecutionPlan_CollectOpsMatchingType(plan->root,
			&t ,1);

	// for each label scan operation try to optimize scanned label
	uint op_count = array_len(label_scan_ops);
	for(uint i = 0; i < op_count; i++) {
		NodeByLabelScan *label_scan = (NodeByLabelScan*)label_scan_ops[i];
		_optimizeLabelScan(label_scan);
	}
}

