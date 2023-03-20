/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../../arithmetic/algebraic_expression.h"
#include "../../graph/rg_matrix/rg_matrix_iter.h"
#include "../execution_plan.h"
#include "op.h"
#include "shared/traverse_functions.h"

typedef struct {
	size_t *I;
    size_t *J;
    size_t *V;
    size_t I_size;
    size_t J_size;
} CSRRecord;

/* OP Traverse */
typedef struct {
    OpBase op;
    Graph *graph;
    AlgebraicExpression *ae;
    RG_Matrix F;  // Filter matrix.
    RG_Matrix M;  // Algebraic expression result.
    RG_Matrix *M_list;
    uint M_list_cap;
    uint M_list_cur;
    uint *IM;

    size_t **IC_list;
    size_t **JC_list;
    size_t *IC_size_list;
    size_t *JC_size_list;
    size_t iter_i;
    size_t iter_j;

	CSRRecord *prev_R;

    EdgeTraverseCtx
        *edge_ctx;  // Edge collection data if the edge needs to be set.
    RG_MatrixTupleIter iter;  // Iterator over M.
    int srcNodeIdx;           // Source node index into record.
    int destNodeIdx;          // Destination node index into record.
    uint record_count;        // Number of held records.
    uint record_cap;          // Max number of records to process.
    Record *records;          // Array of records.
    Record r;                 // Currently selected record.
} OpCondTraverse;

/* Creates a new Traverse operation */
OpBase *NewCondTraverseOp(const ExecutionPlan *plan, Graph *g,
                          AlgebraicExpression *ae);
