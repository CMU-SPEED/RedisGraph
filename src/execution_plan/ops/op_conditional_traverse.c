/*start_query_plan*/
#define QPLAN_ID 2
/*end_query_plan*/

const int QPLAN[6][4][4] = {
    // 0 - 3-chain
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 0, 0}},
    // 1 - 3-clique
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {1, 1, 0, 0}, {0, 0, 0, 0}},
    // 2 - 4-chain
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}},
    // 3 - 4-loop
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {0, 1, 0, 0}, {1, 0, 1, 0}},
    // 4 - 4-diamond
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {1, 1, 0, 0}, {0, 1, 1, 0}},
    // 5 - 4-clique
    {{0, 0, 0, 0}, {1, 0, 0, 0}, {1, 1, 0, 0}, {1, 1, 1, 0}}};

/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include <omp.h>
#include <time.h>

#include "../../query_ctx.h"
#include "../../subgraph_enumeration/subgraph_enumeration.hpp"
#include "../../util/simple_timer.h"
#include "RG.h"
#include "op_conditional_traverse.h"
#include "shared/print_functions.h"

// default number of records to accumulate before traversing
// #define BATCH_SIZE 16
#define BATCH_SIZE 100000000

/* Forward declarations. */
static OpResult CondTraverseInit(OpBase *opBase);
static Record CondTraverseConsume(OpBase *opBase);
static OpResult CondTraverseReset(OpBase *opBase);
static OpBase *CondTraverseClone(const ExecutionPlan *plan,
                                 const OpBase *opBase);
static void CondTraverseFree(OpBase *opBase);

static void CondTraverseToString(const OpBase *ctx, sds *buf) {
    TraversalToString(ctx, buf, ((const OpCondTraverse *)ctx)->ae);
}

static void _populate_filter_matrix(OpCondTraverse *op) {
    GrB_Matrix FM = RG_MATRIX_M(op->F);

    // clear filter matrix
    GrB_Matrix_clear(FM);

    // update filter matrix F, set row i at position srcId
    // F[i, srcId] = true
    for (uint i = 0; i < op->record_count; i++) {
        Record r = op->records[i];
        Node *n = Record_GetNode(r, op->srcNodeIdx);
        NodeID srcId = ENTITY_GET_ID(n);
        GrB_Matrix_setElement_BOOL(FM, true, i, srcId);
    }
}

// evaluate algebraic expression:
// prepends filter matrix as the left most operand
// perform multiplications
// set iterator over result matrix
// removed filter matrix from original expression
// clears filter matrix
void _traverse(OpCondTraverse *op) {
    // if op->F is null, this is the first time we are traversing
    if (op->F == NULL) {
        // create both filter and result matrices
        size_t required_dim = Graph_RequiredMatrixDim(op->graph);
        RG_Matrix_new(&op->M, GrB_BOOL, op->record_cap, required_dim);
        RG_Matrix_new(&op->F, GrB_BOOL, op->record_cap, required_dim);

        // prepend filter matrix to algebraic expression as the leftmost operand
        AlgebraicExpression_MultiplyToTheLeft(&op->ae, op->F);

        // optimize the expression tree
        AlgebraicExpression_Optimize(&op->ae);
    }

    double result = 0.0;
    double tic[2];

    GrB_Matrix M, S, A;
    printf("Mask and Selector Generation: ");
    simple_tic(tic);
    {
        GrB_Index M_nrows, M_ncols;
        GrB_Matrix_nrows(&M_nrows, op->M->matrix);
        GrB_Matrix_ncols(&M_ncols, op->M->matrix);
        GrB_Matrix_new(&M, GrB_BOOL, M_nrows, M_ncols);
        GrB_Matrix_new(&S, GrB_BOOL, M_nrows, M_ncols);

        // Mask Generation
        GrB_Info info;
        for (uint64_t i = 0; i < op->record_count; i++) {
            Record r = op->records[i];
            uint64_t v_idx = 0;
            for (uint64_t j = 0; j < Record_length(r); j++) {
                if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
                Node *n = Record_GetNode(r, j);
                NodeID id = ENTITY_GET_ID(n);

                info = GrB_Matrix_setElement_BOOL(M, true, i, id);
                assert(info == GrB_SUCCESS);

                // Check the plan to generate S
                if (QPLAN[QPLAN_ID][op->destNodeIdx][v_idx]) {
                    info = GrB_Matrix_setElement_BOOL(S, true, i, id);
                    assert(info == GrB_SUCCESS);
                }

                v_idx++;
            }
        }

        GrB_Index adjacency_matrix_nrows, adjacency_matrix_ncols;
        A = op->graph->adjacency_matrix->matrix;
        GrB_Matrix_nrows(&adjacency_matrix_nrows,
                         op->graph->adjacency_matrix->matrix);
        GrB_Matrix_ncols(&adjacency_matrix_ncols,
                         op->graph->adjacency_matrix->matrix);

        GrB_Matrix_resize(M, op->record_count, adjacency_matrix_ncols);
        GrB_Matrix_resize(S, op->record_count, adjacency_matrix_ncols);
        GrB_Matrix_resize(A, adjacency_matrix_ncols, adjacency_matrix_ncols);
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    uint64_t num_threads = op->M_list_cap;
    GrB_Matrix *output_list = NULL;
    printf("Enumeration: ");
    simple_tic(tic);
    {
        output_list = (GrB_Matrix *)malloc(sizeof(GrB_Matrix) * num_threads);
        if (output_list == NULL) {
            return;
        }

        _gb_mxm_like_partition(&output_list, &M, &S, &A);
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    GrB_Matrix_free(&M);
    GrB_Matrix_free(&S);

    op->M_list = (RG_Matrix *)malloc(sizeof(RG_Matrix) * num_threads);
    if (op->M_list == NULL) {
        return;
    }

    op->IM = (uint *)malloc(sizeof(uint) * (num_threads + 1));
    if (op->M_list == NULL) {
        return;
    }
    op->IM[0] = 0;

    // M_list -> output_list
    for (size_t i = 0; i < num_threads; i++) {
        GrB_Index nrows, ncols;
        GrB_Matrix_nrows(&nrows, output_list[i]);
        GrB_Matrix_ncols(&ncols, output_list[i]);
        RG_Matrix_new(&(op->M_list[i]), GrB_BOOL, nrows, ncols);
        op->M_list[i]->matrix = output_list[i];
        op->IM[i + 1] = nrows + op->IM[i];
    }

    // Free output list
    if (output_list != NULL) {
        free(output_list);
    }
}

OpBase *NewCondTraverseOp(const ExecutionPlan *plan, Graph *g,
                          AlgebraicExpression *ae) {
    OpCondTraverse *op = rm_calloc(sizeof(OpCondTraverse), 1);

    op->ae = ae;
    op->graph = g;
    op->record_cap = BATCH_SIZE;

    // Set our Op operations
    OpBase_Init((OpBase *)op, OPType_CONDITIONAL_TRAVERSE,
                "Conditional Traverse", CondTraverseInit, CondTraverseConsume,
                CondTraverseReset, CondTraverseToString, CondTraverseClone,
                CondTraverseFree, false, plan);

    bool aware = OpBase_Aware((OpBase *)op, AlgebraicExpression_Src(ae),
                              &op->srcNodeIdx);
    UNUSED(aware);
    ASSERT(aware == true);

    const char *dest = AlgebraicExpression_Dest(ae);
    op->destNodeIdx = OpBase_Modifies((OpBase *)op, dest);

    const char *edge = AlgebraicExpression_Edge(ae);
    if (edge) {
        // this operation will populate an edge in the Record
        // prepare all necessary information for collecting matching edges
        uint edge_idx = OpBase_Modifies((OpBase *)op, edge);
        QGEdge *e = QueryGraph_GetEdgeByAlias(plan->query_graph, edge);
        op->edge_ctx = EdgeTraverseCtx_New(ae, e, edge_idx);
    }

    size_t num_threads = omp_get_max_threads();
    op->M_list_cap = num_threads;
    op->M_list_cur = 0;
    op->M_list = NULL;
    op->IM = NULL;

    return (OpBase *)op;
}

static OpResult CondTraverseInit(OpBase *opBase) {
    OpCondTraverse *op = (OpCondTraverse *)opBase;
    // Create 'records' with this Init function as 'record_cap'
    // might be set during optimization time (applyLimit)
    // If cap greater than BATCH_SIZE is specified,
    // use BATCH_SIZE as the value.
    if (op->record_cap > BATCH_SIZE) op->record_cap = BATCH_SIZE;
    op->records = rm_calloc(op->record_cap, sizeof(Record));

    return OP_OK;
}

/* Each call to CondTraverseConsume emits a Record containing the
 * traversal's endpoints and, if required, an edge.
 * Returns NULL once all traversals have been performed. */
static Record CondTraverseConsume(OpBase *opBase) {
    OpCondTraverse *op = (OpCondTraverse *)opBase;
    OpBase *child = op->op.children[0];

    /* If we're required to update an edge and have one queued, we can return
     * early. Otherwise, try to get a new pair of source and destination nodes.
     */
    if (op->r != NULL && op->edge_ctx != NULL &&
        EdgeTraverseCtx_SetEdge(op->edge_ctx, op->r)) {
        return OpBase_CloneRecord(op->r);
    }

    NodeID src_id = INVALID_ENTITY_ID;
    NodeID dest_id = INVALID_ENTITY_ID;

    while (true) {
        GrB_Info info =
            RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id, &dest_id, NULL);

        // Managed to get a tuple, break.
        if (info == GrB_SUCCESS) {
            src_id += op->IM[op->M_list_cur - 1];
            break;
        } else if (op->M_list != NULL) {
            if (op->M_list_cur < op->M_list_cap) {
                RG_MatrixTupleIter_attach(&op->iter,
                                          op->M_list[op->M_list_cur]);
                op->M_list_cur++;
                info = RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id,
                                                      &dest_id, NULL);
            }
        }
        // Managed to get a tuple with the new iterator, break.
        if (info == GrB_SUCCESS) {
            src_id += op->IM[op->M_list_cur - 1];
            break;
        }

        /* Run out of tuples, try to get new data.
         * Free old records. */
        op->r = NULL;
        for (uint i = 0; i < op->record_count; i++) {
            OpBase_DeleteRecord(op->records[i]);
        }

        // Ask child operations for data.
        for (op->record_count = 0; op->record_count < op->record_cap;
             op->record_count++) {
            Record childRecord = OpBase_Consume(child);
            // If the Record is NULL, the child has been depleted.
            if (!childRecord) break;
            if (!Record_GetNode(childRecord, op->srcNodeIdx)) {
                /* The child Record may not contain the source node in scenarios
                 * like a failed OPTIONAL MATCH. In this case, delete the Record
                 * and try again. */
                OpBase_DeleteRecord(childRecord);
                op->record_count--;
                continue;
            }

            // Store received record.
            Record_PersistScalars(childRecord);
            op->records[op->record_count] = childRecord;
        }

        // No data.
        if (op->record_count == 0) return NULL;

        _traverse(op);
    }

    /* Get node from current column. */
    op->r = op->records[src_id];
    // Populate the destination node and add it to the Record.
    Node destNode = GE_NEW_NODE();
    Graph_GetNode(op->graph, dest_id, &destNode);
    Record_AddNode(op->r, op->destNodeIdx, destNode);

    if (op->edge_ctx) {
        Node *srcNode = Record_GetNode(op->r, op->srcNodeIdx);
        // Collect all appropriate edges connecting the current pair of
        // endpoints.
        EdgeTraverseCtx_CollectEdges(op->edge_ctx, ENTITY_GET_ID(srcNode),
                                     ENTITY_GET_ID(&destNode));
        // We're guaranteed to have at least one edge.
        EdgeTraverseCtx_SetEdge(op->edge_ctx, op->r);
    }

    return OpBase_CloneRecord(op->r);
}

static OpResult CondTraverseReset(OpBase *ctx) {
    OpCondTraverse *op = (OpCondTraverse *)ctx;

    // Do not explicitly free op->r, as the same pointer is also held
    // in the op->records array and as such will be freed there.
    op->r = NULL;
    for (uint i = 0; i < op->record_count; i++)
        OpBase_DeleteRecord(op->records[i]);
    op->record_count = 0;

    if (op->edge_ctx) EdgeTraverseCtx_Reset(op->edge_ctx);

    GrB_Info info = RG_MatrixTupleIter_detach(&op->iter);
    ASSERT(info == GrB_SUCCESS);

    if (op->F != NULL) RG_Matrix_clear(op->F);
    return OP_OK;
}

static inline OpBase *CondTraverseClone(const ExecutionPlan *plan,
                                        const OpBase *opBase) {
    ASSERT(opBase->type == OPType_CONDITIONAL_TRAVERSE);
    OpCondTraverse *op = (OpCondTraverse *)opBase;
    return NewCondTraverseOp(plan, QueryCtx_GetGraph(),
                             AlgebraicExpression_Clone(op->ae));
}

/* Frees CondTraverse */
static void CondTraverseFree(OpBase *ctx) {
    OpCondTraverse *op = (OpCondTraverse *)ctx;

    GrB_Info info = RG_MatrixTupleIter_detach(&op->iter);
    ASSERT(info == GrB_SUCCESS);

    if (op->F != NULL) {
        RG_Matrix_free(&op->F);
        op->F = NULL;
    }

    if (op->M != NULL) {
        RG_Matrix_free(&op->M);
        op->M = NULL;
    }

    if (op->ae) {
        AlgebraicExpression_Free(op->ae);
        op->ae = NULL;
    }

    if (op->edge_ctx) {
        EdgeTraverseCtx_Free(op->edge_ctx);
        op->edge_ctx = NULL;
    }

    if (op->records) {
        for (uint i = 0; i < op->record_count; i++) {
            OpBase_DeleteRecord(op->records[i]);
        }
        rm_free(op->records);
        op->records = NULL;
    }

    if (op->M_list) {
        for (uint i = 0; i < op->M_list_cap; i++) {
            RG_Matrix_free(&(op->M_list[i]));
        }
        free(op->M_list);
    }

    if (op->IM) {
        free(op->IM);
    }
}
