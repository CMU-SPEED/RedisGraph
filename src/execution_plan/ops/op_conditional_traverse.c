/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include "op_conditional_traverse.h"

#include <time.h>

#include "../../query_ctx.h"
#include "../../subgraph_enumeration/subgraph_enumeration.hpp"
#include "../../util/simple_timer.h"
#include "RG.h"
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

    // #define ORIGINAL
    double result = 0.0;
    double tic[2];

// DO NOT CHANGE
/*start_mode_configuration*/
#define ORIGINAL
/*end_mode_configuration*/

#ifdef ORIGINAL
    // populate filter matrix
    _populate_filter_matrix(op);

    // evaluate expression
    AlgebraicExpression_Eval(op->ae, op->M);
#else
    // Mask creation
    // custom mxm
    simple_tic(tic);

    GrB_Matrix mask;
    GrB_Index mask_nrows, mask_ncols;
    GrB_Matrix_nrows(&mask_nrows, op->M->matrix);
    GrB_Matrix_ncols(&mask_ncols, op->M->matrix);
    GrB_Matrix_new(&mask, GrB_BOOL, mask_nrows, mask_ncols);

    // Generate the mask
    for (uint i = 0; i < op->record_count; i++) {
        Record r = op->records[i];
        for (uint j = 0; j < Record_length(r); j++) {
            if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
            Node *n = Record_GetNode(r, j);
            NodeID id = ENTITY_GET_ID(n);
            GrB_Info info = GrB_Matrix_setElement_BOOL(mask, true, i, id);
            assert(info == GrB_SUCCESS);
        }
    }

    result = simple_toc(tic);
    printf("Mask %f\n", result * 1e3);
#ifdef FUSED_FILTER_AND_TRAVERSE
    // populate filter matrix
    _populate_filter_matrix(op);

    GrB_Info info =
        GrB_mxm(op->M->matrix, mask, GrB_NULL, GrB_LOR_LAND_SEMIRING_BOOL,
                op->F->matrix, op->graph->adjacency_matrix->matrix, GrB_DESC_C);
    assert(info == GrB_SUCCESS);
#endif
#ifdef CN_ACCUMULATE_SELECT
    // TODO: Create filter matrix for non-chains
    // FIXME: This is only for cliques

    // TODO: Common Neighbor Traversal
    // TODO(1): Quick and Dirty GrB (MxM - PLUS_TIMES) and SELECT

    // TODO(1): Initialize t
    GrB_Matrix t;
    // Dimension(T) = Dimension(F)
    GrB_Index t_nrows, t_ncols;
    GrB_Matrix_nrows(&t_nrows, op->F->matrix);
    GrB_Matrix_ncols(&t_ncols, op->F->matrix);
    GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);

    // TODO(1): Get number of vertices per subgraph
    if (op->record_count != 0) {
        uint64_t num_vertices = 0;
        for (uint j = 0; j < Record_length(op->records[0]); j++) {
            if (Record_GetType(op->records[0], j) == REC_TYPE_NODE)
                num_vertices++;
        }
        // printf("num_vertices: %lu\n", num_vertices);

        // // Create F
        // GrB_Matrix F;
        // GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);
        // TODO(1): Introducing the set inclusion function
        // // FIXME
        // GrB_Matrix_select_UINT64(F, GrB_NULL, GrB_NULL, OP_IN, mask,
        // set[num_vertices - 1], GrB_NULL);

        // GxB_Matrix_fprint(mask, "S", GxB_SUMMARY, stdout);
        // GxB_Matrix_fprint(op->graph->adjacency_matrix->matrix, "A",
        // GxB_SUMMARY, stdout);

        simple_tic(tic);

        _gb_matrix_filter(&(op->M->matrix), &mask, &mask,
                          &(op->graph->adjacency_matrix->matrix), num_vertices);

        result = simple_toc(tic);
        printf("Enum %f\n", result * 1e3);

        // // TODO(1): Ensure that the output is integer (not boolean)
        // GrB_Info info = GrB_mxm(
        //     t, mask, GrB_NULL, GrB_PLUS_TIMES_SEMIRING_UINT64, mask,
        //     op->graph->adjacency_matrix->matrix, GrB_DESC_C);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(t, "TEMP", GxB_SUMMARY, stdout);

        // // TODO(1): Ensure t and num_vertices
        // info = GrB_Matrix_select_UINT64(op->M->matrix, GrB_NULL, GrB_NULL,
        //                                 GrB_VALUEEQ_UINT64, t, num_vertices,
        //                                 GrB_NULL);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(op->M->matrix, "OUT", GxB_SUMMARY, stdout);
    }
#endif
#ifdef CN_INTERSECTION
    // TODO: Create filter matrix for non-chains
    // FIXME: This is only for cliques

    // TODO: Common Neighbor Traversal
    // TODO(1): Quick and Dirty GrB (MxM - PLUS_TIMES) and SELECT

    // TODO(1): Initialize t
    GrB_Matrix t;
    // Dimension(T) = Dimension(F)
    GrB_Index t_nrows, t_ncols;
    GrB_Matrix_nrows(&t_nrows, op->F->matrix);
    GrB_Matrix_ncols(&t_ncols, op->F->matrix);
    GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);

    // TODO(1): Get number of vertices per subgraph
    if (op->record_count != 0) {
        uint64_t num_vertices = 0;
        for (uint j = 0; j < Record_length(op->records[0]); j++) {
            if (Record_GetType(op->records[0], j) == REC_TYPE_NODE)
                num_vertices++;
        }
        // printf("num_vertices: %lu\n", num_vertices);

        // // Create F
        // GrB_Matrix F;
        // GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);
        // TODO(1): Introducing the set inclusion function
        // // FIXME
        // GrB_Matrix_select_UINT64(F, GrB_NULL, GrB_NULL, OP_IN, mask,
        // set[num_vertices - 1], GrB_NULL);

        // GxB_Matrix_fprint(mask, "S", GxB_SUMMARY, stdout);
        // GxB_Matrix_fprint(op->graph->adjacency_matrix->matrix, "A",
        // GxB_SUMMARY, stdout);

        simple_tic(tic);

        _gb_matrix_filter(&(op->M->matrix), &mask, &mask,
                          &(op->graph->adjacency_matrix->matrix), num_vertices);

        result = simple_toc(tic);
        printf("_gb_matrix_filter: %f ms\n", result * 1e3);

        // // TODO(1): Ensure that the output is integer (not boolean)
        // GrB_Info info = GrB_mxm(
        //     t, mask, GrB_NULL, GrB_PLUS_TIMES_SEMIRING_UINT64, mask,
        //     op->graph->adjacency_matrix->matrix, GrB_DESC_C);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(t, "TEMP", GxB_SUMMARY, stdout);

        // // TODO(1): Ensure t and num_vertices
        // info = GrB_Matrix_select_UINT64(op->M->matrix, GrB_NULL, GrB_NULL,
        //                                 GrB_VALUEEQ_UINT64, t, num_vertices,
        //                                 GrB_NULL);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(op->M->matrix, "OUT", GxB_SUMMARY, stdout);
    }
#endif
#ifdef CN_MXM_LIKE
    // TODO: Create filter matrix for non-chains
    // FIXME: This is only for cliques

    // TODO: Common Neighbor Traversal
    // TODO(1): Quick and Dirty GrB (MxM - PLUS_TIMES) and SELECT

    // // TODO(1): Initialize t
    // GrB_Matrix t;
    // // Dimension(T) = Dimension(F)
    // GrB_Index t_nrows, t_ncols;
    // // GrB_Matrix_nrows(&t_nrows, op->F->matrix);
    // t_nrows = op->record_count + 1;
    // GrB_Matrix_ncols(&t_ncols, op->F->matrix);
    // GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);

    // TODO(1): Get number of vertices per subgraph
    if (op->record_count != 0) {
        uint64_t num_vertices = 0;
        for (uint j = 0; j < Record_length(op->records[0]); j++) {
            if (Record_GetType(op->records[0], j) == REC_TYPE_NODE)
                num_vertices++;
        }
        // printf("num_vertices: %lu\n", num_vertices);

        // // Create F
        // GrB_Matrix F;
        // GrB_Matrix_new(&t, GrB_UINT64, t_nrows, t_ncols);
        // TODO(1): Introducing the set inclusion function
        // // FIXME
        // GrB_Matrix_select_UINT64(F, GrB_NULL, GrB_NULL, OP_IN, mask,
        // set[num_vertices - 1], GrB_NULL);

        // GxB_Matrix_fprint(mask, "S", GxB_SUMMARY, stdout);
        // GxB_Matrix_fprint(op->graph->adjacency_matrix->matrix, "A",
        // GxB_SUMMARY, stdout);

        // Resize 1
        simple_tic(tic);

        GrB_Index adjacency_matrix_nrows, adjacency_matrix_ncols;
        GrB_Matrix_nrows(&adjacency_matrix_nrows,
                         op->graph->adjacency_matrix->matrix);
        GrB_Matrix_ncols(&adjacency_matrix_ncols,
                         op->graph->adjacency_matrix->matrix);
        // adjacency_matrix_ncols = 5242;
        GrB_Matrix_resize(mask, op->record_count, adjacency_matrix_ncols);

        GrB_Matrix A;
        A = op->graph->adjacency_matrix->matrix;
        GrB_Matrix_resize(A, adjacency_matrix_ncols, adjacency_matrix_ncols);
        GrB_Matrix_wait(A, GrB_MATERIALIZE);

        result = simple_toc(tic);
        printf("Prep %f\n", result * 1e3);

        // // MxM
        // simple_tic(tic);

        _gb_mxm_like_partition_merge(&(op->M->matrix), &mask, &mask, &A);

        // result = simple_toc(tic);
        // printf("Enum %f\n", result * 1e3);

        // // Resize 2
        // simple_tic(tic);

        // // GrB_Index M_nrows;
        // // GrB_Matrix_nrows(&M_nrows, op->M->matrix);

        // // M_nrows =
        // //     adjacency_matrix_nrows > M_nrows ? adjacency_matrix_nrows : M_nrows;
        // // GrB_Matrix_resize(op->M->matrix, M_nrows,
        // //                   adjacency_matrix_ncols);

        // result = simple_toc(tic);
        // printf("resize (step 2): %f ms\n", result * 1e3);

        // // TODO(1): Ensure that the output is integer (not boolean)
        // GrB_Info info = GrB_mxm(
        //     t, mask, GrB_NULL, GrB_PLUS_TIMES_SEMIRING_UINT64, mask,
        //     op->graph->adjacency_matrix->matrix, GrB_DESC_C);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(t, "TEMP", GxB_SUMMARY, stdout);

        // // TODO(1): Ensure t and num_vertices
        // info = GrB_Matrix_select_UINT64(op->M->matrix, GrB_NULL, GrB_NULL,
        //                                 GrB_VALUEEQ_UINT64, t, num_vertices,
        //                                 GrB_NULL);
        // assert(info == GrB_SUCCESS);

        // GxB_Matrix_fprint(op->M->matrix, "OUT", GxB_SUMMARY, stdout);
    }
#endif
    GrB_Matrix_free(&mask);
#endif

    RG_MatrixTupleIter_attach(&op->iter, op->M);
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

    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);

    while (true) {
        GrB_Info info =
            RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id, &dest_id, NULL);

        // Managed to get a tuple, break.
        if (info == GrB_SUCCESS) break;

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

    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &stop);
    // result += (stop.tv_sec - start.tv_sec) * 1e6 + (stop.tv_nsec -
    // start.tv_nsec) / 1e3;

    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);

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

    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &stop);
    // result += (stop.tv_sec - start.tv_sec) * 1e6 +
    //           (stop.tv_nsec - start.tv_nsec) / 1e3;

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
}
