/*start_query_plan*/
#define QPLAN_ID 0
/*end_query_plan*/

#include "query_plan.h"

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
void _traverse(OpCondTraverse *op) {
    GrB_Info info;

    // 0. Initialize
    size_t required_dim = Graph_RequiredMatrixDim(op->graph);
    RG_Matrix_new(&op->M, GrB_BOOL, op->record_cap, required_dim);

    double result = 0.0;
    double tic[2];

    printf("Initialized (dest=%lu)\n", op->destNodeIdx);

    // 1. Get previous result matrix
    RG_Matrix R;
    R = *(op->prev_gbR);
    RG_Matrix_wait(R, true);

    GrB_Index R_nrows, R_ncols;
    GrB_Matrix_nrows(&R_nrows, R->matrix);
    GrB_Matrix_ncols(&R_ncols, R->matrix);

    printf("Got previous result matrix\n");

    // 2. Extract selection matrices
    // Count required selection matrices
    size_t nS = 0;
    for (size_t i = 0;; i++) {
        if (PARTICIPATING_VERTICES[QPLAN_ID][op->destNodeIdx][i] == -1) {
            break;
        }
        nS++;
    }

    // Allocate selection matrices
    RG_Matrix S[nS];
    for (size_t i = 0; i < nS; i++) {
        S[i] = NULL;
        RG_Matrix_new(&(S[i]), GrB_BOOL, R_nrows, R_ncols);
    }

    // Initialize selection matrices
    RG_MatrixTupleIter_attach(&op->iter, R);

    NodeID src_id = INVALID_ENTITY_ID, dest_id = INVALID_ENTITY_ID;
    uint64_t value = 0;

    while (true) {
        GrB_Info info = RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id,
                                                       &dest_id, &value);
        if (info != GrB_SUCCESS) {
            break;
        }
        for (size_t i = 0; i < nS; i++) {
            if (value == PARTICIPATING_VERTICES[QPLAN_ID][op->destNodeIdx][i]) {
                info = GrB_Matrix_setElement_BOOL(S[i]->matrix, true, src_id,
                                                  dest_id);
                assert(info == GrB_SUCCESS);
            }
        }
    }
    for (size_t i = 0; i < nS; i++) {
        RG_Matrix_wait(S[i], true);
        GxB_Matrix_fprint(S[i]->matrix, "S[i] (Before)", GxB_SHORT, stdout);
    }

    printf("Extracted selection matrices (nS=%lu)\n", nS);

    // 3. Get relation matrices
    RG_Matrix A[nS];
    for (size_t i = 0; i < nS; i++) {
        A[i] = NULL;
        RG_Matrix_new(&(A[i]), GrB_BOOL, R_ncols, R_ncols);
        switch (PARTICIPATING_DIRECTION[QPLAN_ID][op->destNodeIdx][i]) {
            case DIR_FB: {
                RG_Matrix tmp, tmp_T;
                tmp = Graph_GetRelationMatrix(
                    op->graph,
                    PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i],
                    false);
                tmp_T = Graph_GetRelationMatrix(
                    op->graph,
                    PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i],
                    true);
                RG_Matrix_wait(tmp, true);
                RG_Matrix_wait(tmp_T, true);
                RG_Matrix_resize(tmp, R_ncols, R_ncols);
                RG_Matrix_resize(tmp_T, R_ncols, R_ncols);
                printf(
                    "%d\n",
                    PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i]);
                GxB_Matrix_fprint(tmp->matrix, "tmp", GxB_SHORT, stdout);
                GxB_Matrix_fprint(tmp_T->matrix, "tmp_T", GxB_SHORT, stdout);
                info = GrB_Matrix_eWiseAdd_BinaryOp(
                    A[i]->matrix, GrB_NULL, GrB_NULL, GrB_LOR, tmp->matrix,
                    tmp_T->matrix, GrB_NULL);
                assert(info == GrB_SUCCESS);
                break;
            }
            case DIR_F: {
                A[i] = Graph_GetRelationMatrix(
                    op->graph,
                    PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i],
                    false);
                break;
            }
            case DIR_B: {
                printf("A[%d] = %d\n", i, PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i]);
                A[i] = Graph_GetRelationMatrix(
                    op->graph,
                    PARTICIPATING_RELATIONSHIPS[QPLAN_ID][op->destNodeIdx][i],
                    true);
                break;
            }
        }
        RG_Matrix_wait(A[i], true);
        RG_Matrix_resize(A[i], R_ncols, R_ncols);
        GxB_Matrix_fprint(A[i]->matrix, "A[i] (Before)", GxB_SHORT, stdout);
    }

    printf("Extracted relation matrices\n");

    // 4. Filter with participating vertices
    for (size_t i = 0; i < nS; i++) {
        info = RG_mxm(S[i], GrB_LOR_LAND_SEMIRING_BOOL, S[i], A[i]);
        assert(info == GrB_SUCCESS);
        RG_Matrix_free(&(A[i]));
        GxB_Matrix_fprint(S[i]->matrix, "S[i] (After)", GxB_SHORT, stdout);
    }

    printf("Filtered participating vertices\n");

    // 5. Intersect all participating matrices
    op->M = S[0];
    for (size_t i = 1; i < nS; i++) {
        RG_Matrix_wait(S[i], true);
        info = GrB_Matrix_eWiseMult_BinaryOp(op->M->matrix, GrB_NULL, GrB_NULL,
                                             GrB_LAND, op->M->matrix,
                                             S[i]->matrix, GrB_NULL);
        assert(info == GrB_SUCCESS);
        RG_Matrix_free(&(S[i]));
    }

    printf("Intersected participating matrices\n");

    // 6. Get target matrix
    RG_Matrix L;
    L = Graph_GetLabelMatrix(op->graph,
                             TARGET_LABEL[QPLAN_ID][op->destNodeIdx]);
    RG_Matrix_wait(L, true);
    RG_Matrix_resize(L, R_ncols, R_ncols);

    printf("Got target matrix\n");

    // 7. Filter with target label
    info = RG_mxm(op->M, GrB_LOR_LAND_SEMIRING_BOOL, op->M, L);
    assert(info == GrB_SUCCESS);

    printf("Filtered with target label\n");

    RG_Matrix_wait(op->M, true);
    RG_MatrixTupleIter_attach(&op->iter, op->M);
    RG_MatrixTupleIter_attach(&op->iter_R, *(op->prev_gbR));

    op->IC_list = (size_t **)1;
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
    op->dest_v = QueryGraph_GetNodeByAlias(plan->query_graph, dest);

    const char *edge = AlgebraicExpression_Edge(ae);
    if (edge) {
        // this operation will populate an edge in the Record
        // prepare all necessary information for collecting matching edges
        uint edge_idx = OpBase_Modifies((OpBase *)op, edge);
        op->dest_e = QueryGraph_GetEdgeByAlias(plan->query_graph, edge);
        op->edge_ctx = EdgeTraverseCtx_New(ae, op->dest_e, edge_idx);
    }

    size_t num_threads = omp_get_max_threads();
    op->M_list_cap = num_threads;
    op->M_list_cur = 0;
    op->M_list = NULL;
    op->IM = NULL;

    op->IC_list = NULL;
    op->JC_list = NULL;
    op->IC_size_list = NULL;
    op->JC_size_list = NULL;
    op->iter_i = 0;
    op->iter_j = 0;

    op->prev_gbR = NULL;
    op->prev_ID = -1;

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

    size_t num_threads = op->M_list_cap;

    /* If we're required to update an edge and have one queued, we can return
     * early. Otherwise, try to get a new pair of source and destination nodes.
     */
    if (op->r != NULL && op->edge_ctx != NULL &&
        EdgeTraverseCtx_SetEdge(op->edge_ctx, op->r)) {
        return OpBase_CloneRecord(op->r);
    }

    NodeID src_id = INVALID_ENTITY_ID;
    NodeID dest_id = INVALID_ENTITY_ID;

    // TRAVERSAL PHASE
    // If the operator didn't apply traverse()
    // Grab inputs and traverse()
    if (op->IC_list == NULL) {
        // Free old records
        op->r = NULL;
        for (uint i = 0; i < op->record_count; i++) {
            OpBase_DeleteRecord(op->records[i]);
        }

        // If its child is ConditionalTraverse,
        // Take CSR from the child
        if (child->type == OPType_CONDITIONAL_TRAVERSE) {
            op->prev_gbR = (RG_Matrix *)OpBase_Consume(child);
            if (op->prev_gbR == NULL) return NULL;
        }

        // If not, create CSR from list of records
        else {
            // Consume child's records
            for (op->record_count = 0; op->record_count < op->record_cap;
                 op->record_count++) {
                Record childRecord = OpBase_Consume(child);
                // If the Record is NULL, the child has been depleted.
                if (!childRecord) break;
                if (!Record_GetNode(childRecord, op->srcNodeIdx)) {
                    /* The child Record may not contain the source node in
                     * scenarios like a failed OPTIONAL MATCH. In this case,
                     * delete the Record and try again. */
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

            uint64_t nrows = op->record_count;
            uint64_t ncols = op->record_count;

            // Initialize prev_R as CSR
            op->prev_gbR = (RG_Matrix *)malloc(sizeof(RG_Matrix));
            if (op->prev_gbR == NULL) {
                return NULL;
            }
            RG_Matrix_new(op->prev_gbR, GrB_UINT64, nrows,
                          op->graph->nodes->itemCount);

            // Mask Generation
            GrB_Info info;
            for (uint64_t i = 0; i < op->record_count; i++) {
                Record r = op->records[i];
                for (uint64_t j = 0; j < Record_length(r); j++) {
                    if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
                    Node *n = Record_GetNode(r, j);
                    NodeID id = ENTITY_GET_ID(n);
                    info = GrB_Matrix_setElement_UINT64(
                        (*(op->prev_gbR))->matrix, op->srcNodeIdx, i, id);
                    assert(info == GrB_SUCCESS);
                }
            }
        }

        // Traverse
        _traverse(op);

        size_t prv_gbR_nvals, prv_gbR_nrows;
        RG_Matrix_nrows(&prv_gbR_nrows, *(op->prev_gbR));
        RG_Matrix_nvals(&prv_gbR_nvals, *(op->prev_gbR));

        op->num_prev_vertices = prv_gbR_nvals / prv_gbR_nrows;
    }

    // RESULT EMISSION PHASE
    // If your parent is not ConditionalTraverse,
    // Return as a list of records
    if (op->op.parent->type != OPType_CONDITIONAL_TRAVERSE) {
        assert(op->prev_gbR != NULL);

        NodeID src_id = INVALID_ENTITY_ID;
        NodeID dest_id = INVALID_ENTITY_ID;

        // Grab one tuple from C
        // Candidate
        GrB_Info info =
            RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id, &dest_id, NULL);
        if (info != GrB_SUCCESS) {
            RG_Matrix_free(op->prev_gbR);
            return NULL;
        }

        assert(src_id != INVALID_ENTITY_ID);
        assert(dest_id != INVALID_ENTITY_ID);

        // Check whether we need to grab a new record
        if (op->prev_ID != src_id) {
            op->r = NULL;
            op->prev_ID = src_id;
        }

        // If we cannot reuse the previous record
        if (op->r == NULL) {
            size_t num_vertices = op->destNodeIdx;

            NodeID srcR_id = INVALID_ENTITY_ID;
            NodeID destR_id = INVALID_ENTITY_ID;
            uint64_t valueR = 0;

            NodeID node_map[100];

            op->r = OpBase_CreateRecord((OpBase *)op);
            for (size_t i = 0; i < op->num_prev_vertices; i++) {
                // Only destination is needed
                // Grab a record from M
                info = RG_MatrixTupleIter_next_UINT64(&op->iter_R, &srcR_id,
                                                      &destR_id, &valueR);
                if (info != GrB_SUCCESS) {
                    break;
                }

                // If we don't have any candidates for this row
                // Skip until we are in the same row
                while (srcR_id < src_id) {
                    info = RG_MatrixTupleIter_next_UINT64(&op->iter_R, &srcR_id,
                                                          &destR_id, &valueR);
                    if (info != GrB_SUCCESS) {
                        break;
                    }
                }
                // Make sure both M and C are at the same row
                assert(srcR_id == src_id);
                assert(destR_id != INVALID_ENTITY_ID);

                node_map[valueR] = destR_id;
                // printf("\t|- Map - node_map[%d] = %d\n", valueR, destR_id);

                // Create a record
                Node node = GE_NEW_NODE();
                Graph_GetNode(op->graph, destR_id, &node);
                Record_AddNode(op->r, valueR, node);
            }

            // dfs the plan
            OpBase *cur = child;

            while (cur->type == OPType_CONDITIONAL_TRAVERSE) {
                OpCondTraverse *cur_traverse = (OpCondTraverse *)cur;
                EdgeTraverseCtx_CollectEdges(
                    cur_traverse->edge_ctx, node_map[cur_traverse->srcNodeIdx],
                    node_map[cur_traverse->destNodeIdx]);
                EdgeTraverseCtx_SetEdge(cur_traverse->edge_ctx, op->r);
                cur = cur->children[0];
            }
        }

        // If we broke, return NULL
        if (info != GrB_SUCCESS) {
            RG_Matrix_free(op->prev_gbR);
            return NULL;
        }

        // Populate the destination node and add it to the Record.
        Node node = GE_NEW_NODE();
        Graph_GetNode(op->graph, dest_id, &node);
        Record_AddNode(op->r, op->destNodeIdx, node);

        if (op->edge_ctx) {
            Node *srcNode = Record_GetNode(op->r, op->srcNodeIdx);
            // Collect all appropriate edges connecting the current pair of
            // endpoints.
            EdgeTraverseCtx_CollectEdges(op->edge_ctx, ENTITY_GET_ID(srcNode),
                                         dest_id);
            // We're guaranteed to have at least one edge.
            EdgeTraverseCtx_SetEdge(op->edge_ctx, op->r);
        }

        // size_t record_str_cap = 0;
        // char *record_str = NULL;
        // size_t record_str_len =
        //     Record_ToString(op->r, &record_str, &record_str_cap);
        // printf("    |- [12] Record %s\n", record_str);

        // Send the clone
        return OpBase_CloneRecord(op->r);
    }

    // If your parent is ConditionalTraverse,
    // Materializing them and pass them through CSR!
    else {
        assert(op->prev_gbR != NULL);

        // Allocate materialized matrix as output_matrix
        RG_Matrix *output_matrix = (RG_Matrix *)malloc(sizeof(RG_Matrix));
        if (output_matrix == NULL) {
            return NULL;
        }

        // New output_matrix dimension
        GrB_Index C_nvals, C_ncols;
        GrB_Matrix_nvals(&C_nvals, op->M->matrix);
        GrB_Matrix_ncols(&C_ncols, op->M->matrix);
        RG_Matrix_new(output_matrix, GrB_UINT64, C_nvals, C_ncols);

        // Use output_nrow to determine current row
        uint64_t output_nrow = 0;

        // Control rows by using old_iter_R
        RG_MatrixTupleIter old_iter_R = op->iter_R;

        NodeID src_id = INVALID_ENTITY_ID;
        NodeID dest_id = INVALID_ENTITY_ID;

        while (true) {
            // Grab one candidate
            GrB_Info info = RG_MatrixTupleIter_next_UINT64(&op->iter, &src_id,
                                                           &dest_id, NULL);
            if (info != GrB_SUCCESS) {
                break;
            }

            // If we are in the new row, change the iterator
            if (op->prev_ID != src_id) {
                old_iter_R = op->iter_R;
                op->prev_ID = src_id;
            }

            // Make sure we are using old_iter_R
            op->iter_R = old_iter_R;

            NodeID srcR_id = INVALID_ENTITY_ID;
            NodeID destR_id = INVALID_ENTITY_ID;
            uint64_t valueR = 0;

            op->r = OpBase_CreateRecord((OpBase *)op);

            // Grab a row from M
            size_t num_vertices = op->destNodeIdx;
            for (size_t i = 0; i < op->num_prev_vertices; i++) {
                info = RG_MatrixTupleIter_next_UINT64(&op->iter_R, &srcR_id,
                                                      &destR_id, &valueR);
                if (info != GrB_SUCCESS) {
                    break;
                }

                // printf("%d %d\n", srcR_id, src_id);

                // If this row does not contain any candidate,
                // Skip to the earliest row that contains candidates
                while (srcR_id < src_id) {
                    info = RG_MatrixTupleIter_next_UINT64(&op->iter_R, &srcR_id,
                                                          &destR_id, &valueR);
                    if (info != GrB_SUCCESS) {
                        break;
                    }
                    // printf("%d %d\n", srcR_id, src_id);
                }

                // if (srcR_id > src_id) {
                //     info = GxB_EXHAUSTED;
                //     break;
                // }

                assert(srcR_id == src_id);
                assert(destR_id != INVALID_ENTITY_ID);

                // Set the materialized matrix
                GrB_Matrix_setElement_UINT64((*output_matrix)->matrix, valueR,
                                             output_nrow, destR_id);
            }

            // If we broke, break the loop
            if (info != GrB_SUCCESS) {
                break;
            }

            // Set the candidate into the materialized matrix
            GrB_Matrix_setElement_UINT64((*output_matrix)->matrix,
                                         op->destNodeIdx, output_nrow, dest_id);

            // Move to the next row in the materialized matrix
            output_nrow++;
        }

        RG_Matrix_free(op->prev_gbR);

        // Return the materialized matrix as a mask for the next iteration
        return (Record)(output_matrix);
    }

    return NULL;
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
