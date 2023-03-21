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

#include "op_conditional_traverse.h"

#include <omp.h>
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
void _traverse(OpCondTraverse *op) {
    // printf("Traverse\n");

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

    // For outputs
    size_t num_threads = op->M_list_cap;
    size_t **IC_list = NULL, *IC_size_list = NULL;
    size_t **JC_list = NULL, *JC_size_list = NULL;
    size_t *IB_arr = NULL, *JB_arr = NULL, IB_size = 0, JB_size = 0;
    printf("Preparation: ");
    simple_tic(tic);
    {
        IC_list = (size_t **)malloc(sizeof(size_t *) * num_threads);
        if (IC_list == NULL) {
            return;
        }
        IC_size_list = (size_t *)malloc(sizeof(size_t) * num_threads);
        if (IC_size_list == NULL) {
            return;
        }
        JC_list = (size_t **)malloc(sizeof(size_t *) * num_threads);
        if (JC_list == NULL) {
            return;
        }
        JC_size_list = (size_t *)malloc(sizeof(size_t) * num_threads);
        if (JC_size_list == NULL) {
            return;
        }

        size_t num_vertices = op->prev_R->J_size / (op->prev_R->I_size - 1);
        size_t plan_nnz = 0;
        for (size_t i = 0; i < num_vertices; i++) {
            // For clique
            if (QPLAN[QPLAN_ID][op->destNodeIdx][i]) plan_nnz++;
        }

        // Build B from records and plan
        IB_size = op->prev_R->I_size;
        JB_size = (op->prev_R->I_size - 1) * plan_nnz;
        IB_arr = (size_t *)malloc(sizeof(size_t) * IB_size);
        if (IB_arr == NULL) {
            return;
        }
        JB_arr = (size_t *)malloc(sizeof(size_t) * JB_size);
        if (JB_arr == NULL) {
            return;
        }

#pragma omp parallel for num_threads(num_threads)
        for (size_t i = 0; i < op->prev_R->I_size - 1; i++) {
            IB_arr[i] = i * plan_nnz;
            size_t j_idx = 0;
            for (size_t j = 0; j < num_vertices; j++) {
                if (QPLAN[QPLAN_ID][op->destNodeIdx][j]) {
                    JB_arr[i * plan_nnz + j_idx] =
                        op->prev_R->J[i * num_vertices + j];
                    j_idx++;
                }
            }
        }
        IB_arr[op->prev_R->I_size - 1] = (op->prev_R->I_size - 1) * plan_nnz;
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    printf("Enumeration: ");
    simple_tic(tic);
    {
        mxm_like_partition_no_conv(
            &IC_list, &IC_size_list, &JC_list, &JC_size_list, op->prev_R->I,
            op->prev_R->I_size, op->prev_R->J, op->prev_R->J_size, IB_arr,
            IB_size, JB_arr, JB_size, &(op->graph->adjacency_matrix->matrix));
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    free(IB_arr);
    free(JB_arr);

    // Transfer to the Consume method
    op->IC_list = IC_list;
    op->IC_size_list = IC_size_list;
    op->JC_list = JC_list;
    op->JC_size_list = JC_size_list;

    op->IM = (uint *)malloc(sizeof(uint) * (num_threads + 1));
    if (op->IM == NULL) {
        return;
    }

    op->IM[0] = 0;
    for (size_t i = 0; i < num_threads; i++) {
        op->IM[i + 1] = IC_size_list[i] - 1 + op->IM[i];
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

    op->IC_list = NULL;
    op->JC_list = NULL;
    op->IC_size_list = NULL;
    op->JC_size_list = NULL;
    op->iter_i = 0;
    op->iter_j = 0;

    op->prev_R = NULL;

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
            // printf("Take R from child\n");
            op->prev_R = (CSRRecord *)OpBase_Consume(child);
            if (op->prev_R == NULL) return NULL;
        }

        // If not, create CSR from list of records
        else {
            // printf("Generate R from child\n");

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

            // printf("Start generating\n");

            uint64_t current_record_size = 0;
            for (uint j = 0; j < Record_length(op->records[0]); j++) {
                if (Record_GetType(op->records[0], j) == REC_TYPE_NODE)
                    current_record_size++;
            }
            // printf("current_record_size (%lu)\n", current_record_size);

            op->prev_R = (CSRRecord *)malloc(sizeof(CSRRecord));
            if (op->prev_R == NULL) {
                return NULL;
            }

            // Create R (which will be used as a mask)
            op->prev_R->I_size = op->record_count + 1;
            op->prev_R->J_size = op->record_count * current_record_size;
            op->prev_R->I =
                (size_t *)malloc(sizeof(size_t) * op->prev_R->I_size);
            if (op->prev_R->I == NULL) {
                return NULL;
            }
            op->prev_R->J =
                (size_t *)malloc(sizeof(size_t) * op->prev_R->J_size);
            if (op->prev_R->J == NULL) {
                return NULL;
            }
            op->prev_R->I[0] = 0;

#pragma omp parallel for num_threads(num_threads)
            for (size_t i = 1; i < op->prev_R->I_size; i++) {
                op->prev_R->I[i] = i * current_record_size;
            }

#pragma omp parallel for num_threads(num_threads)
            for (size_t i = 0; i < op->record_count; i++) {
                Record r = op->records[i];
                uint r_len = 0;
                for (uint j = 0; j < Record_length(r); j++) {
                    if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
                    Node *n = Record_GetNode(r, j);
                    NodeID id = ENTITY_GET_ID(n);
                    op->prev_R->J[(i * current_record_size) + r_len++] = id;
                }
                assert(r_len == current_record_size);
                cpp_sort(op->prev_R->J + op->prev_R->I[i],
                         op->prev_R->J + op->prev_R->I[i + 1]);
            }
        }

        // Traverse
        _traverse(op);

        assert(op->IC_list != NULL);
        assert(op->JC_list != NULL);
        assert(op->IC_size_list != NULL);
        assert(op->JC_size_list != NULL);
    }

    // RESULT EMISSION PHASE
    // If your parent is not ConditionalTraverse,
    // Return as a list of records
    if (op->op.parent->type != OPType_CONDITIONAL_TRAVERSE) {
        assert(op->prev_R != NULL);

        while (true) {
            // Loop m
            // If M_list is not out of bound
            if (op->M_list_cur < op->M_list_cap) {
                // Loop i
                // If M_list[m].IC is not out of bound
                if (op->iter_i < op->IC_size_list[op->M_list_cur] - 1) {
                    // Loop j
                    // If M_list[m].IC[i].JC is not out of bound
                    if (op->iter_j <
                        op->IC_list[op->M_list_cur][op->iter_i + 1]) {
                        // Source ID = cur_i + M_offset_i
                        src_id = op->iter_i + op->IM[op->M_list_cur];
                        // Destination ID = cur_j
                        dest_id = op->JC_list[op->M_list_cur][op->iter_j];

                        // Advance j
                        op->iter_j++;

                        // printf("%lu (%lu + %lu) %lu\n", src_id, op->iter_i,
                        // op->IM[op->M_list_cur], dest_id);
                        // assert(src_id != INVALID_ENTITY_ID);
                        // assert(dest_id != INVALID_ENTITY_ID);

                        // Break the loop
                        break;
                    }
                    // If M_list[m].IC[i].JC is out of bound
                    else {
                        // Advance i
                        op->iter_i++;
                        if (op->r != NULL) OpBase_DeleteRecord(op->r);
                        op->r = NULL;
                        // No need to set j = 0 (CSR)
                        // op->iter_j = 0;
                    }
                }
                // If M_list[m].IC is out of bound
                else {
                    // Free unused IC and JC
                    free(op->IC_list[op->M_list_cur]);
                    free(op->JC_list[op->M_list_cur]);
                    // Advance m
                    op->M_list_cur++;
                    // Set i = j = 0
                    op->iter_i = op->iter_j = 0;
                    if (op->r != NULL) OpBase_DeleteRecord(op->r);
                    op->r = NULL;
                }
            }
            // If M_list is out of bound
            else {
                free(op->IC_list);
                free(op->IC_size_list);
                free(op->JC_list);
                free(op->JC_size_list);

                free(op->prev_R->I);
                free(op->prev_R->J);
                free(op->prev_R);

                return NULL;
            }
        }

        assert(src_id != INVALID_ENTITY_ID);
        assert(dest_id != INVALID_ENTITY_ID);

        if (op->r == NULL) {
            size_t num_vertices = op->prev_R->J_size / (op->prev_R->I_size - 1);

            op->r = OpBase_CreateRecord((OpBase *)op);
            for (size_t i = 0; i < num_vertices; i++) {
                Node node = GE_NEW_NODE();
                Graph_GetNode(op->graph,
                              op->prev_R->J[(src_id * num_vertices) + i],
                              &node);
                Record_AddNode(op->r, i, node);
            }
        }

        // Populate the destination node and add it to the Record.
        Node node = GE_NEW_NODE();
        Graph_GetNode(op->graph, dest_id, &node);
        Record_AddNode(op->r, op->destNodeIdx, node);

        return OpBase_CloneRecord(op->r);
    }

    // If your parent is ConditionalTraverse,
    // Materializing them and pass them through CSR!
    else {
        // printf("Result Emission (Materialize + Bypass)\n");
        assert(op->prev_R != NULL);

        CSRRecord *output_matrix = (CSRRecord *)malloc(sizeof(CSRRecord));
        if (output_matrix == NULL) {
            return NULL;
        }

        // Compute in_offset
        size_t *in_offset =
            (size_t *)malloc(sizeof(size_t) * (num_threads + 1));
        if (in_offset == NULL) {
            return NULL;
        }
        in_offset[0] = 0;
        for (size_t i = 0; i < num_threads; i++) {
            // in_offset[i] + nrows(C[i])
            size_t nrows_C_i = op->IC_size_list[i] - 1;
            in_offset[i + 1] = in_offset[i] + nrows_C_i;
        }

        // Compute out_offset
        size_t *out_offset =
            (size_t *)malloc(sizeof(size_t) * (num_threads + 1));
        if (out_offset == NULL) {
            return NULL;
        }
        out_offset[0] = 0;
        for (size_t i = 0; i < num_threads; i++) {
            // in_offset[i] + nvals(C[i])
            size_t nvals_C_i = op->JC_size_list[i];
            out_offset[i + 1] = out_offset[i] + nvals_C_i;
        }

        size_t num_vertices = op->prev_R->J_size / (op->prev_R->I_size - 1);

        // Initialize output_matrix
        output_matrix->I_size = out_offset[num_threads] + 1;
        output_matrix->J_size = out_offset[num_threads] * (num_vertices + 1);
        output_matrix->I =
            (size_t *)malloc(sizeof(size_t) * output_matrix->I_size);
        if (output_matrix->I == NULL) {
            return NULL;
        }
#pragma omp parallel for num_threads(num_threads)
        for (size_t i = 0; i < out_offset[num_threads] + 1; i++) {
            output_matrix->I[i] = i * (num_vertices + 1);
        }

        output_matrix->J =
            (size_t *)malloc(sizeof(size_t) * output_matrix->J_size);
        if (output_matrix->J == NULL) {
            return NULL;
        }

        // Do multiple times
#pragma omp parallel for num_threads(num_threads)
        for (size_t m = 0; m < op->M_list_cap; m++) {
            // Input: C and R (from in_offset[i] to in_offset[i+1])
            // Output: P (from out_offset[i] to out_offset[i+1])
            size_t *IC = op->IC_list[m];
            size_t IC_size = op->IC_size_list[m];
            size_t *JC = op->JC_list[m];
            size_t JC_size = op->JC_size_list[m];
            size_t *IR = op->prev_R->I + in_offset[m];
            size_t IR_size = in_offset[m + 1] - in_offset[m];
            size_t *JR = op->prev_R->J + (in_offset[m] * num_vertices);
            size_t JR_size = IR_size * num_vertices;
            assert((IC_size - 1) == IR_size);

            size_t *JP =
                output_matrix->J + (out_offset[m] * (num_vertices + 1));
            // Loop each row in R
            for (size_t i = 0; i < IR_size; i++) {
                size_t *JR_st = JR + (i * num_vertices);
                size_t *JR_en = JR + ((i + 1) * num_vertices);

                // Union the current row in R with all C
                // Loop each element in row C
                size_t *JC_pt = JC + IC[i];
                size_t *JC_en = JC + IC[i + 1];
                for (; JC_pt != JC_en; JC_pt++) {
                    // Loop each element in JR
                    // Assume JC_st to JC_en is in the sorted order
                    bool is_inserted = false;
                    for (size_t *JR_pt = JR_st; JR_pt != JR_en; JR_pt++) {
                        *JP = *JR_pt;
                        JP++;
                    }
                    *JP = *JC_pt;
                    JP++;
                }
            }
        }

        free(op->IC_list);
        free(op->IC_size_list);
        free(op->JC_list);
        free(op->JC_size_list);

        free(op->prev_R->I);
        free(op->prev_R->J);
        free(op->prev_R);

        return (Record)output_matrix;
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
