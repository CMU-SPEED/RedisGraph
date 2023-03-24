/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include "proc_subgraph_enumeration.h"

#include <omp.h>
#include <time.h>

#include "../datatypes/array.h"
#include "../graph/graphcontext.h"
#include "../query_ctx.h"
#include "../subgraph_enumeration/subgraph_enumeration.hpp"
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../util/simple_timer.h"
#include "../value.h"
#include "RG.h"

#define MATERIALIZED

// CALL algo.subgraphEnumeration()

typedef struct {
    GraphContext *gc;
    uint64_t **output;
    uint64_t query_size;
    SIValue *formatted_output;
    SIValue r;

    uint64_t current_m;

    uint64_t num_partition;
    uint64_t current_partition;
    uint64_t current_i;
    uint64_t current_j;

    uint64_t **IC;
    uint64_t *IC_size;
    uint64_t *cumulative_IC_size;
    uint64_t **JC;
    uint64_t *JC_size;
    uint64_t *IM;
    uint64_t IM_size;
    uint64_t *JM;
    uint64_t JM_size;
} SubgraphEnumerationContext;

ProcedureResult Proc_SubgraphEnumerationInvoke(ProcedureCtx *ctx,
                                               const SIValue *args,
                                               const char **yield) {
    if (array_len((SIValue *)args) != 1) return PROCEDURE_ERR;

    double result = 0.0;
    double tic[2];
    simple_tic(tic);

    uint64_t query = args[0].longval;

    if (query == 0 || query > 8) return PROCEDURE_ERR;

    SubgraphEnumerationContext *pdata =
        rm_malloc(sizeof(SubgraphEnumerationContext));

    pdata->gc = QueryCtx_GetGraphCtx();
    pdata->output = array_newlen(uint64_t *, 0);
    pdata->formatted_output = array_new(SIValue, 1);

    // TODO: Add argument to change the plan
    uint64_t **plan;
    switch (query) {
        case 1:
            // 3-clique query plan
            pdata->query_size = 3;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 2);
            plan[1][0] = 1;
            plan[2][0] = 1;
            plan[2][1] = 2;
            break;
        case 2:
            // 4-loop query plan
            pdata->query_size = 4;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 1);
            plan[3] = array_newlen(uint64_t, 2);
            plan[1][0] = 1;
            plan[2][0] = 2;
            plan[3][0] = 1;
            plan[3][1] = 3;
            break;
        case 3:
            // 4-clique query plan
            pdata->query_size = 4;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 2);
            plan[3] = array_newlen(uint64_t, 3);
            plan[1][0] = 1;
            plan[2][0] = 1;
            plan[2][1] = 2;
            plan[3][0] = 1;
            plan[3][1] = 2;
            plan[3][2] = 3;
            break;
        case 4:
            // 5-loop query plan
            pdata->query_size = 5;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 1);
            plan[3] = array_newlen(uint64_t, 1);
            plan[4] = array_newlen(uint64_t, 2);
            plan[1][0] = 1;
            plan[2][0] = 2;
            plan[3][0] = 3;
            plan[4][0] = 1;
            plan[4][1] = 4;
            break;
        case 5:
            // 5-clique query plan
            pdata->query_size = 5;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 2);
            plan[3] = array_newlen(uint64_t, 3);
            plan[4] = array_newlen(uint64_t, 4);
            plan[1][0] = 1;
            plan[2][0] = 1;
            plan[2][1] = 2;
            plan[3][0] = 1;
            plan[3][1] = 2;
            plan[3][2] = 3;
            plan[4][0] = 1;
            plan[4][1] = 2;
            plan[4][2] = 3;
            plan[4][3] = 4;
            break;
        case 6:
            // 3-chain query plan
            pdata->query_size = 3;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 1);
            plan[1][0] = 1;
            plan[2][0] = 2;
            break;
        case 7:
            // 4-chain query plan
            pdata->query_size = 4;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 1);
            plan[3] = array_newlen(uint64_t, 1);
            plan[1][0] = 1;
            plan[2][0] = 2;
            plan[3][0] = 3;
            break;
        case 8:
            // 5-chain query plan
            pdata->query_size = 5;
            plan = array_newlen(uint64_t *, pdata->query_size);
            plan[0] = array_newlen(uint64_t, 0);
            plan[1] = array_newlen(uint64_t, 1);
            plan[2] = array_newlen(uint64_t, 1);
            plan[3] = array_newlen(uint64_t, 1);
            plan[4] = array_newlen(uint64_t, 1);
            plan[1][0] = 1;
            plan[2][0] = 2;
            plan[3][0] = 3;
            plan[4][0] = 4;
            break;
    }

    GrB_Matrix A = NULL;
    RG_Matrix_export(&A, Graph_GetAdjacencyMatrix(pdata->gc->g, false));

    // Enumerate
    enumerate_subgraph_v1(&(pdata->IC), &(pdata->IC_size), &(pdata->JC),
                          &(pdata->JC_size), &(pdata->IM), &(pdata->IM_size),
                          &(pdata->JM), &(pdata->JM_size), plan, A);

    // Free
    GrB_Matrix_free(&A);
    array_foreach(plan, e, array_free(e));
    array_free(plan);

    pdata->num_partition = omp_get_max_threads();
    pdata->current_partition = 0;
    pdata->current_i = 0;
    pdata->current_j = 0;
    pdata->current_m = UINT64_MAX;

    pdata->r = SIArray_New(0);

    pdata->cumulative_IC_size =
        array_newlen(uint64_t, pdata->num_partition + 1);
    pdata->cumulative_IC_size[0] = 0;
    for (size_t i = 0; i < pdata->num_partition; i++) {
        pdata->cumulative_IC_size[i + 1] =
            (pdata->cumulative_IC_size[i] - 1) + pdata->IC_size[i];
    }

    ctx->privateData = pdata;

    result += simple_toc(tic);
    printf("Compute: %f ms\n", result * 1e3);

    return PROCEDURE_OK;
}

SIValue *Proc_SubgraphEnumerationStep(ProcedureCtx *ctx) {
    ASSERT(ctx->privateData != NULL);

    SubgraphEnumerationContext *pdata =
        (SubgraphEnumerationContext *)ctx->privateData;

    while (true) {
        if (pdata->current_partition < pdata->num_partition) {
            if (pdata->current_i <
                pdata->IC_size[pdata->current_partition] - 1) {
                if (pdata->current_j <
                    pdata->IC[pdata->current_partition][pdata->current_i + 1]) {
                    size_t M_row =
                        pdata->cumulative_IC_size[pdata->current_partition] +
                        pdata->current_i;

                    if (pdata->current_m != M_row) {
                        SIArray_Free(pdata->r);
                        pdata->r = SI_Array(pdata->query_size);
                        // Clone M
                        for (size_t i = 0; i < pdata->query_size - 1; i++) {
                            size_t M_rowsize = pdata->query_size;
                            size_t M_col = i;

                            Node n = GE_NEW_NODE();
                            Graph_GetNode(
                                pdata->gc->g,
                                pdata->JM[M_row * (M_rowsize - 1) + M_col], &n);
                            pdata->r.array[i] = SI_Node(&n);
                        }
                        pdata->current_m = M_row;
                    }

                    // Append candidates
                    Node n = GE_NEW_NODE();
                    Graph_GetNode(
                        pdata->gc->g,
                        pdata->JC[pdata->current_partition][pdata->current_j],
                        &n);
                    pdata->r.array[pdata->query_size - 1] =
                        SI_Node(&n);
                    
                    pdata->formatted_output[0] = SIArray_Clone(pdata->r);
                    pdata->current_j++;
                    break;
                } else {
                    pdata->current_i++;
                }
            } else {
                free(pdata->IC[pdata->current_partition]);
                free(pdata->JC[pdata->current_partition]);

                pdata->current_partition++;
                pdata->current_i = pdata->current_j = 0;
            }
        } else {
            free(pdata->IC);
            free(pdata->IC_size);
            free(pdata->JC);
            free(pdata->JC_size);
            free(pdata->IM);
            free(pdata->JM);
            return NULL;
        }
    }

    return pdata->formatted_output;
}

ProcedureResult Proc_SubgraphEnumerationFree(ProcedureCtx *ctx) {
    // clean up
    if (ctx->privateData) {
        SubgraphEnumerationContext *pdata = ctx->privateData;

        // Free output
        array_foreach(pdata->output, e, array_free(e));
        array_free(pdata->output);
        array_free(pdata->cumulative_IC_size);

        SIArray_Free(pdata->r);

        rm_free(ctx->privateData);
    }

    return PROCEDURE_OK;
}

ProcedureCtx *Proc_SubgraphEnumerationCtx() {
    void *privateData = NULL;
    ProcedureOutput *outputs = array_new(ProcedureOutput, 1);
#ifdef MATERIALIZED
    ProcedureOutput output = {.name = "nodes", .type = T_ARRAY};
    array_append(outputs, output);
#else
    ProcedureOutput output = {.name = "count", .type = T_INT64};
    array_append(outputs, output);
#endif

    ProcedureCtx *ctx =
        ProcCtxNew("algo.subgraphEnumeration", 1, outputs,
                   Proc_SubgraphEnumerationStep, Proc_SubgraphEnumerationInvoke,
                   Proc_SubgraphEnumerationFree, privateData, true);
    return ctx;
}
