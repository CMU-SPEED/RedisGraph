/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include "proc_subgraph_enumeration.h"

#include "../datatypes/array.h"
#include "../graph/graphcontext.h"
#include "../query_ctx.h"
#include "../subgraph_enumeration/subgraph_enumeration.hpp"
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../value.h"
#include "RG.h"

// CALL algo.subgraphEnumeration()

typedef struct {
    uint64_t count;     // Current schema ID.
    GraphContext *gc;   // Graph context.
    uint64_t **output;  // Output label.
    uint64_t output_size;
    uint64_t query_size;
    SIValue *formatted_output;
} SubgraphEnumerationContext;

ProcedureResult Proc_SubgraphEnumerationInvoke(ProcedureCtx *ctx,
                                               const SIValue *args,
                                               const char **yield) {
    if (array_len((SIValue *)args) != 1) return PROCEDURE_ERR;

    uint64_t query = args[0].longval;

    if (query == 0 || query > 5) return PROCEDURE_ERR;

    SubgraphEnumerationContext *pdata =
        rm_malloc(sizeof(SubgraphEnumerationContext));

    pdata->count = 0;
    pdata->gc = QueryCtx_GetGraphCtx();
    pdata->output = array_newlen(uint64_t *, 0);
    pdata->output_size = 0;
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
    }

    GrB_Matrix A = NULL;
    RG_Matrix_export(&A, Graph_GetAdjacencyMatrix(pdata->gc->g, false));

    // Enumerate
    enumerate_subgraph(&(pdata->output), &(pdata->output_size), plan, A, 3);

    // Free
    GrB_Matrix_free(&A);
    array_foreach(plan, e, array_free(e));
    array_free(plan);

    ctx->privateData = pdata;

    return PROCEDURE_OK;
}

SIValue *Proc_SubgraphEnumerationStep(ProcedureCtx *ctx) {
    ASSERT(ctx->privateData != NULL);

    SubgraphEnumerationContext *pdata =
        (SubgraphEnumerationContext *)ctx->privateData;

    // depleted?
    if (pdata->count >= pdata->output_size) return NULL;
        // if (pdata->count >= 100) return NULL;

#ifdef MATERIALIZED
    pdata->formatted_output[0] = SI_Array(pdata->query_size);

    for (uint64_t i = 0; i < pdata->query_size; i++) {
        Node n = GE_NEW_NODE();
        Graph_GetNode(pdata->gc->g, pdata->output[pdata->count][i], &n);
        SIArray_Append(&(pdata->formatted_output[0]), SI_Node(&n));
    }

    pdata->count += 1;

    if (pdata->count % 100000 == 0) {
        printf("%d\n", pdata->count);
    }
#else
    pdata->formatted_output[0] = SI_LongVal(pdata->output_size);
    pdata->count = pdata->output_size;
#endif

    return pdata->formatted_output;
}

ProcedureResult Proc_SubgraphEnumerationFree(ProcedureCtx *ctx) {
    // clean up
    if (ctx->privateData) {
        SubgraphEnumerationContext *pdata = ctx->privateData;

        // Free output
        array_foreach(pdata->output, e, array_free(e));
        array_free(pdata->output);

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
