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
    uint64_t count;    // Current schema ID.
    GraphContext *gc;  // Graph context.
    SIValue *output;   // Output label.
} SubgraphEnumerationContext;

ProcedureResult Proc_SubgraphEnumerationInvoke(ProcedureCtx *ctx,
                                               const SIValue *args,
                                               const char **yield) {
    if (array_len((SIValue *)args) != 0) return PROCEDURE_ERR;

    SubgraphEnumerationContext *pdata =
        rm_malloc(sizeof(SubgraphEnumerationContext));

    pdata->count = 0;
    pdata->gc = QueryCtx_GetGraphCtx();
    pdata->output = array_new(SIValue, 1);

    GrB_Matrix graph = pdata->gc->g->adjacency_matrix->matrix;

    uint64_t **output = array_newlen(uint64_t *, 0);
    uint64_t output_size = 0;
    uint64_t **plan = array_newlen(uint64_t *, 3);
    plan[0] = array_newlen(uint64_t, 0);
    plan[1] = array_newlen(uint64_t, 1);
    plan[2] = array_newlen(uint64_t, 2);
    plan[1][0] = 1;
    plan[2][0] = 1;
    plan[2][1] = 2;

    printf("LEN | %d %d\n", array_len(output), array_len(plan));

    enumerate_subgraph(&output, &output_size, plan, graph, 3);
    printf("Output Size: %d\n", output_size);

    // Free plan
    array_foreach(plan, e, array_free(e));
    array_free(plan);
    array_foreach(output, e, array_free(e));
    array_free(output);

    ctx->privateData = pdata;

    return PROCEDURE_OK;
}

SIValue *Proc_SubgraphEnumerationStep(ProcedureCtx *ctx) {
    ASSERT(ctx->privateData != NULL);

    SubgraphEnumerationContext *pdata =
        (SubgraphEnumerationContext *)ctx->privateData;

    // depleted?
    if (pdata->count >= 3) return NULL;

    pdata->output[0] = SI_Array(3);

    Node n1 = GE_NEW_NODE();
    Graph_GetNode(pdata->gc->g, pdata->count * 3 + 0, &n1);
    SIArray_Append(&(pdata->output[0]), SI_Node(&n1));

    Node n2 = GE_NEW_NODE();
    Graph_GetNode(pdata->gc->g, pdata->count * 3 + 1, &n2);
    SIArray_Append(&(pdata->output[0]), SI_Node(&n2));

    Node n3 = GE_NEW_NODE();
    Graph_GetNode(pdata->gc->g, pdata->count * 3 + 2, &n3);
    SIArray_Append(&(pdata->output[0]), SI_Node(&n3));

    pdata->count += 1;

    return pdata->output;
}

ProcedureResult Proc_SubgraphEnumerationFree(ProcedureCtx *ctx) {
    // clean up
    if (ctx->privateData) {
        SubgraphEnumerationContext *pdata = ctx->privateData;
        array_free(pdata->output);
        rm_free(ctx->privateData);
    }

    return PROCEDURE_OK;
}

ProcedureCtx *Proc_SubgraphEnumerationCtx() {
    void *privateData = NULL;
    ProcedureOutput *outputs = array_new(ProcedureOutput, 1);
    ProcedureOutput output = {.name = "nodes", .type = T_ARRAY};
    array_append(outputs, output);

    ProcedureCtx *ctx =
        ProcCtxNew("algo.subgraphEnumeration", 0, outputs,
                   Proc_SubgraphEnumerationStep, Proc_SubgraphEnumerationInvoke,
                   Proc_SubgraphEnumerationFree, privateData, true);
    return ctx;
}
