/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
 * or the Server Side Public License v1 (SSPLv1).
 */

#include "op_project.h"

#include "../../query_ctx.h"
#include "../../util/arr.h"
#include "../../util/rmalloc.h"
#include "../../util/simple_timer.h"
#include "RG.h"
#include "op_sort.h"

/* Forward declarations. */
static Record ProjectConsume(OpBase *opBase);
static OpResult ProjectReset(OpBase *opBase);
static OpBase *ProjectClone(const ExecutionPlan *plan, const OpBase *opBase);
static void ProjectFree(OpBase *opBase);

OpBase *NewProjectOp(const ExecutionPlan *plan, AR_ExpNode **exps) {
    OpProject *op = rm_malloc(sizeof(OpProject));
    op->exps = exps;
    op->singleResponse = false;
    op->exp_count = array_len(exps);
    op->record_offsets = array_new(uint, op->exp_count);
    op->r = NULL;
    op->projection = NULL;

    // Set our Op operations
    OpBase_Init((OpBase *)op, OPType_PROJECT, "Project", NULL, ProjectConsume,
                ProjectReset, NULL, ProjectClone, ProjectFree, false, plan);

    for (uint i = 0; i < op->exp_count; i++) {
        // The projected record will associate values with their resolved name
        // to ensure that space is allocated for each entry.
        int record_idx =
            OpBase_Modifies((OpBase *)op, op->exps[i]->resolved_name);
        array_append(op->record_offsets, record_idx);
    }

    op->time = 0.0;

    return (OpBase *)op;
}

static Record ProjectConsume(OpBase *opBase) {
    OpProject *op = (OpProject *)opBase;

    if (op->op.childCount) {
        OpBase *child = op->op.children[0];
        op->r = OpBase_Consume(child);
        if (!op->r) return NULL;
    } else {
        // QUERY: RETURN 1+2
        // Return a single record followed by NULL on the second call.
        if (op->singleResponse) return NULL;
        op->singleResponse = true;
        op->r = OpBase_CreateRecord(opBase);
    }

    op->projection = OpBase_CreateRecord(opBase);

    for (uint i = 0; i < op->exp_count; i++) {
        AR_ExpNode *exp = op->exps[i];
        SIValue v = AR_EXP_Evaluate(exp, op->r);
        int rec_idx = op->record_offsets[i];
        // /* Persisting a value is only necessary here if 'v' refers to a
        // scalar held in Record 'r'.
        //  * Graph entities don't need to be persisted here as Record_Add will
        //  copy them internally.
        //  * The RETURN projection here requires persistence:
        //  * MATCH (a) WITH toUpper(a.name) AS e RETURN e
        //  * TODO This is a rare case; the logic of when to persist can be
        //  improved.  */
        if (!(v.type & SI_GRAPHENTITY)) SIValue_Persist(&v);
        
        // FIXME: Why with Record_Add my new performance is worse? (Reasonable -
        // more instructions) But my old performance is better!?

        // double tic[2], result;
        // simple_tic(tic);
		// printf("%d (type=%d)\n", rec_idx, v.type);

        Record_Add(op->projection, rec_idx, v);
        
        // op->time += simple_toc(tic);

        // /* If the value was a graph entity with its own allocation, as with a
        // query like:
        //  * MATCH p = (src) RETURN nodes(p)[0]
        //  * Ensure that the allocation is freed here. */
        if ((v.type & SI_GRAPHENTITY)) SIValue_Free(v);
    }

    OpBase_DeleteRecord(op->r);
    op->r = NULL;

    // Emit the projected Record once.
    Record projection = op->projection;
    op->projection = NULL;

    return projection;
}

static OpResult ProjectReset(OpBase *opBase) {
    OpProject *op = (OpProject *)opBase;
    op->singleResponse = false;
    return OP_OK;
}

static OpBase *ProjectClone(const ExecutionPlan *plan, const OpBase *opBase) {
    ASSERT(opBase->type == OPType_PROJECT);
    OpProject *op = (OpProject *)opBase;
    AR_ExpNode **exps;
    array_clone_with_cb(exps, op->exps, AR_EXP_Clone);
    return NewProjectOp(plan, exps);
}

static void ProjectFree(OpBase *ctx) {
    OpProject *op = (OpProject *)ctx;

    printf("Projection %f\n", op->time * 1e3);

    if (op->exps) {
        for (uint i = 0; i < op->exp_count; i++) AR_EXP_Free(op->exps[i]);
        array_free(op->exps);
        op->exps = NULL;
    }

    if (op->record_offsets) {
        array_free(op->record_offsets);
        op->record_offsets = NULL;
    }

    if (op->r) {
        OpBase_DeleteRecord(op->r);
        op->r = NULL;
    }

    if (op->projection) {
        OpBase_DeleteRecord(op->projection);
        op->projection = NULL;
    }
}
