#include <stdio.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../util/arr.h"
}

extern "C" void gb_matrix_filter(GrB_Matrix &C, GrB_Matrix M, GrB_Matrix B,
                                 GrB_Matrix A, uint64_t v);

extern "C" void gb_matrix_intersection(GrB_Matrix &C, GrB_Matrix M,
                                       GrB_Matrix B, GrB_Matrix A,
                                       std::vector<uint64_t> &v);

extern "C" void gb_mxm_like_partition_merge(GrB_Matrix &C, GrB_Matrix &M,
                                            GrB_Matrix &A, GrB_Matrix &B);

extern "C" void gb_set_include(bool *z, const uint64_t *x, uint64_t i,
                               uint64_t j, const uint64_t *y) {
    std::vector<uint64_t> set = *((std::vector<uint64_t> *)*y);
    *z = std::find(set.begin(), set.end(), *x) != set.end();
}

extern "C" void enumerate_subgraph(uint64_t ***out, uint64_t *out_size,
                                   uint64_t **plan, GrB_Matrix A,
                                   uint64_t mode) {
    // Rebuild N_P_plus as STL/C++ vector
    std::vector<std::vector<uint64_t>> N_P_plus(array_len(plan));
    for (uint64_t i = 0; i < array_len(plan); i++) {
        for (uint64_t j = 0; j < array_len(plan[i]); j++) {
            N_P_plus[i].push_back(plan[i][j]);
        }
    }

    // Variable Initialization
    GrB_Info info;

    // Set Inclusion Operator
    GrB_IndexUnaryOp set_include_op;
    info = GrB_IndexUnaryOp_new(&set_include_op,
                                (GxB_index_unary_function)gb_set_include,
                                GrB_BOOL, GrB_UINT64, GrB_UINT64);
    assert(info == GrB_SUCCESS);

    // Each result set
    GrB_Index V_P_size = N_P_plus.size();
    GrB_Index V_G_size;
    GrB_Matrix_nrows(&V_G_size, A);
    std::vector<GrB_Matrix> R(V_P_size);

    // ⭐️ Compute R[0]
    // R[0] = b(diag(V(G)))
    info = GrB_Matrix_new(&(R[0]), GrB_UINT64, V_G_size, V_G_size);
    assert(info == GrB_SUCCESS);
    for (uint64_t i = 0; i < V_G_size; i++) {
        info = GrB_Matrix_setElement_UINT64(R[0], 1, i, i);
        assert(info == GrB_SUCCESS);
    }

    // ⭐️ Loop from 1 to |V(P)| (excluding)
    for (uint64_t i = 1; i < V_P_size; i++) {
        // ⭐️ Compute C
        // ((R[i-1] * (R[i-1] in O[i])) (^,^) A) * b(R[i-1] = 0)
        // Candidate Matrix
        // Each row is an int-map vector demonstrating
        //  all candidates for the vertex based on
        //  the walk represented by the row
        // Dimension: number of walks at T=i-1 x number of vertices
        // 👉 Compute (R[i-1] * (R[i-1] in O[i])) as R_in_O
        GrB_Matrix R_in_O;
        GrB_Index num_walks;
        info = GrB_Matrix_nrows(&num_walks, R[i - 1]);
        assert(info == GrB_SUCCESS);
        info = GrB_Matrix_new(&R_in_O, GrB_BOOL, num_walks, V_G_size);
        assert(info == GrB_SUCCESS);
        info = GrB_Matrix_select_UINT64(R_in_O, GrB_NULL, GrB_NULL,
                                        set_include_op, R[i - 1],
                                        (uint64_t) & (N_P_plus[i]), GrB_NULL);
        assert(info == GrB_SUCCESS);

        // 👉 Use mxm-like routine
        GrB_Matrix C;

        switch (mode) {
            case 1:
                gb_matrix_filter(C, R[i - 1], R_in_O, A, N_P_plus[i].size());
                break;
            case 2:
                gb_matrix_intersection(C, R[i - 1], R_in_O, A, N_P_plus[i]);
                break;
            case 3:
                gb_mxm_like_partition_merge(C, R[i - 1], R_in_O, A);
                break;
        }

        // ⭐️ Compute R[i]
        // 👉 Loop for each row vector m in R[i-1]
        //    Select row vector m with (selection vector * A)
        GrB_Index C_nvals;
        info = GrB_Matrix_nvals(&C_nvals, C);
        assert(info == GrB_SUCCESS);

        // 👉 Loop for each element in C
        GrB_Index *IC, *JC;
        void *VC;
        GrB_Index IC_size, JC_size, VC_size;
        bool C_iso, C_jumbled;
        info = GxB_Matrix_unpack_CSR(C, &IC, &JC, &VC, &IC_size, &JC_size,
                                     &VC_size, &C_iso, &C_jumbled, NULL);
        assert(info == GrB_SUCCESS);

        // FIXME: May need to remove
        // Free C
        info = GrB_Matrix_free(&C);
        assert(info == GrB_SUCCESS);

        GrB_Index *IR = new GrB_Index[C_nvals + 1];
        GrB_Index *JR = new GrB_Index[(C_nvals + 1) * (i + 1)];
        uint64_t *VR = new uint64_t[(C_nvals + 1) * (i + 1)];
        IR[0] = 0;

        // ⭐️ R[i][j] = m + q
        // Breakdown R[i-1] to CSR
        GrB_Index R_rows;
        GrB_Matrix_nrows(&R_rows, R[i - 1]);
        GrB_Index R_nvals;
        GrB_Matrix_nvals(&R_nvals, R[i - 1]);

        GrB_Index *IR_prev = new GrB_Index[R_nvals];
        GrB_Index *JR_prev = new GrB_Index[R_nvals];
        uint64_t *VR_prev = new uint64_t[R_nvals];
        info = GrB_Matrix_extractTuples_UINT64(IR_prev, JR_prev, VR_prev,
                                               &R_nvals, R[i - 1]);
        assert(info == GrB_SUCCESS);

        GrB_Index j = 0, idx = 0;
        for (uint64_t k = 0; k < R_rows; k++) {
            for (uint64_t ic = IC[k]; ic < IC[k + 1]; ic++) {
                uint64_t p = JC[ic];
                bool is_visited = false;

                // R[i][j] = m + q
                // where q is zero vector and q[p] = i
                uint64_t en = (k * i) + i;
                for (uint64_t l = k * i; l < en; l++) {
                    if (!is_visited && JR_prev[l] >= p) {
                        JR[idx] = p;
                        VR[idx] = (uint64_t)i + 1;
                        idx++;
                        is_visited = true;
                    }

                    JR[idx] = JR_prev[l];
                    VR[idx] = (uint64_t)VR_prev[l];
                    idx++;
                }

                if (!is_visited) {
                    JR[idx] = p;
                    VR[idx] = (uint64_t)i + 1;
                    idx++;
                }

                IR[j + 1] = IR[j] + i + 1;
                j++;
            }
        }

        delete[] IR_prev;
        delete[] JR_prev;
        delete[] VR_prev;

        info = GrB_Matrix_import_UINT64(
            &(R[i]), GrB_UINT64, C_nvals, V_G_size, IR, JR, VR, (C_nvals + 1),
            ((C_nvals + 1) * (i + 1)), (C_nvals + 1) * (i + 1), GrB_CSR_FORMAT);
        assert(info == GrB_SUCCESS);

        delete[] IR;
        delete[] JR;
        delete[] VR;

        info = GrB_Matrix_free(&(R[i - 1]));
        assert(info == GrB_SUCCESS);
    }

    // Free structures
    info = GrB_IndexUnaryOp_free(&set_include_op);
    assert(info == GrB_SUCCESS);

    // Extract output
    uint64_t pattern_size = N_P_plus.size();
    GrB_Index output_size;
    info = GrB_Matrix_nvals(&output_size, R[pattern_size - 1]);
    assert(info == GrB_SUCCESS);
    GrB_Index *IR_last = new GrB_Index[output_size];
    GrB_Index *JR_last = new GrB_Index[output_size];
    uint64_t *VR_last = new uint64_t[output_size];
    info = GrB_Matrix_extractTuples_UINT64(IR_last, JR_last, VR_last,
                                           &output_size, R[pattern_size - 1]);
    assert(info == GrB_SUCCESS);
    // Free R[last]
    GrB_Matrix_free(&(R[pattern_size - 1]));

    // Put output into the output vector
    *out_size = output_size / pattern_size;

    *out = array_newlen(uint64_t *, *out_size);
    for (uint64_t i = 0; i < *out_size; i++) {
        uint64_t *tmp = NULL;
        tmp = array_newlen(uint64_t, pattern_size);

        for (uint64_t j = 0; j < pattern_size; j++) {
            tmp[VR_last[(i * pattern_size) + j] - 1] =
                JR_last[(i * pattern_size) + j];
        }

        (*out)[i] = tmp;
    }

    delete[] IR_last;
    delete[] JR_last;
    delete[] VR_last;
}