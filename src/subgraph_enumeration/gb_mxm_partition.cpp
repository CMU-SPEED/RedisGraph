#include <omp.h>
#include <stdio.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <vector>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../util/simple_timer.h"
}

extern "C" void mxm_partition_merge(size_t **IC, size_t *IC_size, size_t **JC,
                                    size_t *JC_size, size_t **VC,
                                    size_t *VC_size, size_t *IM, size_t IM_size,
                                    size_t *JM, size_t JM_size, size_t *IB,
                                    size_t IB_size, size_t *JB, size_t JB_size,
                                    size_t *VB, size_t VB_size, GrB_Matrix *A);

extern "C" void gb_mxm_partition(GrB_Matrix &C, GrB_Matrix &M, GrB_Matrix &A,
                                 GrB_Matrix &B) {
    GrB_Info info;

    // ⭐️ Extract essential information
    GrB_Index nrows_A, ncols_A, nvals_A;
    GrB_Matrix_nvals(&nvals_A, A);
    GrB_Matrix_nrows(&nrows_A, A);
    GrB_Matrix_ncols(&ncols_A, A);

    GrB_Index nrows_B, ncols_B, nvals_B;
    GrB_Matrix_nvals(&nvals_B, B);
    GrB_Matrix_nrows(&nrows_B, B);
    GrB_Matrix_ncols(&ncols_B, B);

    GrB_Index nrows_M = nrows_A, ncols_M = ncols_B, nvals_M = 0;
    if (M != NULL) {
        GrB_Matrix_nvals(&nvals_M, M);
        GrB_Matrix_nrows(&nrows_M, M);
        GrB_Matrix_ncols(&ncols_M, M);
    }
    assert(nrows_B == nrows_M);
    assert(ncols_B == nrows_A);
    assert(ncols_A == ncols_M);

    // ⭐️ Compute C dimensions
    GrB_Index nrows_C = nrows_M;

    // Extract B
    GrB_Index *IB_arr = new GrB_Index[nrows_B + 1];
    GrB_Index *JB_arr = new GrB_Index[nvals_B];
    bool *VB_arr = new bool[nvals_B];
    GrB_Index IB_len = nrows_B + 1, JB_len = nvals_B, VB_len = nvals_B;
    info = GrB_Matrix_export_BOOL(IB_arr, JB_arr, VB_arr, &IB_len, &JB_len,
                                  &VB_len, GrB_CSR_FORMAT, B);
    assert(info == GrB_SUCCESS);

    // Extract M
    GrB_Index *IM_arr, *JM_arr, IM_len = 0, JM_len = 0, VM_len = 0;
    uint64_t *VM_arr;
    if (M != NULL) {
        IM_arr = new GrB_Index[nrows_M + 1];
        JM_arr = new GrB_Index[nvals_M];
        VM_arr = new uint64_t[nvals_M];
        IM_len = nrows_M + 1;
        JM_len = nvals_M;
        VM_len = nvals_M;
        info = GrB_Matrix_export_UINT64(IM_arr, JM_arr, VM_arr, &IM_len, &JM_len,
                                      &VM_len, GrB_CSR_FORMAT, M);
        assert(info == GrB_SUCCESS);
        delete[] VM_arr;
    }

    std::vector<size_t> IB(IB_arr, IB_arr + (nrows_B + 1));
    delete[] IB_arr;
    std::vector<size_t> JB(JB_arr, JB_arr + nvals_B);
    delete[] JB_arr;
    std::vector<size_t> VB(VB_arr, VB_arr + nvals_B);
    delete[] VB_arr;

    std::vector<size_t> IM, JM;
    if (M != NULL) {
        IM = std::vector<size_t>(IM_arr, IM_arr + (nrows_M + 1));
        delete[] IM_arr;
        JM = std::vector<size_t>(JM_arr, JM_arr + nvals_M);
        delete[] JM_arr;
    }

    size_t *IC = NULL, IC_size = 0, *JC = NULL, JC_size = 0, *VC = NULL,
           VC_size = 0;

    double result = 0.0;
    double tic[2];

    simple_tic(tic);
    {
        mxm_partition_merge(&IC, &IC_size, &JC, &JC_size, &VC, &VC_size,
                            IM.data(), IM.size(), JM.data(), JM.size(),
                            IB.data(), IB.size(), JB.data(), JB.size(),
                            VB.data(), VB.size(), &A);
    }
    result = simple_toc(tic);
    printf("(My MxM) MxM + Merge: %f ms\n", result * 1e3);

    // If there is no data
    if (IC[nrows_C] == 0) {
        // New matrix
        info = GrB_Matrix_new(&C, GrB_BOOL, nrows_M, ncols_M);
    } else {
        info =
            GrB_Matrix_import_UINT64(&C, GrB_UINT64, nrows_M, ncols_M, IC, JC, VC,
                                     IC_size, JC_size, VC_size, GrB_CSR_FORMAT);
        free(IC);
        free(JC);
        free(VC);
    }
    assert(info == GrB_SUCCESS);
}
