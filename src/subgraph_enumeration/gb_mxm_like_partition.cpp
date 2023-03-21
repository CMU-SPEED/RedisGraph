#include <stdio.h>
#include <omp.h>

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

extern "C" void mxm_like_partition(
    std::vector<std::vector<size_t> *> &IC, std::vector<std::vector<size_t> *> &JC,
    std::vector<size_t> &IM, std::vector<size_t> &JM, std::vector<size_t> &IA,
    std::vector<size_t> &JA, std::vector<size_t> &IB, std::vector<size_t> &JB);

extern "C" void gb_mxm_like_partition(GrB_Matrix *&C, GrB_Matrix &M,
                                            GrB_Matrix &A, GrB_Matrix &B) {
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
    assert(nrows_A == nrows_M);
    assert(ncols_A == nrows_B);
    assert(ncols_B == ncols_M);

    // ⭐️ Compute C dimensions
    GrB_Index nrows_C = nrows_M;

    // ⭐️ Extract A, B, M as CSR vectors
    // IA = nrows + 1, JA = nvals, VA = nvals
    // Extract A
    GrB_Index *IA_arr = new GrB_Index[nrows_A + 1];
    GrB_Index *JA_arr = new GrB_Index[nvals_A];
    bool *VA_arr = new bool[nvals_A];
    GrB_Index IA_len = nrows_A + 1, JA_len = nvals_A, VA_len = nvals_A;
    info = GrB_Matrix_export_BOOL(IA_arr, JA_arr, VA_arr, &IA_len, &JA_len,
                                  &VA_len, GrB_CSR_FORMAT, A);
    assert(info == GrB_SUCCESS);
    delete[] VA_arr;

    // Extract B
    GrB_Index *IB_arr = new GrB_Index[nrows_B + 1];
    GrB_Index *JB_arr = new GrB_Index[nvals_B];
    bool *VB_arr = new bool[nvals_B];
    GrB_Index IB_len = nrows_B + 1, JB_len = nvals_B, VB_len = nvals_B;
    info = GrB_Matrix_export_BOOL(IB_arr, JB_arr, VB_arr, &IB_len, &JB_len,
                                  &VB_len, GrB_CSR_FORMAT, B);
    assert(info == GrB_SUCCESS);
    delete[] VB_arr;

    // Extract M
    GrB_Index *IM_arr, *JM_arr, IM_len = 0, JM_len = 0, VM_len = 0;
    bool *VM_arr;
    if (M != NULL) {
        IM_arr = new GrB_Index[nrows_M + 1];
        JM_arr = new GrB_Index[nvals_M];
        VM_arr = new bool[nvals_M];
        IM_len = nrows_M + 1;
        JM_len = nvals_M;
        VM_len = nvals_M;
        info = GrB_Matrix_export_BOOL(IM_arr, JM_arr, VM_arr, &IM_len, &JM_len,
                                      &VM_len, GrB_CSR_FORMAT, M);
        assert(info == GrB_SUCCESS);
        delete[] VM_arr;
    }

    std::vector<size_t> IA(IA_arr, IA_arr + (nrows_A + 1));
    delete[] IA_arr;
    std::vector<size_t> JA(JA_arr, JA_arr + nvals_A);
    delete[] JA_arr;
    std::vector<size_t> IB(IB_arr, IB_arr + (nrows_B + 1));
    delete[] IB_arr;
    std::vector<size_t> JB(JB_arr, JB_arr + nvals_B);
    delete[] JB_arr;

    std::vector<size_t> IM, JM;
    if (M != NULL) {
        IM = std::vector<size_t>(IM_arr, IM_arr + (nrows_M + 1));
        delete[] IM_arr;
        JM = std::vector<size_t>(JM_arr, JM_arr + nvals_M);
        delete[] JM_arr;
    }

    std::vector<std::vector<size_t> *> IC, JC;

    mxm_like_partition(IC, JC, IM, JM, IB, JB, IA, JA);

    size_t num_threads = omp_get_max_threads();

    // If there is no data
#pragma omp parallel for num_threads(num_threads)
    for (size_t i = 0; i < num_threads; i++) {
        if (IC[i]->back() == 0) {
            // New matrix
            info = GrB_Matrix_new(&(C[i]), GrB_BOOL, IC[i]->size() - 1, ncols_M);
        } else {
            std::vector<bool> VC(JC[i]->size(), 1);
            bool *VC_data = new bool[VC.size()];
            std::copy(std::begin(VC), std::end(VC), VC_data);
            // Import C as CSR
            info = GrB_Matrix_import_BOOL(&(C[i]), GrB_BOOL, IC[i]->size() - 1, ncols_M, IC[i]->data(),
                                        JC[i]->data(), VC_data, IC[i]->size(), JC[i]->size(),
                                        VC.size(), GrB_CSR_FORMAT);
            delete[] VC_data;
            delete IC[i];
            delete JC[i];
        }
        assert(info == GrB_SUCCESS);
    }
}

extern "C" void _gb_mxm_like_partition(GrB_Matrix **C, GrB_Matrix *M,
                                             GrB_Matrix *A, GrB_Matrix *B) {
    gb_mxm_like_partition(*C, *M, *A, *B);
}