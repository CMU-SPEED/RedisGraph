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
}

extern "C" void mxm_like_partition_merge(std::vector<size_t> &IC, std::vector<size_t> &JC,
                              std::vector<size_t> &IM, std::vector<size_t> &JM,
                              std::vector<size_t> &IA, std::vector<size_t> &JA,
                              std::vector<size_t> &IB,
                              std::vector<size_t> &JB);

extern "C" void gb_mxm_like_partition_merge(GrB_Matrix &C, GrB_Matrix &M,
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
    GrB_Index nrows_M, ncols_M, nvals_M;
    GrB_Matrix_nvals(&nvals_M, M);
    GrB_Matrix_nrows(&nrows_M, M);
    GrB_Matrix_ncols(&ncols_M, M);

    // ⭐️ Assert dimensions
    // M
    assert(nrows_A == nrows_M);
    // K
    assert(ncols_A == nrows_B);
    // N
    assert(ncols_B == ncols_M);

    // ⭐️ Compute C dimensions
    GrB_Index nrows_C, ncols_C;
    nrows_C = nrows_M;
    ncols_C = ncols_M;

    // ⭐️ Extract A, B, M as CSR vectors
    // IA = nrows + 1, JA = nvals, VA = nvals
    // Extract A
    GrB_Index *IA_arr = new GrB_Index[nrows_A + 1];
    GrB_Index *JA_arr = new GrB_Index[nvals_A];
    uint64_t *VA_arr = new uint64_t[nvals_A];
    GrB_Index IA_len = nrows_A + 1, JA_len = nvals_A, VA_len = nvals_A;
    info = GrB_Matrix_export_UINT64(IA_arr, JA_arr, VA_arr, &IA_len, &JA_len,
                                    &VA_len, GrB_CSR_FORMAT, A);
    assert(info == GrB_SUCCESS);
    // Extract B
    GrB_Index *IB_arr = new GrB_Index[nrows_B + 1];
    GrB_Index *JB_arr = new GrB_Index[nvals_B];
    bool *VB_arr = new bool[nvals_B];
    GrB_Index IB_len = nrows_B + 1, JB_len = nvals_B, VB_len = nvals_B;
    info = GrB_Matrix_export_BOOL(IB_arr, JB_arr, VB_arr, &IB_len, &JB_len,
                                  &VB_len, GrB_CSR_FORMAT, B);
    assert(info == GrB_SUCCESS);
    // Extract M
    GrB_Index *IM_arr = new GrB_Index[nrows_M + 1];
    GrB_Index *JM_arr = new GrB_Index[nvals_M];
    uint64_t *VM_arr = new uint64_t[nvals_M];
    GrB_Index IM_len = nrows_M + 1, JM_len = nvals_M, VM_len = nvals_M;
    info = GrB_Matrix_export_UINT64(IM_arr, JM_arr, VM_arr, &IM_len, &JM_len,
                                    &VM_len, GrB_CSR_FORMAT, M);
    assert(info == GrB_SUCCESS);

    std::vector<size_t> IA(IA_arr, IA_arr + (nrows_A + 1));
    delete[] IA_arr;
    std::vector<size_t> JA(JA_arr, JA_arr + nvals_A);
    delete[] JA_arr;
    std::vector<size_t> IB(IB_arr, IB_arr + (nrows_B + 1));
    delete[] IB_arr;
    std::vector<size_t> JB(JB_arr, JB_arr + nvals_B);
    delete[] JB_arr;
    std::vector<size_t> IM(IM_arr, IM_arr + (nrows_M + 1));
    delete[] IM_arr;
    std::vector<size_t> JM(JM_arr, JM_arr + nvals_M);
    delete[] JM_arr;
    std::vector<size_t> IC, JC;

    // Do mxm
    mxm_like_partition_merge(IC, JC, IM, JM, IB, JB, IA, JA);

    std::cout << std::endl << IC.size() << " " << JC.size() << std::endl;

    // If there is no data
    if (IC[nrows_C] == 0) {
        // New matrix
        info = GrB_Matrix_new(&C, GrB_BOOL, nrows_M, ncols_M);
    } else {
        std::vector<bool> VC(JC.size(), 1);
        bool *VC_data = new bool[VC.size()];
        std::copy(std::begin(VC), std::end(VC), VC_data);
        // Import C as CSR
        info = GrB_Matrix_import_BOOL(&C, GrB_BOOL, nrows_M, ncols_M, IC.data(),
                                      JC.data(), VC_data, IC.size(), JC.size(),
                                      VC.size(), GrB_CSR_FORMAT);
        delete[] VC_data;
    }
    assert(info == GrB_SUCCESS);
}