#include <omp.h>
#include <stdio.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <vector>

extern "C" {
#include "../src/execution_plan/record.h"
#include "../util/simple_timer.h"
}

extern "C" void mxv_v1(std::vector<size_t> &Ic, std::vector<size_t> &Vc,
                       size_t *Im, size_t Im_size, size_t *IA, size_t IA_size,
                       size_t *JA, size_t JA_size, bool *VA, size_t VA_size,
                       size_t *Ib, size_t Ib_size, size_t *Vb, size_t Vb_size);

extern "C" void mxv_v2(std::vector<size_t> &Ic, std::vector<size_t> &Vc,
                       size_t *Im, size_t Im_size, size_t *IA, size_t IA_size,
                       size_t *JA, size_t JA_size, bool *VA, size_t VA_size,
                       size_t *Ib, size_t Ib_size, size_t *Vb, size_t Vb_size);

extern "C" void mxm_partition_merge(size_t **IC, size_t *IC_size, size_t **JC,
                                    size_t *JC_size, size_t **VC,
                                    size_t *VC_size, size_t *IM, size_t IM_size,
                                    size_t *JM, size_t JM_size, size_t *IB,
                                    size_t IB_size, size_t *JB, size_t JB_size,
                                    size_t *VB, size_t VB_size, GrB_Matrix *A) {
    size_t num_threads = omp_get_max_threads();

    // Build A from GraphBLAS Matrix A
    size_t *IA_arr, *JA_arr, IA_size, JA_size, VA_size;

    GrB_Index nrows_A;
    GrB_Matrix_nrows(&nrows_A, *A);
    GrB_Matrix_nvals(&JA_size, *A);
    IA_size = nrows_A + 1;
    VA_size = JA_size;

    IA_arr = new size_t[IA_size];
    JA_arr = new size_t[JA_size];
    bool *VA_arr = new bool[JA_size];
    GrB_Index IA_len = IA_size, JA_len = JA_size, VA_len = JA_size;

    GrB_Info info = GrB_Matrix_export_BOOL(
        IA_arr, JA_arr, VA_arr, &IA_len, &JA_len, &VA_len, GrB_CSR_FORMAT, *A);
    assert(info == GrB_SUCCESS);

    // Building M and B and be merged (but why?)
    // Reduce the number of GET_NODE
    size_t *IM_arr = IM, *JM_arr = JM;
    size_t *IB_arr = IB, *JB_arr = JB, *VB_arr = VB;

    // Create partitions
    std::vector<std::vector<size_t> *> partitioned_IC(num_threads);
    std::vector<std::vector<size_t> *> partitioned_JC(num_threads);
    std::vector<std::vector<size_t> *> partitioned_VC(num_threads);

    size_t partition_size = (IB_size - 1) / num_threads;
    std::vector<size_t> partition_offset(num_threads + 1);
    partition_offset[0] = 0;
    partition_offset[1] = partition_size + ((IB_size - 1) % num_threads);
    partitioned_IC[0] = new std::vector<size_t>();
    partitioned_IC[0]->push_back(0);
    partitioned_JC[0] = new std::vector<size_t>();
    partitioned_VC[0] = new std::vector<size_t>();

    for (size_t t = 1; t < num_threads; t++) {
        partition_offset[t + 1] = partition_offset[t] + partition_size;
        partitioned_IC[t] = new std::vector<size_t>();
        partitioned_IC[t]->push_back(0);
        partitioned_JC[t] = new std::vector<size_t>();
        partitioned_VC[t] = new std::vector<size_t>();
    }

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (size_t partition = 0; partition < num_threads; partition++) {
        for (size_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<size_t> tmp_C;
            std::vector<size_t> tmp_V;

            size_t *M_st = NULL, M_size = 0;
            if (IM_size != 0) {
                M_st = JM_arr + IM_arr[ib];
                M_size = IM_arr[ib + 1] - IM_arr[ib];
            }

            size_t *Ib_st = JB_arr + IB_arr[ib];
            size_t Ib_size = IB_arr[ib + 1] - IB_arr[ib];
            size_t *Vb_st = VB_arr + IB_arr[ib];
            size_t Vb_size = IB_arr[ib + 1] - IB_arr[ib];

            // Copy and Sorted
            std::vector<size_t> M_sorted(M_st, M_st + M_size);
            std::sort(std::begin(M_sorted), std::end(M_sorted));

            mxv_v1(tmp_C, tmp_V, M_sorted.data(), M_sorted.size(), IA_arr,
                   IA_size, JA_arr, JA_size, VA_arr, VA_size, Ib_st, Ib_size,
                   Vb_st, Vb_size);

            // FIXME: We should not copy (unnecessary operations)
            partitioned_JC[partition]->insert(partitioned_JC[partition]->end(),
                                              tmp_C.begin(), tmp_C.end());
            partitioned_VC[partition]->insert(partitioned_VC[partition]->end(),
                                              tmp_V.begin(), tmp_V.end());
            partitioned_IC[partition]->push_back(
                partitioned_JC[partition]->size());
        }
    }

    // Start offset for IC
    std::vector<size_t> start_rows(num_threads + 1);
    start_rows[0] = 0;
    for (size_t i = 0; i < num_threads; i++) {
        start_rows[i + 1] = start_rows[i] + (partitioned_IC[i]->size() - 1);
    }

    // Start offset for JC
    std::vector<size_t> start_cols(num_threads + 1);
    start_cols[0] = 0;
    for (size_t i = 0; i < num_threads; i++) {
        start_cols[i + 1] = start_cols[i] + partitioned_JC[i]->size();
    }

    *IC = NULL;
    *IC_size = start_rows[num_threads] + 1;
    *JC = NULL;
    *JC_size = start_cols[num_threads];
    *VC = NULL;
    *VC_size = start_cols[num_threads];

    *IC = (size_t *)malloc(sizeof(size_t) * (start_rows[num_threads] + 1));
    if (*IC == NULL) {
        return;
    }

    *JC = (size_t *)malloc(sizeof(size_t) * start_cols[num_threads]);
    if (*JC == NULL) {
        return;
    }

    *VC = (size_t *)malloc(sizeof(size_t) * start_cols[num_threads]);
    if (*VC == NULL) {
        return;
    }

    double result = 0.0;
    double tic[2];

    simple_tic(tic);
    {
// Merge
#pragma omp parallel for num_threads(num_threads)
        for (size_t partition = 0; partition < num_threads; partition++) {
            // Copy I
            for (size_t i = 0; i < partitioned_IC[partition]->size() - 1; i++) {
                (*IC)[start_rows[partition] + i] =
                    partitioned_IC[partition]->at(i) + start_cols[partition];
            }

            // Copy J and V
            for (size_t j = 0; j < partitioned_JC[partition]->size(); j++) {
                (*JC)[start_cols[partition] + j] =
                    partitioned_JC[partition]->at(j);
                (*VC)[start_cols[partition] + j] =
                    partitioned_VC[partition]->at(j);
            }
        }
        (*IC)[start_rows[num_threads]] = start_cols[num_threads];
    }
    result = simple_toc(tic);
    printf("Merge: %f ms\n", result * 1e3);
}
