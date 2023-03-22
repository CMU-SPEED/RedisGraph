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

extern "C" void mxv_like_v1(std::vector<size_t> &Ic, size_t *Im, size_t Im_size,
                            size_t *IA, size_t IA_size, size_t *JA,
                            size_t JA_size, size_t *Ib, size_t Ib_size);

extern "C" void mxm_like_partition_no_conv(
    size_t ***IC, size_t **IC_size, size_t ***JC, size_t **JC_size, size_t *IM,
    size_t IM_size, size_t *JM, size_t JM_size, size_t *IB, size_t IB_size,
    size_t *JB, size_t JB_size, GrB_Matrix *A) {
    size_t num_threads = omp_get_max_threads();

    // Build A from GraphBLAS Matrix A
    size_t *IA_arr, *JA_arr, IA_size, JA_size;

    GrB_Index nrows_A;
    GrB_Matrix_nrows(&nrows_A, *A);
    GrB_Matrix_nvals(&JA_size, *A);
    IA_size = nrows_A + 1;
    // printf("IA_size=%lu JA_size=%lu\n", IA_size, JA_size);

    IA_arr = new size_t[IA_size];
    JA_arr = new size_t[JA_size];
    bool *VA_arr = new bool[JA_size];
    GrB_Index IA_len = IA_size, JA_len = JA_size, VA_len = JA_size;

    GrB_Info info = GrB_Matrix_export_BOOL(
        IA_arr, JA_arr, VA_arr, &IA_len, &JA_len, &VA_len, GrB_CSR_FORMAT, *A);
    assert(info == GrB_SUCCESS);
    delete[] VA_arr;

    // Building M and B and be merged (but why?)
    // Reduce the number of GET_NODE
    size_t *IM_arr = IM, *JM_arr = JM;
    size_t *IB_arr = IB, *JB_arr = JB;

    // Create partitions
    std::vector<std::vector<size_t> *> partitioned_IC(num_threads);
    std::vector<std::vector<size_t> *> partitioned_JC(num_threads);

    size_t partition_size = (IB_size - 1) / num_threads;
    std::vector<size_t> partition_offset(num_threads + 1);
    partition_offset[0] = 0;
    partition_offset[1] = partition_size + ((IB_size - 1) % num_threads);
    partitioned_IC[0] = new std::vector<size_t>();
    partitioned_IC[0]->push_back(0);
    partitioned_JC[0] = new std::vector<size_t>();

    for (size_t t = 1; t < num_threads; t++) {
        partition_offset[t + 1] = partition_offset[t] + partition_size;
        partitioned_IC[t] = new std::vector<size_t>();
        partitioned_IC[t]->push_back(0);
        partitioned_JC[t] = new std::vector<size_t>();
    }

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (size_t partition = 0; partition < num_threads; partition++) {
        for (size_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<size_t> tmp_C;

            size_t *M_st = NULL, M_size = 0;
            if (IM_size != 0) {
                M_st = JM_arr + IM_arr[ib];
                M_size = IM_arr[ib + 1] - IM_arr[ib];
            }

            size_t *B_st = JB_arr + IB_arr[ib];
            size_t B_size = IB_arr[ib + 1] - IB_arr[ib];

            // Copy and Sorted
            std::vector<size_t> M_sorted(M_st, M_st + M_size);
            std::sort(std::begin(M_sorted), std::end(M_sorted));

            mxv_like_v1(tmp_C, M_sorted.data(), M_sorted.size(), IA_arr, IA_size, JA_arr, JA_size,
                        B_st, B_size);

            // FIXME: We should not copy (unnecessary operations)
            partitioned_JC[partition]->insert(partitioned_JC[partition]->end(),
                                              tmp_C.begin(), tmp_C.end());
            partitioned_IC[partition]->push_back(
                partitioned_JC[partition]->size());
        }

        // Pack outputs
        (*IC)[partition] = partitioned_IC[partition]->data();
        (*IC_size)[partition] = partitioned_IC[partition]->size();
        (*JC)[partition] = partitioned_JC[partition]->data();
        (*JC_size)[partition] = partitioned_JC[partition]->size();
    }
}
