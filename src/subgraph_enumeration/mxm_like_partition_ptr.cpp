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

extern "C" void mxm_like_partition_ptr(size_t ***IC, size_t **IC_size,
                                       size_t ***JC, size_t **JC_size,
                                       Record **records, uint64_t record_count,
                                       GrB_Matrix *A, bool **plan,
                                       uint64_t plan_nnz,
                                       uint64_t current_record_size) {
    size_t num_threads = omp_get_max_threads();

    double result = 0.0;
    double tic[2];

    simple_tic(tic);

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

//     // Build M from records
//     size_t *IM_arr, *JM_arr, IM_size, JM_size;
//     IM_size = record_count + 1;
//     JM_size = record_count * current_record_size;
//     IM_arr = new size_t[IM_size];
//     JM_arr = new size_t[JM_size];
//     IM_arr[0] = 0;

// #pragma omp parallel for num_threads(num_threads)
//     for (size_t i = 1; i < IM_size; i++) {
//         IM_arr[i] = i * current_record_size;
//     }

// #pragma omp parallel for num_threads(num_threads)
//     for (size_t i = 0; i < record_count; i++) {
//         Record r = (*records)[i];
//         uint r_len = 0;
//         for (uint j = 0; j < Record_length(r); j++) {
//             if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
//             Node *n = Record_GetNode(r, j);
//             NodeID id = ENTITY_GET_ID(n);
//             JM_arr[(i * current_record_size) + r_len++] = id;
//         }
//         assert(r_len == current_record_size);
//         std::sort(JM_arr + IM_arr[i], JM_arr + IM_arr[i + 1]);
//     }

    // Build B from records and plan
    size_t *IB_arr, *JB_arr, IB_size, JB_size;
    IB_size = record_count + 1;
    JB_size = record_count * plan_nnz;
    IB_arr = new size_t[IB_size];
    JB_arr = new size_t[JB_size];
    IB_arr[0] = 0;

#pragma omp parallel for num_threads(num_threads)
    for (size_t i = 1; i < IB_size; i++) {
        IB_arr[i] = i * plan_nnz;
    }

#pragma omp parallel for num_threads(num_threads)
    for (size_t i = 0; i < record_count; i++) {
        Record r = (*records)[i];
        uint r_len = 0, actual_len = 0;
        for (uint j = 0; j < Record_length(r); j++) {
            if (Record_GetType(r, j) != REC_TYPE_NODE) continue;
            Node *n = Record_GetNode(r, j);
            NodeID id = ENTITY_GET_ID(n);
            if ((*plan)[r_len]) {
                JB_arr[(i * plan_nnz) + actual_len++] = id;
            }
            r_len++;
        }
        assert(actual_len == plan_nnz);
        std::sort(JB_arr + IB_arr[i], JB_arr + IB_arr[i + 1]);
    }

    result = simple_toc(tic);
    printf("ConvB %f\n", result * 1e3);

    simple_tic(tic);

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

    result = simple_toc(tic);
    printf("Part %f\n", result * 1e3);

    simple_tic(tic);

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (size_t partition = 0; partition < num_threads; partition++) {
        for (size_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<size_t> tmp_C;

            size_t *M_st, M_size = 0;
            // TODO: Is it because of IM.size() -> IM_size
            if (IM_size != 0) {
                M_st = JM_arr + IM_arr[ib];
                M_size = IM_arr[ib + 1] - IM_arr[ib];
            }

            size_t *B_st = JB_arr + IB_arr[ib];
            size_t B_size = IB_arr[ib + 1] - IB_arr[ib];

            // printf("M [ ");
            // for (size_t i = 0; i < M_size; i++) {
            //     printf("%lu ", *(M_st + i));
            // }
            // printf("]  *  ");

            // printf("B [ ");
            // for (size_t i = 0; i < B_size; i++) {
            //     printf("%lu ", *(B_st + i));
            // }
            // printf("]\n");

            mxv_like_v1(tmp_C, M_st, M_size, IA_arr, IA_size, JA_arr, JA_size,
                        B_st, B_size);

            // printf("C [ ");
            // for (size_t i = 0; i < tmp_C.size(); i++) {
            //     printf("%lu ", tmp_C[i]);
            // }
            // printf("]\n");

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

        // printf("IC[%lu].size=%lu JC[%lu].size=%lu\n", partition,
        //        (*IC_size)[partition], partition, (*JC_size)[partition]);
    }

    result = simple_toc(tic);
    printf("Enum %f\n", result * 1e3);

    simple_tic(tic);
}
