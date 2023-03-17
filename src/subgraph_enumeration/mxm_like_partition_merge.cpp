#include <omp.h>
#include <stdio.h>

#include <chrono>
#include <iostream>
#include <vector>

extern "C" {
#include "../util/simple_timer.h"
}

extern "C" void mxv_like_v1(std::vector<size_t> &Ic, size_t *Im, size_t Im_size,
                            size_t *IA, size_t IA_size, size_t *JA,
                            size_t JA_size, size_t *Ib, size_t Ib_size);

extern "C" void mxm_like_partition_merge(
    std::vector<size_t> &IC, std::vector<size_t> &JC, std::vector<size_t> &IM,
    std::vector<size_t> &JM, std::vector<size_t> &IA, std::vector<size_t> &JA,
    std::vector<size_t> &IB, std::vector<size_t> &JB) {
    size_t num_threads = omp_get_max_threads();
    // printf("mxm_like_partition_merge::num_threads = %d\n", num_threads);

    double result = 0.0;
    double tic[2];
    simple_tic(tic);

    size_t *IM_arr, *JM_arr;
    if (IM.size() != 0) {
        IM_arr = IM.data();
        JM_arr = JM.data();
    }

    size_t *IA_arr = IA.data();
    size_t *JA_arr = JA.data();
    size_t *IB_arr = IB.data();
    size_t *JB_arr = JB.data();

    size_t IA_size = IA.size();
    size_t JA_size = JA.size();

    // Create partitions
    std::vector<std::vector<size_t> *> partitioned_IC(num_threads);
    std::vector<std::vector<size_t> *> partitioned_JC(num_threads);

    size_t partition_size = (IB.size() - 1) / num_threads;
    std::vector<size_t> partition_offset(num_threads + 1);
    partition_offset[0] = 0;
    partition_offset[1] = partition_size + ((IB.size() - 1) % num_threads);
    partitioned_IC[0] = new std::vector<size_t>();
    partitioned_JC[0] = new std::vector<size_t>();

    for (size_t t = 1; t < num_threads; t++) {
        partition_offset[t + 1] = partition_offset[t] + partition_size;
        partitioned_IC[t] = new std::vector<size_t>();
        partitioned_JC[t] = new std::vector<size_t>();
    }

    result = simple_toc(tic);
    printf("Part %f\n", result * 1e3);

    simple_tic(tic);

    size_t IM_size = IM.size();

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (size_t partition = 0; partition < num_threads; partition++) {
        for (size_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<size_t> tmp_C;

            size_t *M_st, M_size = 0;
            if (IM_size != 0) {
                M_st = JM_arr + IM_arr[ib];
                M_size = IM_arr[ib + 1] - IM_arr[ib];
            }

            size_t *B_st = JB_arr + IB_arr[ib];
            size_t B_size = IB_arr[ib + 1] - IB_arr[ib];

            mxv_like_v1(tmp_C, M_st, M_size, IA_arr, IA_size, JA_arr, JA_size,
                        B_st, B_size);

            // FIXME: We should not copy (unnecessary operations)
            partitioned_JC[partition]->insert(partitioned_JC[partition]->end(),
                                              tmp_C.begin(), tmp_C.end());
            partitioned_IC[partition]->push_back(
                partitioned_JC[partition]->size());
        }
    }

    result = simple_toc(tic);
    printf("Enum %f\n", result * 1e3);

    simple_tic(tic);

    // Merge
    IC.push_back(0);
    for (size_t partition = 0; partition < num_threads; partition++) {
        // Update JC
        JC.insert(JC.end(), partitioned_JC[partition]->begin(),
                  partitioned_JC[partition]->end());
        delete partitioned_JC[partition];

        // Update IC
        size_t prev_cap = IC.back();
        for (size_t i = 0; i < partitioned_IC[partition]->size(); i++) {
            partitioned_IC[partition]->at(i) += prev_cap;
        }
        IC.insert(IC.end(), partitioned_IC[partition]->begin(),
                  partitioned_IC[partition]->end());
        delete partitioned_IC[partition];
    }

    result = simple_toc(tic);
    printf("Merge %f\n", result * 1e3);
}

// extern "C" void mxm_like_partition_merge(std::vector<size_t> &IC,
// std::vector<size_t> &JC,
//                               std::vector<size_t> &IM, std::vector<size_t>
//                               &JM, std::vector<size_t> &IA,
//                               std::vector<size_t> &JA, std::vector<size_t>
//                               &IB, std::vector<size_t> &JB) {
//     size_t num_threads = omp_get_max_threads();

//     size_t *IM_arr = IM.data();
//     size_t *JM_arr = JM.data();
//     size_t *IA_arr = IA.data();
//     size_t *JA_arr = JA.data();
//     size_t *IB_arr = IB.data();
//     size_t *JB_arr = JB.data();

//     size_t IA_size = IA.size();
//     size_t JA_size = JA.size();

//     // Create partitions
//     std::vector<std::vector<size_t> *> partitioned_IC(num_threads);
//     std::vector<std::vector<size_t> *> partitioned_JC(num_threads);

//     size_t partition_size = (IB.size() - 1) / num_threads;
//     std::vector<size_t> partition_offset(num_threads + 1);
//     partition_offset[0] = 0;
//     partition_offset[1] = partition_size + ((IB.size() - 1) % num_threads);
//     partitioned_IC[0] = new std::vector<size_t>();
//     partitioned_JC[0] = new std::vector<size_t>();

//     for (size_t t = 1; t < num_threads; t++) {
//         partition_offset[t + 1] = partition_offset[t] + partition_size;
//         partitioned_IC[t] = new std::vector<size_t>();
//         partitioned_JC[t] = new std::vector<size_t>();
//     }

// // Loop for each row vector in B
// #pragma omp parallel for num_threads(num_threads)
//     for (size_t partition = 0; partition < num_threads; partition++) {
//         for (size_t ib = partition_offset[partition];
//              ib < partition_offset[partition + 1]; ib++) {
//             std::vector<size_t> tmp_C;

//             size_t *M_st = JM_arr + IM_arr[ib];
//             size_t *B_st = JB_arr + IB_arr[ib];
//             size_t M_size = IM_arr[ib + 1] - IM_arr[ib];
//             size_t B_size = IB_arr[ib + 1] - IB_arr[ib];

//             mxv_like_v1(tmp_C, M_st, M_size, IA_arr, IA_size, JA_arr,
//             JA_size,
//                         B_st, B_size);

//             // FIXME: We should not copy (unnecessary operations)
//             partitioned_JC[partition]->insert(partitioned_JC[partition]->end(),
//                                               tmp_C.begin(), tmp_C.end());

//             partitioned_IC[partition]->push_back(
//                 partitioned_JC[partition]->size());
//         }
//     }

//     // Merge
//     IC.push_back(0);
//     for (size_t partition = 0; partition < num_threads; partition++) {
//         // Update JC
//         JC.insert(JC.end(), partitioned_JC[partition]->begin(),
//                   partitioned_JC[partition]->end());
//         delete partitioned_JC[partition];

//         // Update IC
//         size_t prev_cap = IC.back();
//         for (size_t i = 0; i < partitioned_IC[partition]->size(); i++) {
//             partitioned_IC[partition]->at(i) += prev_cap;
//         }
//         IC.insert(IC.end(), partitioned_IC[partition]->begin(),
//                   partitioned_IC[partition]->end());
//         delete partitioned_IC[partition];
//     }
// }
