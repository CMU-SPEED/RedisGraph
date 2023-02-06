#include <omp.h>

#include "mxm_like.hpp"
#include "mxv_like.hpp"

#define START_TIMING ;
// start = std::chrono::duration_cast<std::chrono::microseconds>(   \
    //             std::chrono::system_clock::now().time_since_epoch()) \
    //             .count();

#define STOP_TIMING ;
// stop = std::chrono::duration_cast<std::chrono::microseconds>(   \
    //            std::chrono::system_clock::now().time_since_epoch()) \
    //            .count();                                            \
    // std::cout << "," << (stop - start) / 1000.0;

void mxm_partition_merge(std::vector<size_t> &IC, std::vector<size_t> &JC,
                         std::vector<size_t> &IM, std::vector<size_t> &JM,
                         std::vector<size_t> &IA, std::vector<size_t> &JA,
                         std::vector<size_t> &IB, std::vector<size_t> &JB) {
    size_t start, stop;

    START_TIMING;

    size_t num_threads = omp_get_max_threads();

    size_t *IM_arr = IM.data();
    size_t *JM_arr = JM.data();
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

    STOP_TIMING;

    START_TIMING;

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (size_t partition = 0; partition < num_threads; partition++) {
        for (size_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<size_t> tmp_C;

            size_t *M_st = JM_arr + IM_arr[ib];
            size_t *B_st = JB_arr + IB_arr[ib];
            size_t M_size = IM_arr[ib + 1] - IM_arr[ib];
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

    STOP_TIMING;

    START_TIMING;

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

    STOP_TIMING;
}
