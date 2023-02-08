#include <omp.h>

#include <chrono>
#include <iostream>
#include <vector>

extern "C" void mxv_like_v1(std::vector<uint64_t> &Ic, uint64_t *Im, uint64_t Im_size,
                            uint64_t *IA, uint64_t IA_size, uint64_t *JA,
                            uint64_t JA_size, uint64_t *Ib, uint64_t Ib_size);

extern "C" void mxm_like_partition_merge_indexless(
    std::vector<uint64_t> &IC, std::vector<uint64_t> &JC, uint64_t M_size,
    std::vector<uint64_t> &JM, std::vector<uint64_t> &IA, std::vector<uint64_t> &JA,
    uint64_t B_size, std::vector<uint64_t> &JB) {

    // M, B are special matrices -- all rows contain the same number of non-zero elements
    // The number of non-zero elements equals to
    //  - M = n(completed iteration)
    //  - B = n(|current plan|)
    // So, M and B can be stored as a single list each
    
    uint64_t num_threads = omp_get_max_threads();

    uint64_t *JM_arr = JM.data();
    uint64_t *JB_arr = JB.data();

    uint64_t *IA_arr = IA.data();
    uint64_t *JA_arr = JA.data();
    uint64_t IA_size = IA.size();
    uint64_t JA_size = JA.size();

    // Create partitions
    std::vector<std::vector<uint64_t> *> partitioned_IC(num_threads);
    std::vector<std::vector<uint64_t> *> partitioned_JC(num_threads);

    // Compute for finding each partition's size
    uint64_t num_B_rows = JB.size() / B_size;

    uint64_t partition_size = (num_B_rows - 1) / num_threads;
    std::vector<uint64_t> partition_offset(num_threads + 1);
    partition_offset[0] = 0;
    partition_offset[1] = partition_size + ((num_B_rows - 1) % num_threads);
    partitioned_IC[0] = new std::vector<uint64_t>();
    partitioned_JC[0] = new std::vector<uint64_t>();

    for (uint64_t t = 1; t < num_threads; t++) {
        partition_offset[t + 1] = partition_offset[t] + partition_size;
        partitioned_IC[t] = new std::vector<uint64_t>();
        partitioned_JC[t] = new std::vector<uint64_t>();
    }

// Loop for each row vector in B
#pragma omp parallel for num_threads(num_threads)
    for (uint64_t partition = 0; partition < num_threads; partition++) {
        for (uint64_t ib = partition_offset[partition];
             ib < partition_offset[partition + 1]; ib++) {
            std::vector<uint64_t> tmp_C;

            uint64_t *M_st = JM_arr + M_size;
            uint64_t *B_st = JB_arr + B_size;

            mxv_like_v1(tmp_C, M_st, M_size, IA_arr, IA_size, JA_arr, JA_size,
                        B_st, B_size);

            // FIXME: We should not copy (unnecessary operations)
            partitioned_JC[partition]->insert(partitioned_JC[partition]->end(),
                                              tmp_C.begin(), tmp_C.end());
            partitioned_IC[partition]->push_back(
                partitioned_JC[partition]->size());
        }
    }

    // Merge
    IC.push_back(0);
    for (uint64_t partition = 0; partition < num_threads; partition++) {
        // Update JC
        JC.insert(JC.end(), partitioned_JC[partition]->begin(),
                  partitioned_JC[partition]->end());
        delete partitioned_JC[partition];

        // Update IC
        uint64_t prev_cap = IC.back();
        for (uint64_t i = 0; i < partitioned_IC[partition]->size(); i++) {
            partitioned_IC[partition]->at(i) += prev_cap;
        }
        IC.insert(IC.end(), partitioned_IC[partition]->begin(),
                  partitioned_IC[partition]->end());
        delete partitioned_IC[partition];
    }
}
