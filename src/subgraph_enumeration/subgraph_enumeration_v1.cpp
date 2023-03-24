#include <omp.h>
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

extern "C" void mxm_like_partition_no_conv(
    size_t ***IC, size_t **IC_size, size_t ***JC, size_t **JC_size, size_t *IM,
    size_t IM_size, size_t *JM, size_t JM_size, size_t *IB, size_t IB_size,
    size_t *JB, size_t JB_size, GrB_Matrix *A);

extern "C" void enumerate_subgraph_v1(
    uint64_t ***IC_out, uint64_t **IC_out_size, uint64_t ***JC_out,
    uint64_t **JC_out_size, uint64_t **IM_out, uint64_t *IM_out_size,
    uint64_t **JM_out, uint64_t *JM_out_size, uint64_t **plan, GrB_Matrix A) {
    GrB_Info info;
    size_t num_threads = omp_get_max_threads();

    // Rebuild qplan as STL/C++ vector
    std::vector<std::vector<uint64_t>> qplan(array_len(plan));
    for (uint64_t i = 0; i < array_len(plan); i++) {
        for (uint64_t j = 0; j < array_len(plan[i]); j++) {
            qplan[i].push_back(plan[i][j]);
        }
    }

    GrB_Index nV_P = qplan.size();
    GrB_Index nV_G;
    GrB_Matrix_nrows(&nV_G, A);

    // Generate initial M
    std::vector<size_t> *IM, *JM;
    IM = new std::vector<size_t>(nV_G + 1);
    JM = new std::vector<size_t>(nV_G);
    (*IM)[0] = 0;
    for (size_t i = 0; i < nV_G; i++) {
        (*IM)[i + 1] = i + 1;
        (*JM)[i] = i;
    }

    size_t **IC = NULL, *IC_size = NULL, **JC = NULL, *JC_size = NULL;

    // Do MxM-like and materialization
    for (size_t iter = 2; iter <= nV_P; iter++) {
        // Select
        size_t nnz_plan = qplan[iter - 1].size();
        std::vector<bool> plan_bitmap(nV_P, false);
        for (size_t i = 0; i < nnz_plan; i++) {
            plan_bitmap[qplan[iter - 1][i] - 1] = true;
        }

        std::vector<size_t> IB(IM->size()), JB((IM->size() - 1) * nnz_plan);
#pragma omp parallel for num_threads(num_threads)
        for (size_t i = 0; i < IM->size(); i++) {
            IB[i] = i * nnz_plan;
        }

#pragma omp parallel for num_threads(num_threads)
        for (size_t iM = 0; iM < IM->size() - 1; iM++) {
            for (size_t jM = (*IM)[iM], v = 0, idx = 0; jM < (*IM)[iM + 1];
                 jM++, idx++) {
                if (plan_bitmap[idx]) {
                    JB[(iM * nnz_plan) + v] = (*JM)[jM];
                    v++;
                }
            }
        }

        // MxM-like
        if (IC != NULL) {
            for (size_t i = 0; i < num_threads; i++) {
                free(IC[i]);
            }
            free(IC);
        }
        IC = (size_t **)malloc(sizeof(size_t *) * num_threads);
        if (IC == NULL) {
            return;
        }

        if (IC_size != NULL) {
            free(IC_size);
        }
        IC_size = (size_t *)malloc(sizeof(size_t) * num_threads);
        if (IC_size == NULL) {
            return;
        }

        if (JC != NULL) {
            for (size_t i = 0; i < num_threads; i++) {
                free(JC[i]);
            }
            free(JC);
        }
        JC = (size_t **)malloc(sizeof(size_t *) * num_threads);
        if (JC == NULL) {
            return;
        }

        if (JC_size != NULL) {
            free(JC_size);
        }
        JC_size = (size_t *)malloc(sizeof(size_t) * num_threads);
        if (JC_size == NULL) {
            return;
        }

        mxm_like_partition_no_conv(
            &IC, &IC_size, &JC, &JC_size, IM->data(), IM->size(), JM->data(),
            JM->size(), IB.data(), IB.size(), JB.data(), JB.size(), &A);

        if (iter < nV_P) {
            // Materialize (and implicitly merge) MxM results into M_new
            // Compute nR(M_new)
            std::vector<size_t> cumulative_IC(num_threads + 1);
            cumulative_IC[0] = 0;
            for (size_t i = 0; i < num_threads; i++) {
                cumulative_IC[i + 1] = cumulative_IC[i] + (IC_size[i] - 1);
            }
            std::vector<size_t> cumulative_JC(num_threads + 1);
            cumulative_JC[0] = 0;
            for (size_t i = 0; i < num_threads; i++) {
                cumulative_JC[i + 1] = cumulative_JC[i] + JC_size[i];
            }

            size_t nR_M_new = cumulative_JC[num_threads];
            std::vector<size_t> *IM_new, *JM_new;
            IM_new = new std::vector<size_t>(nR_M_new + 1);
            JM_new = new std::vector<size_t>(nR_M_new * iter);

            (*IM_new)[0] = 0;
#pragma omp parallel for num_threads(num_threads)
            for (size_t i = 0; i < nR_M_new; i++) {
                (*IM_new)[i + 1] = (i + 1) * iter;
            }

#pragma omp parallel for num_threads(num_threads)
            for (size_t i = 0; i < num_threads; i++) {
                size_t *local_IC = IC[i];
                size_t *local_JC = JC[i];
                size_t local_IC_size = IC_size[i];
                size_t iM_new = 0;
                // Loop each row in local C
                for (size_t iC = 0; iC < local_IC_size - 1; iC++) {
                    // Loop for each element in the row
                    for (size_t jC = local_IC[iC]; jC < local_IC[iC + 1];
                         jC++) {
                        // Copy a row from M whose row ID equals C's row ID
                        // M's row id = offset ()
                        size_t iM = iC + cumulative_IC[i];
                        size_t iM_new = jC + cumulative_JC[i];
                        size_t cnt = 0;
                        for (size_t jM = (*IM)[iM]; jM < (*IM)[iM + 1]; jM++) {
                            (*JM_new)[iM_new * iter + cnt] = (*JM)[jM];
                            cnt++;
                        }
                        // Add candidate
                        (*JM_new)[iM_new * iter + cnt] = local_JC[jC];
                    }
                }
            }

            delete IM;
            delete JM;
            IM = IM_new;
            JM = JM_new;
        }
    }

    *IC_out = IC;
    *IC_out_size = IC_size;
    *JC_out = JC;
    *JC_out_size = JC_size;
    *IM_out = IM->data();
    *IM_out_size = IM->size();
    *JM_out = JM->data();
    *JM_out_size = JM->size();
}