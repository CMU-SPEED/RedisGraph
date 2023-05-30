#include <stdint.h>

#include "../src/execution_plan/record.h"

void enumerate_subgraph(uint64_t ***out, uint64_t *out_size, uint64_t **plan,
                        GrB_Matrix A, uint64_t mode);

void _gb_mxm_like_partition_merge(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *A,
                                  GrB_Matrix *B);

void _gb_mxm_like_partition(GrB_Matrix **C, GrB_Matrix *M, GrB_Matrix *A,
                            GrB_Matrix *B);

void _gb_matrix_filter(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *B,
                       GrB_Matrix *A, uint64_t v);

void _gb_matrix_filter_v2(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *A,
                          GrB_Matrix *B, uint64_t v);

void _gb_matrix_filter_v2a(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *A,
                           GrB_Matrix *B, uint64_t v);

void mxm_like_partition_ptr(size_t ***IC, size_t **IC_size, size_t ***JC,
                            size_t **JC_size, Record **records,
                            uint64_t record_count, GrB_Matrix *A, bool **plan,
                            uint64_t plan_nnz, uint64_t current_record_size);

void mxm_like_partition_no_conv(size_t ***IC, size_t **IC_size, size_t ***JC,
                                size_t **JC_size, size_t *IM, size_t IM_size,
                                size_t *JM, size_t JM_size, size_t *IB,
                                size_t IB_size, size_t *JB, size_t JB_size,
                                GrB_Matrix *A, bool is_B_identity);

void cpp_sort(size_t *a_st, size_t *a_en);

void enumerate_subgraph_v1(uint64_t ***IC_out, uint64_t **IC_out_size,
                           uint64_t ***JC_out, uint64_t **JC_out_size,
                           uint64_t **IM_out, uint64_t *IM_out_size,
                           uint64_t **JM_out, uint64_t *JM_out_size,
                           uint64_t **plan, GrB_Matrix A);