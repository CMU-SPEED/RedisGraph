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

void mxm_like_partition_ptr(size_t ***IC, size_t **IC_size, size_t ***JC,
                            size_t **JC_size, Record **records,
                            uint64_t record_count, GrB_Matrix *A, bool **plan,
                            uint64_t plan_nnz, uint64_t current_record_size);
