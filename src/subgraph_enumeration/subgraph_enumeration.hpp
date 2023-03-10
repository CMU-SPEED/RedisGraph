#include <stdint.h>

void enumerate_subgraph(uint64_t ***out, uint64_t *out_size, uint64_t **plan,
                        GrB_Matrix A, uint64_t mode);

void _gb_mxm_like_partition_merge(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *A,
                                  GrB_Matrix *B);

void _gb_matrix_filter(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *B,
                                 GrB_Matrix *A, uint64_t v);

// void _gb_matrix_intersection(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *B,
//                                  GrB_Matrix *A, uint64_t v);