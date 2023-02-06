#include <vector>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
}

uint64_t gb_mxm_like_partition_merge(GrB_Matrix &C, GrB_Matrix &M,
                                     GrB_Matrix &A, GrB_Matrix &B);

void gb_matrix_filter(GrB_Matrix &C, GrB_Matrix M, GrB_Matrix B, GrB_Matrix A,
                      uint64_t v);

void gb_matrix_intersection(GrB_Matrix &C, GrB_Matrix M, GrB_Matrix B,
                            GrB_Matrix A, std::vector<uint64_t> &v);