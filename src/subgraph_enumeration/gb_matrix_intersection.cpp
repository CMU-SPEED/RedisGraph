#include <cassert>
#include <vector>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
}

extern "C" void gb_matrix_intersection(GrB_Matrix &C, GrB_Matrix M,
                                       GrB_Matrix B, GrB_Matrix A,
                                       std::vector<uint64_t> &v) {
    GrB_Info info;

    uint64_t nrows, ncols;
    info = GrB_Matrix_nrows(&nrows, B);
    assert(info == GrB_SUCCESS);
    info = GrB_Matrix_ncols(&ncols, B);
    assert(info == GrB_SUCCESS);
    info = GrB_Matrix_new(&C, GrB_BOOL, nrows, ncols);
    assert(info == GrB_SUCCESS);

    if (v.size() == 0) return;

    GrB_Matrix S;
    info = GrB_Matrix_new(&S, GrB_BOOL, nrows, ncols);
    assert(info == GrB_SUCCESS);
    info = GrB_Matrix_select_UINT64(S, NULL, NULL, GrB_VALUEEQ_UINT64, B, v[0],
                                    NULL);
    assert(info == GrB_SUCCESS);

    if (M == GrB_NULL) {
        info = GrB_mxm(C, GrB_NULL, GrB_NULL, GrB_LOR_LAND_SEMIRING_BOOL, S, A,
                       GrB_NULL);
        assert(info == GrB_SUCCESS);
    }

    else {
        info =
            GrB_mxm(C, M, NULL, GrB_LOR_LAND_SEMIRING_BOOL, S, A, GrB_DESC_C);
        assert(info == GrB_SUCCESS);
    }

    for (size_t j = 1; j < v.size(); j++) {
        info = GrB_Matrix_select_UINT64(S, NULL, NULL, GrB_VALUEEQ_UINT64, B,
                                        v[j], NULL);
        assert(info == GrB_SUCCESS);
        info = GrB_mxm(C, C, NULL, GrB_LOR_LAND_SEMIRING_BOOL, S, A, NULL);
        assert(info == GrB_SUCCESS);
    }

    info = GrB_Matrix_free(&S);
    assert(info == GrB_SUCCESS);
}

// extern "C" void _gb_matrix_intersection(GrB_Matrix *C, GrB_Matrix *M,
//                                              GrB_Matrix *A, GrB_Matrix *B) {
//     gb_matrix_intersection(*C, *M, *A, *B);
// }