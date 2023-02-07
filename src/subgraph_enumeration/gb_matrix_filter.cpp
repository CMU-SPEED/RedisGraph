#include <cassert>
#include <iostream>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
}

extern "C" void gb_matrix_filter(GrB_Matrix &C, GrB_Matrix M, GrB_Matrix B,
                                 GrB_Matrix A, uint64_t v) {
    GrB_Info info;

    uint64_t nrows, ncols;
    info = GrB_Matrix_nrows(&nrows, B);
    assert(info == GrB_SUCCESS);
    info = GrB_Matrix_ncols(&ncols, B);
    assert(info == GrB_SUCCESS);
    GrB_Matrix S;
    info = GrB_Matrix_new(&S, GrB_BOOL, nrows, ncols);
    assert(info == GrB_SUCCESS);
    info = GrB_Matrix_new(&C, GrB_UINT64, nrows, ncols);
    assert(info == GrB_SUCCESS);

    info =
        GrB_Matrix_select_UINT64(S, NULL, NULL, GrB_VALUEGT_UINT64, B, 0, NULL);
    assert(info == GrB_SUCCESS);
    info =
        GrB_mxm(C, M, NULL, GrB_PLUS_TIMES_SEMIRING_UINT64, S, A, GrB_DESC_C);
    assert(info == GrB_SUCCESS);
    info =
        GrB_Matrix_select_UINT64(C, NULL, NULL, GrB_VALUEEQ_UINT64, C, v, NULL);
    assert(info == GrB_SUCCESS);

    info = GrB_Matrix_free(&S);
    assert(info == GrB_SUCCESS);
}
