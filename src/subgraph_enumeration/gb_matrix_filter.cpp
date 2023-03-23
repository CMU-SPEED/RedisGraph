#include <cassert>
#include <iostream>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../util/simple_timer.h"
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

    double result = 0.0;
    double tic[2];

    printf("(GraphBLAS CN) Binaralization: ");
    simple_tic(tic);
    {
        info = GrB_Matrix_select_UINT64(S, NULL, NULL, GrB_VALUEGT_UINT64, B, 0,
                                        NULL);
        assert(info == GrB_SUCCESS);
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    printf("(GraphBLAS CN) MxM: ");
    simple_tic(tic);
    {
        info = GrB_mxm(C, M, NULL, GrB_PLUS_TIMES_SEMIRING_UINT64, B, A,
                       GrB_DESC_C);
        assert(info == GrB_SUCCESS);
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    printf("(GraphBLAS CN) Filter: ");
    simple_tic(tic);
    {
        info = GrB_Matrix_select_UINT64(C, NULL, NULL, GrB_VALUEEQ_UINT64, C, v,
                                        NULL);
        assert(info == GrB_SUCCESS);
    }
    result = simple_toc(tic);
    printf("%f ms\n", result * 1e3);

    info = GrB_Matrix_free(&S);
    assert(info == GrB_SUCCESS);
}

extern "C" void _gb_matrix_filter(GrB_Matrix *C, GrB_Matrix *M, GrB_Matrix *A,
                                  GrB_Matrix *B, uint64_t v) {
    gb_matrix_filter(*C, *M, *A, *B, v);
}