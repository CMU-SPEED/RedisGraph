//------------------------------------------------------------------------------
// GB_bitmap_AxB_saxpy_generic_flipped.c: C=A*B, C bitmap/full, flipped mult
//------------------------------------------------------------------------------

// SuiteSparse:GraphBLAS, Timothy A. Davis, (c) 2017-2022, All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//------------------------------------------------------------------------------

// C is bitmap/full
// multiply op is flipped, and not positional, FIRST, or SECOND

#define GB_AXB_SAXPY_GENERIC_METHOD GB_bitmap_AxB_saxpy_generic_flipped 
#define C_IS_SPARSE_OR_HYPERSPARSE  0
#define OP_IS_POSITIONAL            0
#define FLIPXY                      1
#define OP_IS_INT64                 0
#define OP_IS_FIRSTI                0
#define OP_IS_FIRSTJ                0
#define OP_IS_FIRST                 0
#define OP_IS_SECOND                0

#include "GB_AxB_saxpy_generic_method.c"

