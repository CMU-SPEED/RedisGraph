#include <vector>

extern "C" {
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
}

void grb_set_include(bool *z, const uint64_t *x, uint64_t i, uint64_t j,
                     const uint64_t *y);

extern "C" size_t bfs_se_template(std::vector<std::vector<size_t> > &output,
                       std::vector<std::vector<size_t> > N_P_plus, GrB_Matrix A,
                       size_t mode);