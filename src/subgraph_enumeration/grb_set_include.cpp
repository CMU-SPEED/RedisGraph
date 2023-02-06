#include <algorithm>

#include "subgraph_enumeration.hpp"

void grb_set_include(bool *z, const uint64_t *x, uint64_t i, uint64_t j,
                     const uint64_t *y) {
    std::vector<size_t> set = *((std::vector<size_t> *)*y);
    *z = std::find(set.begin(), set.end(), *x) != set.end();
}