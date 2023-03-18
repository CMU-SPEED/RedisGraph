#include <algorithm>

extern "C" void cpp_sort(size_t **a_st, size_t **a_en) {
    std::sort(*a_st, *a_en);
}