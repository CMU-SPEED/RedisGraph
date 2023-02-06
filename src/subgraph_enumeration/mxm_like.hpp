#include <vector>

extern "C" void mxm_partition_merge(
    std::vector<size_t> &IC, std::vector<size_t> &JC, std::vector<size_t> &IM,
    std::vector<size_t> &JM, std::vector<size_t> &IA, std::vector<size_t> &JA,
    std::vector<size_t> &IB, std::vector<size_t> &JB);