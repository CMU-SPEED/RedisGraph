#include <cassert>
#include <vector>

extern "C" void masked_extract_row_nc(std::vector<size_t> &Ic, size_t *Im, size_t Im_size,
                            size_t *IA, size_t IA_size, size_t *JA,
                            size_t JA_size, size_t *Ib, size_t Ib_size) {
    for (size_t Ai = IA[Ib[0]], mi = 0; Ai < IA[Ib[0] + 1] && mi < Im_size;) {
        if (Im[mi] < JA[Ai]) {
            mi++;
        } else if (Im[mi] == JA[Ai]) {
            Ic.push_back(Im[mi]);
            mi++;
            Ai++;
        } else if (Im[mi] > JA[Ai]) {
            Ai++;
        }
    }
}