#include <cassert>
#include <vector>

extern "C" void masked_extract_row(std::vector<size_t> &Ic, size_t *Im, size_t Im_size,
                            size_t *IA, size_t IA_size, size_t *JA,
                            size_t JA_size, size_t *Ib, size_t Ib_size) {
    // Extract 1st row and complemently mask it with a write mask
    for (size_t Ai = IA[Ib[0]], mi = 0; Ai < IA[Ib[0] + 1];) {
        // m is out of bound
        if (mi >= Im_size) {
            Ic.insert(Ic.end(), JA + Ai, JA + IA[Ib[0] + 1]);
            break;
        }
        // m > a
        else if (Im[mi] > JA[Ai]) {
            Ic.push_back(JA[Ai]);
            Ai++;
        }
        // m = a
        else if (Im[mi] == JA[Ai]) {
            mi++;
            Ai++;
        }
        // m < a
        else if (Im[mi] < JA[Ai]) {
            mi++;
        }
    }
}