#include <cassert>
#include <vector>

extern "C" void mxv_like_v2(std::vector<size_t> &Ic, size_t *Im, size_t Im_size,
                            size_t *IA, size_t IA_size, size_t *JA,
                            size_t JA_size, size_t *Ib, size_t Ib_size) {
    // Ignore when |b| = 0
    if (Ib_size == 0) return;
    // Rename Im_size
    size_t m_nrows = Im_size;
    // Get the Ib[0]-th row vector of A as an initial c
    size_t mi = 0;
    // Extract 1st row and combine it with a write mask
    for (size_t Ai = IA[Ib[0]]; Ai < IA[Ib[0] + 1];) {
        // m is out of bound
        if (mi >= m_nrows) {
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

    // Intersect with the rest
    size_t ci = 0, c_nrows = 0;
    std::vector<size_t> t;
    for (size_t bi = 1; bi < Ib_size; bi++) {
        ci = 0;
        c_nrows = Ic.size();
        for (size_t Ai = IA[Ib[bi]]; Ai < IA[Ib[bi] + 1] && ci < c_nrows;) {
            if (Ic[ci] < JA[Ai]) {
                ci++;
            } else if (Ic[ci] == JA[Ai]) {
                t.push_back(Ic[ci]);
                ci++;
                Ai++;
            } else if (Ic[ci] > JA[Ai]) {
                Ai++;
            }
        }
        Ic = t;
        t.clear();
    }
}