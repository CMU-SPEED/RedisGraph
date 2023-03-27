#include <cassert>
#include <vector>

// +, *
extern "C" void mxv_v1(std::vector<size_t> &Ic, std::vector<size_t> &Vc,
                       size_t *Im, size_t Im_size, size_t *IA, size_t IA_size,
                       size_t *JA, size_t JA_size, size_t *VA, size_t VA_size,
                       size_t *Ib, size_t Ib_size, size_t *Vb, size_t Vb_size) {
    // mxv
    // Input: A (matrix), B (column vector), M (column vector)
    // Output: C (column vector)

    // ⭐️ mxv
    if (Ib_size == 0) return;

    // Find the smallest neighborhood
    size_t smallest_v = 0;
    size_t smallest_n = IA[Ib[0] + 1] - IA[Ib[0]];
    size_t n_neighbors;
    for (size_t i = 1; i < Ib_size; i++) {
        n_neighbors = IA[Ib[i] + 1] - IA[Ib[i]];
        if (n_neighbors < smallest_n) {
            smallest_v = i;
            smallest_n = n_neighbors;
        }
    }

    if (smallest_n == 0) return;

    // ⭐️ Extract dimensions
    size_t m_nrows;
    m_nrows = Im_size;

    // 👉 Get the Ib[0]-th row vector of A as an initial c
    size_t mi = 0;
    // 👉 Check the mask
    for (size_t Ai = IA[Ib[smallest_v]]; Ai < IA[Ib[smallest_v] + 1];) {
        // m is out of bound
        if (mi >= m_nrows) {
            Ic.insert(Ic.end(), JA + Ai, JA + IA[Ib[smallest_v] + 1]);
            Vc.insert(Vc.end(), VA + Ai, VA + IA[Ib[smallest_v] + 1]);
            break;
        }

        // m > a
        else if (Im[mi] > JA[Ai]) {
            Ic.push_back(JA[Ai]);
            Vc.push_back(VA[Ai]);
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

    // 👉 *Union* with the rest
    size_t ci = 0, c_nrows = 0;
    std::vector<size_t> t, v;
    for (size_t bi = 0; bi < Ib_size; bi++) {
        if (bi == smallest_v) continue;
        assert(Ic.size() == Vc.size());
        ci = 0;
        c_nrows = Ic.size();
        for (size_t Ai = IA[Ib[bi]]; Ai < IA[Ib[bi] + 1];) {
            // c is out of bound
            if (ci >= c_nrows) {
                t.push_back(JA[Ai]);
                v.push_back(Vc[ci]);
            }

            // c < a
            if (Ic[ci] < JA[Ai]) {
                t.push_back(Ic[ci]);
                v.push_back(Vc[ci]);
                ci++;
            }

            else if (Ic[ci] == JA[Ai]) {
                t.push_back(Ic[ci]);
                v.push_back(VA[Ai]);
                ci++;
                Ai++;
            }

            else if (Ic[ci] > JA[Ai]) {
                t.push_back(JA[Ai]);
                v.push_back(JA[Ai]);
                Ai++;
            }
        }
        Ic = t;
        Vc = v;
        t.clear();
        v.clear();
    }
}