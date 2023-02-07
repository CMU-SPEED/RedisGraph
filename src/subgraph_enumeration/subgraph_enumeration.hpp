#include <stdint.h>

void enumerate_subgraph(uint64_t ***out, uint64_t *out_size, uint64_t **plan, GrB_Matrix A,
                        uint64_t mode);