//
// Created by Sachin Basil John on 23.08.22.
//

#ifndef SUDOKUBECBACKEND_COMMON_H
#define SUDOKUBECBACKEND_COMMON_H

#include <inttypes.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <exception>

typedef unsigned char byte;
typedef int64_t Value;  //we have Java Long as value

struct Cuboid {
    void *ptr;
    size_t numRows;
    uint16_t numCols;
    bool isDense;

    Cuboid() : ptr(nullptr), numRows(0), numCols(0), isDense(false) {}

    Cuboid(void *p, size_t nr, uint16_t nc, bool isd) : ptr(p), numRows(nr), numCols(nc), isDense(isd) {}
};

struct DenseCuboid : Cuboid {
    DenseCuboid() : Cuboid() {}

    void realloc() {
        if (!ptr) free(ptr);
        ptr = calloc(numRows, sizeof(Value));
    }

    DenseCuboid(Cuboid &&that) : DenseCuboid(that.ptr, that.numCols) {}
    DenseCuboid(void *p, uint16_t nc) : Cuboid(p, 1uL << nc, nc, true) {}
};


#endif //SUDOKUBECBACKEND_COMMON_H
