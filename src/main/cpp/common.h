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

struct SparseCuboidRow : Cuboid {
    uint16_t keySize;
    uint16_t recSize;

    static inline uint16_t bitsToBytes(uint16_t nc) { return (nc + 8) >> 3; }  //TODO: Change 8 to 7
    void realloc() {
        if (!ptr) free(ptr);
        ptr = calloc(numRows, recSize);
    }

    SparseCuboidRow() : Cuboid(), keySize(0), recSize(0) {}

    SparseCuboidRow(Cuboid &&that) : SparseCuboidRow(that.ptr, that.numRows, that.numCols) {}

    SparseCuboidRow(Cuboid &that) : SparseCuboidRow(that.ptr, that.numRows, that.numCols) {}

    SparseCuboidRow(void *p, size_t nr, uint16_t nc) : Cuboid(p, nr, nc, false) {
        keySize = bitsToBytes(numCols);
        recSize = keySize + sizeof(Value);
    }
};

#endif //SUDOKUBECBACKEND_COMMON_H
