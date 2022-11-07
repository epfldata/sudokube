//
// Created by Sachin Basil John on 04.11.22.
//

#ifndef SUDOKUBECBACKEND_MOMENTS_H
#define SUDOKUBECBACKEND_MOMENTS_H

#include <inttypes.h>
#include <cstdlib>
#include "RowStore/RowStore.h"

#define KEY_SIZE_WORDS 14
#define VALUE_SIZE_WORDS 2
#define REC_SIZE_WORDS 16

#define TILE_SIZE 32
#define FACTOR 4
#define RECS_IN_TILE (TILE_SIZE /REC_SIZE_WORDS)

typedef unsigned T;

#define CORES_PER_BLOCK_ALONG_X 8
#define CORES_PER_BLOCK (CORES_PER_BLOCK_ALONG_X * REC_SIZE_WORDS)
#define ENTRY_PER_CORE 2
#define ENTRIES_IN_BLOCK (CORES_PER_BLOCK * ENTRY_PER_CORE)

struct TypedCuboid {
    int *ptr;
    size_t numRows;

    void realloc() {
        if (!ptr) free(ptr);
        assert(sizeof(T) <= VALUE_SIZE_WORDS * sizeof(int));
        ptr = (int *) calloc(numRows, REC_SIZE_WORDS * sizeof(int));
    }

    TypedCuboid(SparseCuboidRow &that) : ptr(nullptr), numRows(that.numRows) {
//        numRows = std::min<size_t>(4.18 * 1000 * 1000UL, that.numRows);
        assert(that.numCols <= KEY_SIZE_WORDS * 32);
        realloc();
        for (size_t i = 0; i < numRows; i++) {
            memcpy(getKey(i), that.getKey(i), KEY_SIZE_WORDS * sizeof(int));
            *getVal(i) = *that.getVal(i);
        }
    }

    TypedCuboid() : ptr(nullptr), numRows(0) {}

    TypedCuboid(int *p, size_t nr, uint16_t nc) : ptr(p), numRows(nr) {
        assert(nc <= KEY_SIZE_WORDS * 32);
    }

    inline int *getKey(size_t idx) {
        //Convert to int* because we do int-sized pointer arithmetic
        int *key =  ptr + REC_SIZE_WORDS * idx;
        return key;
    }

    inline T *getVal(size_t idx) {
        //Convert to int* because we do int-sized pointer arithmetic
        T *val = (T *) (ptr + REC_SIZE_WORDS * idx + KEY_SIZE_WORDS);
        return val;
    }
};

#endif //SUDOKUBECBACKEND_MOMENTS_H
