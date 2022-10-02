//
// Created by Sachin Basil John on 23.08.22.
//

#ifndef SUDOKUBECBACKEND_COMMON_H
#define SUDOKUBECBACKEND_COMMON_H

#include <inttypes.h>
#include <cstring>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <exception>

typedef unsigned char byte;
typedef int64_t Value;  //we have Java Long as value

struct RowStoreTempRec {
    //STL requires a static type. So we use maximum possible key size. We support only 64*5 = 320 bits for those functions.
    const static unsigned int rowStoreTempKeySizeWords = 5;
    uint64_t key[rowStoreTempKeySizeWords];
    Value val;

    /*
    * Returns whether k1 < k2 . Assumes the bytes are in little endian (LS byte first). Requires globalnumkeybytes to be set.
    * TODO: Check why :: If inlined, weird behaviour( no sorting happens) when run multiple times within the same sbt instance. No issue for the first time.
    */
    static bool temprec_compare_keys(const RowStoreTempRec &k1, const RowStoreTempRec &k2) {
        //DO NOT use unsigned !! i >= 0 always true
        for (short i = rowStoreTempKeySizeWords - 1; i >= 0; i--) {
            if (k1.key[i] < k2.key[i]) return true;
            if (k1.key[i] > k2.key[i]) return false;
        }
        return false;
    }

    /**
 * Compare two dynamically typed keys of the same size
 * @param numkeybytes Maximum number of bytes to be compared
 * @return true if k1 < k2 , else false
 */
    inline static bool compare_keys(uint64_t *k1, uint64_t *k2, const int numkeyWords) {
        //Warning : Do not use unsigned here. i>= 0 always true
        for (int i = numkeyWords - 1; i >= 0; i--) {
            if (k1[i] < k2[i]) return true;
            if (k1[i] > k2[i]) return false;
        }
        return false;
    }
};


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

    /**
* Returns the pointer to LSB of key at some idx in a dynamic sized array of records
* @param array Array representing sparse cuboid
* @param idx Index of record whose key is to be fetched
* @param recSize Size of the dynamic typed record (in bytes)
* @returns pointer to least significant byte of key
*/
    inline byte *getKey(size_t idx) {
        //Convert to byte* because we do byte-sized pointer arithmetic
        byte *key = (byte *) ptr + recSize * idx;
        return key;
    }

    /**
 * Returns the pointer to value at some idx in a dynamic sized array of records
 * @param array Array representing sparse cuboid
 * @param idx Index of record whose key is to be fetched
 * @param recSize Size of the dynamic typed record (in bytes)
 * @returns pointer to value
 */
    inline Value *getVal(size_t idx) {
        //Convert to byte* because we do byte-sized pointer arithmetic
        Value *val = (Value *) ((byte *) ptr + (idx + 1) * recSize - sizeof(Value));
        return val;
    }
};

struct SparseCuboidTempRecord : Cuboid {
    SparseCuboidTempRecord() : Cuboid() {}

    SparseCuboidTempRecord(void *ptr, size_t nr, unsigned int nc) : Cuboid(ptr, nr, nc, false) {}

    inline RowStoreTempRec *begin() { return (RowStoreTempRec *) ptr; }

    inline RowStoreTempRec *end() { return ((RowStoreTempRec *) ptr) + numRows; }

    void realloc() {
        if (!ptr) free(ptr);
        ptr = calloc(numRows, sizeof(RowStoreTempRec));
    }

    inline byte *getKey(size_t idx) {
        auto key = ((RowStoreTempRec *) ptr)[idx].key;
        return (byte *) key;
    }

    inline Value *getVal(size_t idx) {
        Value *val = &((RowStoreTempRec *) ptr)[idx].val;
        return val;
    }
};

struct SparseCuboidCol : Cuboid {
    size_t numRowsInWords;
    uint64_t *keyPtr;

    void realloc() {
        if (!ptr) free(ptr);
        /*
         * assumes sizeof(Value) == sizeof(uint64_t);
         * for every 64 rows, there are 64 words for values and one word per column
         */
        ptr = calloc(numRowsInWords * (64 + numCols), sizeof(uint64_t));
        keyPtr = (uint64_t *) ptr + (numRowsInWords << 6);
    }

    SparseCuboidCol() : Cuboid(), numRowsInWords(0) {}

    SparseCuboidCol(void *p, size_t nr, unsigned int nc) : Cuboid(p, nr, nc, false) {
        numRowsInWords = (nr + 63) >> 6;
        keyPtr = (uint64_t *) p + (numRowsInWords << 6);
    }

    SparseCuboidCol(Cuboid &&that) : SparseCuboidCol(that.ptr, that.numRows, that.numCols) {}

    SparseCuboidCol(Cuboid &that) : SparseCuboidCol(that.ptr, that.numRows, that.numCols) {}


    inline uint64_t *getKey(unsigned int c, size_t w) {
        return keyPtr + c * numRowsInWords + w;
    }

    inline uint64_t *getVal(size_t i) {
        return (uint64_t *) ptr + i;
    }
};
#endif //SUDOKUBECBACKEND_COMMON_H
