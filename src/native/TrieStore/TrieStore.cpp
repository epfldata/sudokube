//
// Created by Sachin Basil John on 23.08.22.
//

#include "TrieStore.h"

#ifdef ROWSTORE

bool TrieStore::addDenseCuboidToTrie(RowStore &rowStore, const std::vector<int> &cuboidDims, unsigned int d_id) {
    DenseCuboid dense(rowStore.read(d_id));
    assert(sizeof(double) == sizeof(Value));
    Value *denseArray = (Value *) dense.ptr;
    std::vector<double> valueVec(dense.numRows);
    for (int i = 0; i < dense.numRows; i++) //long to double conversion;
        valueVec[i] = denseArray[i];
    std::vector<double> localprimarymoments(dense.numCols);
    for (int i = 0; i < dense.numCols; i++) {
        localprimarymoments[i] = primaryMoments[cuboidDims[i]];
    }
    comomentTransform(valueVec, localprimarymoments);
    globalSetTrie.insertUptoDim(cuboidDims, valueVec, maxDim);
    return globalSetTrie.count < globalSetTrie.maxSize;
}

bool TrieStore::addSparseCuboidToTrie(RowStore &rowStore, const std::vector<int> &cuboidDims, unsigned int s_id) {
    SparseCuboidRow sparse(rowStore.read(s_id));
    DenseCuboid dense(nullptr, sparse.numCols);
    dense.realloc();
    assert(sizeof(double) == sizeof(Value));
    double *denseArray = (double *) dense.ptr;
    for (size_t r = 0; r < sparse.numRows; r++) {
        uint64_t key64 = 0; //Assumes keySize < 8 bytes = 64 bits
        auto key = sparse.getKey(r);
        memcpy(&key64, key, sparse.keySize);
        Value value = *sparse.getVal(r);
        denseArray[key64] = value; //long to double
    }
    std::vector<double> valueVec(denseArray, denseArray + dense.numRows);
    std::vector<double> localprimarymoments(dense.numCols);
    for (int i = 0; i < dense.numCols; i++) {
        localprimarymoments[i] = primaryMoments[cuboidDims[i]];
    }
    comomentTransform(valueVec, localprimarymoments);

    globalSetTrie.insertUptoDim(cuboidDims, valueVec, maxDim);
    free(dense.ptr);
    return globalSetTrie.count < globalSetTrie.maxSize;
}

#endif

#ifdef COLSTORE

bool TrieStore::addDenseCuboidToTrie(ColumnStore &colStore, const std::vector<int> &cuboidDims, unsigned int d_id) {
    DenseCuboid dense(colStore.read(d_id));
    assert(sizeof(double) == sizeof(Value));
    Value *denseArray = (Value *) dense.ptr;
    std::vector<double> valueVec(dense.numRows);
    for (int i = 0; i < dense.numRows; i++) //long to double conversion;
        valueVec[i] = denseArray[i];
    std::vector<double> localprimarymoments(dense.numCols);
    for (int i = 0; i < dense.numCols; i++) {
        localprimarymoments[i] = primaryMoments[cuboidDims[i]];
    }
    comomentTransform(valueVec, localprimarymoments);
    globalSetTrie.insertUptoDim(cuboidDims, valueVec, maxDim);
    return globalSetTrie.count < globalSetTrie.maxSize;
}

bool TrieStore::addSparseCuboidToTrie(ColumnStore &colStore, const std::vector<int> &cuboidDims, unsigned int s_id) {
//    std::cout << "SparseCuboid" << s_id << " of dimensionality " << cuboidDims.size();
    SparseCuboidCol sparse(colStore.read(s_id));
    DenseCuboid dense(nullptr, sparse.numCols);
    dense.realloc();
    assert(sizeof(double) == sizeof(Value));
    assert(sparse.numCols < 32);
    double *denseArray = (double *) dense.ptr;
    uint64_t array[64];

//Conversion from Sparse to Dense
    for (size_t r = 0, rw = 0; r < sparse.numRows; r += 64, rw++) {
        memset(array, 0, sizeof(array));
        for (int c0 = 0; c0 < 64 && c0 < sparse.numCols; c0++) {
            memcpy(array + (63 - c0), sparse.getKey(c0, rw), sizeof(uint64_t));
        }
        ColumnStore::transpose64(array);
        for (size_t r0 = 0; r0 < 64 && r + r0 < sparse.numRows; r0++) {
            uint64_t result = array[63 - r0];
            Value value = *sparse.getVal(r + r0);
            denseArray[result] = value; //Overwriting value, there should be no duplicates
        }
    }
    std::vector<double> valueVec(denseArray, denseArray + dense.numRows);
    std::vector<double> localprimarymoments(dense.numCols);
    for (int i = 0; i < dense.numCols; i++) {
        localprimarymoments[i] = primaryMoments[cuboidDims[i]];
    }
    comomentTransform(valueVec, localprimarymoments);

    globalSetTrie.insertUptoDim(cuboidDims, valueVec, maxDim);
    free(dense.ptr);
    return globalSetTrie.count < globalSetTrie.maxSize;
}

#endif