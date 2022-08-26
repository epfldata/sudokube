//
// Created by Sachin Basil John on 23.08.22.
//

#include "TrieStore.h"

bool TrieStore::addDenseCuboidToTrie(const vector<int> &cuboidDims, unsigned int d_id) {
    cout << "DenseCuboid " << d_id << " of dimensionality " << cuboidDims.size();
    size_t len;
    value_t *values = fetch(d_id, len);
    vector<value_t> valueVec(values, values + len);
    momentTransform(valueVec);
    globalSetTrie.insertAll(cuboidDims, valueVec);
    cout << "  TrieCount = " << globalSetTrie.count << endl;
    return globalSetTrie.count < globalSetTrie.maxSize;
}

bool TrieStore::addSparseCuboidToTrie(const vector<int> &cuboidDims, unsigned int s_id) {
    cout << "SparseCuboid" << s_id << " of dimensionality " << cuboidDims.size();
    size_t rows;
    void *ptr;
    unsigned short keySize;
    globalRegistry.read(s_id, ptr, rows, keySize);
    unsigned int recSize = keySize + sizeof(value_t);
    byte **store = (byte **) ptr;

    size_t newsize = 1LL << cuboidDims.size();
    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);

    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehash2 calloc : %lu MB\n", numMB);
    memset(newstore, 0, sizeof(value_t) * newsize);

    for (size_t r = 0; r < rows; r++) {
        size_t i = from_Key_to_Long(keySize, getKey(store, r, recSize));
        newstore[i] += *getVal(store, r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
    }

    vector<value_t> valueVec(newstore, newstore + newsize);
    momentTransform(valueVec);

    globalSetTrie.insertAll(cuboidDims, valueVec);
    cout << "  TrieCount = " << globalSetTrie.count << endl;
    free(newstore);
    return globalSetTrie.count < globalSetTrie.maxSize;
}