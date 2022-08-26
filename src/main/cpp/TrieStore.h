//
// Created by Sachin Basil John on 23.08.22.
//

#ifndef SUDOKUBECBACKEND_TRIESTORE_H
#define SUDOKUBECBACKEND_TRIESTORE_H
#include "common.h"
#include "SetTrie.h"

struct TrieStore {
    SetTrie globalSetTrie;
    bool addDenseCuboidToTrie(const vector<int> &cuboidDims, unsigned int d_id);
    bool addSparseCuboidToTrie(const vector<int> &cuboidDims, unsigned int s_id);
    void initTrie(size_t maxsize) { globalSetTrie.init(maxsize); }

    void saveTrie(const char *filename) { globalSetTrie.saveToFile(filename); }

    void loadTrie(const char *filename) { globalSetTrie.loadFromFile(filename); }

    void prepareFromTrie(const vector<int> &query, map<int, value_t> &result) {
        globalSetTrie.getNormalizedSubset(query, result, 0, 0, globalSetTrie.nodes);
    }
};


#endif //SUDOKUBECBACKEND_TRIESTORE_H
