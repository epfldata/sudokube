#ifndef SETTRIE_H
#define SETTRIE_H

#include <cmath>
#include <vector>
#include "common.h"
#include <map>
#include <cstdio>
#include <string>
#include <iostream>

template<typename V>
struct Node {
    int key;
    V value;  //values should double because they may exceed LONG_MAX
    size_t firstChild;
    size_t nextSibling;


    Node() : key(-1), value(-1), firstChild(-1), nextSibling(-1) {} //-1 is passed to unsigned int. TODO: Check if okay

    void set(int k, V v, size_t ns) {
        key = k;
        value = v;
        firstChild = -1;
        nextSibling = ns;
    }
};


inline size_t pupInt(const std::vector<int> bits, size_t i) {
    size_t result = 0;
    for (int b: bits) {
        int ibit = i & 0x1;
        result += (ibit << b);
        i >>= 1;
    }
    return result;
}

inline int bitsFromInt(size_t i, int keyarray[]) {
    int count = 0;
    int bit = 0;
    while (i > 0) {
        if ((i & 0x1) != 0) {
            keyarray[count++] = bit;
        }
        bit++;
        i >>= 1;
    }
    return count;
}

template<typename V>
inline void comomentTransform(std::vector<V> &values, std::vector<V> &theta) {
    size_t N = values.size();
    size_t h = 1;
    int logh = 0;
    while (h < N) {
        V p = theta[logh];
        for (size_t i = 0; i < N; i += h << 1) {
            for (size_t j = i; j < i + h; j++) {
                V first = values[j] + values[j + h];
                V second = values[j + h] - p * first;
                values[j] = first;
                values[j + h] = second;
            }
        }
        h <<= 1;
        logh++;
    }
}

template<typename V>
struct SetTrie {
    Node<V> *nodes;
    size_t count;
    size_t maxSize;

    SetTrie() : count(0), maxSize(0) {
        nodes = nullptr;
    }

    void init(size_t ms) {
        //DO NOT reset count here. Count is loaded first when loading from file.
        if (nodes) delete[] nodes;
        maxSize = ms;
        nodes = new Node<V>[maxSize];
    }

    inline Node<V> *findOrInsert(Node<V> *n, int key, V value) {
        size_t i = n->firstChild;
        Node<V> *prev = nullptr;
        Node<V> *cur = nodes + i;

        while (i != -1 && cur->key < key) {
            prev = cur;
            i = cur->nextSibling;
            cur = nodes + i;
        }
        if (i != -1 && cur->key == key) {
            return cur;
        } else {
            Node<V> *nc = nodes + count;
            nc->set(key, -1, i);
            if (prev == nullptr) {
                size_t prevfc = n->firstChild;
                n->firstChild = count;
            } else {
                prev->nextSibling = count;
            }
            count++;
            return nc;
        }
    }

    void printTree(size_t pos, FILE *fp, int level) {
        for (int i = 0; i < level; i++) fprintf(fp, "  |");
        fprintf(fp, "-- k=%d pos=%zu fc=%zu ns=%zu\n", nodes[pos].key, pos, nodes[pos].firstChild,
                nodes[pos].nextSibling);
        size_t c = nodes[pos].firstChild;
        while (c != -1 && level < 2) {
            printTree(c, fp, level + 1);
            c = nodes[c].nextSibling;
        }
    }

    void insert(const std::vector<int> &keypath, V value) {
        Node<V> *n = nodes;
        if (count < maxSize) {
            if (keypath.size() == 0) {
                if (count == 0) {
                    nodes[0].value = value;
                    count = 1;
                } else {
                    if (nodes[0].value != value) {
                        std::cerr << "NewValue " << value << "contradicts existing value " << nodes[0].value
                                  << " for total"<< std::endl;
                        throw std::runtime_error("Moment contradiction");
                    }
                }
            }
            for (const int k: keypath) {
                n = findOrInsert(n, k, value);
            }
            if (n->value != -1 && std::abs(n->value - value) >= 0.01) {
                std::cout << "NewValue " << value << "contradicts existing value " << n->value << " for path ";
                for (const int k: keypath) std::cerr << k << " ";
                std::cerr << std::endl;
                throw std::runtime_error("Moment contradiction");
            }
            if (n->value == -1)
                n->value = value;
        }
    }

    void insertAll(const std::vector<int> &cuboidDims, const std::vector<V> &values) {
        size_t cuboidDimSize = cuboidDims.size();
        int tempKey[cuboidDimSize];
        size_t i = 0;
        for (const auto value: values) {
            if (count >= maxSize) break;
            int n = bitsFromInt(i, tempKey);
            std::vector<int> key(tempKey, tempKey + n);
            for (int x = 0; x < n; x++) key[x] = cuboidDims[key[x]];
            insert(key, value);
            i++;
        }
    }


    void insertUptoDim(const std::vector<int> &cuboidDims, const std::vector<V> &values, const int maxDim) {
        size_t cuboidDimSize = cuboidDims.size();
        int tempKey[cuboidDimSize];
        size_t i = 0;
        for (const auto value: values) {
            if (count >= maxSize) break;
            int n = bitsFromInt(i, tempKey);
            if (n <= maxDim) {
                std::vector<int> key(tempKey, tempKey + n);
                for (int x = 0; x < n; x++) key[x] = cuboidDims[key[x]];
                insert(key, value);
            }
            i++;
        }
    }

    //slice factors for queryIdx .. -1/q for sv=0, 1/p for sv=1 , 1 for sv=*
    void getNormalizedSubsetForSlice(const std::vector<int> &query, std::vector<V> &result,
                                     const std::vector<V> &sliceFactors, size_t queryIdx, size_t aggIdx, size_t keyPathNumber,
                                     Node<V> *n, V sliceProduct) {
        //key path tracks ONLY the aggregation bits
        result[keyPathNumber] += n->value * sliceProduct;
        size_t childID = n->firstChild;
        size_t queryKey;
        const size_t qSize = query.size();
        while (childID != -1 && queryIdx < qSize) {
            n = nodes + childID;
            queryKey = query[queryIdx];
            if (n->key == queryKey) {
                size_t newKeyPathNumber = keyPathNumber;
                size_t newAggIdx = aggIdx;
                if(sliceFactors[queryIdx] == 1.0) {//agg dim. WARNING: this is also possible if sv=1 and p=1
                    newKeyPathNumber += (1 << aggIdx);
                    newAggIdx += 1;
                }
                getNormalizedSubsetForSlice(query, result, sliceFactors, queryIdx + 1, newAggIdx,newKeyPathNumber ,
                                            n, sliceProduct * sliceFactors[queryIdx]);
                childID = n->nextSibling;
            } else if (n->key < queryKey) {
                childID = n->nextSibling;
            } else {
                if(sliceFactors[queryIdx] == 1.0) { //agg dim
                    aggIdx++;
                }
                queryIdx++;
            }
        }
    }

    ~SetTrie() {
        if (nodes) delete[] nodes;
    }

    void saveToFile(const char *filename) {
        FILE *fp = fopen(filename, "wb");
        fprintf(fp, "%lu", count);
        int wroteElems = fwrite(nodes, sizeof(Node<V>), count, fp);
        assert(wroteElems == count);
        fclose(fp);
//        FILE *tree = fopen("treesave.txt", "w");
//        printTree(0, tree, 0);
//        fclose(tree);
    }

    void loadFromFile(const char *filename) {
        FILE *fp = fopen(filename, "rb");
        int b = fscanf(fp, "%lu", &count);
        init(count);
        int readElems = fread(nodes, sizeof(Node<V>), count, fp);
        assert(readElems == count);
        fclose(fp);

//        FILE *tree = fopen("treeload.txt", "w");
//        printTree(0, tree, 0);
//        fclose(tree);
    }
};

#endif