#ifndef SETTRIE_H
#define SETTRIE_H

#include <cmath>
#include <vector>
#include "Keys.h"
#include <map>
#include <cstdio>
#include <string>
#include <iostream>



struct Node {
    int key;
    value_t value;  //values are double because they may exceed LONG_MAX
    size_t firstChild;
    size_t nextSibling;


    Node() : key(-1), value(-1), firstChild(-1), nextSibling(-1) {} //-1 is passed to unsigned int. TODO: Check if okay

    void set(int k, value_t v, size_t ns) {
        key = k;
        value = v;
        firstChild = -1;
        ns = nextSibling;
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

inline void momentTransform(std::vector<value_t> &values) {
    size_t N = values.size();
    size_t h = 1;
    while (h < N) {
        for (size_t i = 0; i < N; i += h * 2) {
            for (size_t j = i; j < i + h; j++) {
                values[j] += values[j + h];
            }
        }
        h *= 2;
    }
}

struct SetTrie {
    Node *nodes;
    size_t count;
    size_t maxSize;

    SetTrie() : count(0), maxSize(0) {
        nodes = nullptr;
    }

    void init(size_t ms) {
        if (nodes) delete[] nodes;
        maxSize = ms;
        nodes = new Node[maxSize];
    }

    inline Node *findOrInsert(Node *n, int key, value_t value) {
        size_t i = n->firstChild;
        Node *prev = nullptr;
        Node *cur = nodes + i;

        while (i != -1 && cur->key < key) {
            prev = cur;
            i = cur->nextSibling;
            cur = nodes + i;
        }
        if (i != -1 && cur->key == key) {
            return cur;
        } else {
            Node *nc = nodes + count;
            nc->set(key, value, i);
            if (prev == nullptr) {
                n->firstChild = count;
            } else {
                prev->nextSibling = count;
            }
            count++;
            return nc;
        }
    }

    void insert(const std::vector<int> &keypath, value_t value) {
        Node *n = nodes;
        if (count < maxSize) {
            if (keypath.size() == 0 && count == 0) {
                nodes[0].value = value;
                count = 1;
            }
            for (const int k: keypath) {
                if (n->value != 0) { //Do not insert anything as child to node with 0 moment
                    n = findOrInsert(n, k, value);
                }
            }
            if(n->value != value) {
                std::cout <<"NewValue " << value << "contradicts existing value " << n->value << " for path ";
                for(const int k: keypath) std::cout << k << " ";
                std::cout << std::endl;
            }
        }
    }


    void insertAll(const std::vector<int> &cuboidDims, const std::vector<value_t> &values) {
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

    void
    getNormalizedSubset(const std::vector<int> &query, std::map<int, value_t> &result, size_t queryIdx, size_t keyPathNumber,
                        Node *n) const {
        result.emplace(keyPathNumber, n->value);
        size_t childID = n->firstChild;
        size_t queryKey;
        size_t qSize = query.size();
        while (childID != -1 && queryIdx < qSize) {
            n = nodes + childID;
            queryKey = query[queryIdx];
            if (n->key == queryKey) {
                getNormalizedSubset(query, result, queryIdx + 1, keyPathNumber + (1 << queryIdx), n);
                childID = n->nextSibling;
            } else if (n->key < queryKey) {
                childID = n->nextSibling;
            } else {
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
        fwrite(nodes, sizeof(Node), count, fp);
        fclose(fp);
    }

    void loadFromFile(const char *filename) {
        FILE *fp = fopen(filename, "rb");
        fscanf(fp, "%lu", &count);
        init(count);
        fread(nodes, sizeof(Node), count, fp);
        fclose(fp);
    }
};

#endif