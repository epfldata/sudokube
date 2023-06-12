//
// Created by Sachin Basil John on 23.08.22.
//

#ifndef SUDOKUBECBACKEND_TRIESTORE_H
#define SUDOKUBECBACKEND_TRIESTORE_H

#include "common.h"
#include "SetTrie.h"
#include <string>

//#define ROWSTORE 1
//#define COLSTORE 1

#ifdef ROWSTORE
#include "RowStore/RowStore.h"
#endif

#ifdef COLSTORE
#include "ColumnStore/ColumnStore.h"
#endif

struct TrieStore {
    SetTrie<double> globalSetTrie;
    std::vector<double> primaryMoments; //We need to store primary moments to do slice transformations
    const size_t maxDim = 6; //We don't store moments with dimensionality more than this

#ifdef ROWSTORE
    bool addDenseCuboidToTrie(RowStore &rowStore, const std::vector<int> &cuboidDims, unsigned int d_id);
    bool addSparseCuboidToTrie(RowStore &rowStore, const std::vector<int> &cuboidDims, unsigned int s_id);
#endif
#ifdef COLSTORE
    bool addDenseCuboidToTrie(ColumnStore &colStore, const std::vector<int> &cuboidDims, unsigned int d_id);
    bool addSparseCuboidToTrie(ColumnStore &colStore, const std::vector<int> &cuboidDims, unsigned int s_id);
#endif

    void reset() {
        delete[] globalSetTrie.nodes;
        globalSetTrie = SetTrie<double>();
    }
    void initTrie(size_t maxsize) { globalSetTrie.init(maxsize); }

    void setPrimaryMoments(double *pm, size_t s) {
        primaryMoments = std::vector<double>(pm, pm + s);
    }

    void saveTrie(const char *filename) {
        std::cout << "Trie Count = " << globalSetTrie.count << std::endl;

        //Also write primary moments to disk
        std::string file2 = filename;
        FILE *fp = fopen((file2 + "pmbin").c_str(), "wb");
        int numCols = primaryMoments.size();
        fwrite(&numCols, sizeof(int), 1, fp);
        fwrite(&primaryMoments[0], sizeof(double), numCols, fp);
        fclose(fp);

        globalSetTrie.saveToFile(filename);

    }

    void loadTrie(const char *filename) {
        globalSetTrie.loadFromFile(filename);
        std::string file2 = filename;
        std::string pmfilename = (file2 + "pmbin");
        FILE *fp = fopen(pmfilename.c_str(), "rb");
        if(!fp) throw std::runtime_error("Error opening primary moments " + pmfilename);
        int numCols;
        auto rv = fread(&numCols, sizeof(int), 1, fp);
        primaryMoments.reserve(numCols);
        for (int i = 0; i < numCols; i++) primaryMoments[i] = 1.0;
        rv = fread(&primaryMoments[0], sizeof(double), numCols, fp);
        if(rv != numCols) throw std::runtime_error("incorrect number of primary moments read");
        fclose(fp);
    }

    void
    prepareFromTrie(const std::vector<int> &query, std::vector<double> &result, const std::vector<int> sliceValues) {
        size_t len = sliceValues.size();
        std::vector<double> sliceFactors(len);
        double sliceProd = 1.0;

        /*
         Matrix for reverse transformation from central moments
            b=0  b=1
     sv=0   q   -1
     sv=1   p    1
         */
        for (int i = 0; i < len; i++) {
            double p = primaryMoments[query[i]];
            switch (sliceValues[i]) {
                case 0:
                    sliceFactors[i] = -1 / (1 - p); //transform b=0 -> b=1
                    sliceProd *= (1 - p); //initial value for b=0
                    break;
                case 1:
                    sliceFactors[i] = 1 / p;  //transform b=0 -> b=1
                    sliceProd *= p; //initial value for b=0
                    break;
                case -1:
                    sliceFactors[i] = 1.0; //no change
                    break;
            }
        }
        globalSetTrie.getNormalizedSubsetForSlice(query, result, sliceFactors, 0, 0, 0, globalSetTrie.nodes, sliceProd);
    }
};


#endif //SUDOKUBECBACKEND_TRIESTORE_H
