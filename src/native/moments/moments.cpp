#include "moments.h"
#include <cmath>
//#include <libcuckoo/cuckoohash_map.hh>
#include <thread>
#include <pthread.h>
#include <sched.h>
#include <inttypes.h>
#include "SetTrie.h"

using namespace std::chrono;
const int sampleFactor = 4;

extern void computeCUDA(TypedCuboid &moments, const TypedCuboid &data);
//extern void computeCUDASimulation(TypedCuboid &moments, const TypedCuboid &data);

void compute(TypedCuboid &moments, TypedCuboid &data) {
    size_t numRowsPerPercent = std::max(1UL, moments.numRows / 100);
    for (size_t i = 0; i < moments.numRows; i++) {
        if (i % numRowsPerPercent == 0) {
            printf("%d%% ", (i * 100) / moments.numRows);
            fflush(stdout);
        }
        //Compute only 25 samples
        if (i % (sampleFactor * numRowsPerPercent) > 0) continue;
        int *momentkey = moments.getKey(i);
        T *momentval = moments.getVal(i);
        for (size_t j = 0; j < data.numRows; j++) {
            const int *datakey = data.getKey(j);
            const T *dataval = data.getVal(j);
            size_t k = 0;
            for (; k < KEY_SIZE_WORDS; k++) {
                if ((momentkey[k] & datakey[k]) != momentkey[k])
                    break;
            }
            if (k == KEY_SIZE_WORDS) {
//                printf("moment[%d] += data[%d] %f  momentval=%x \n", i, j, (float) *dataval, momentval);
                *momentval += *dataval;
            }
        }
    }
}

const int nbits = 429;

void byteArrayToSet(int *key, std::vector<int> &result) {
    result.clear();
    for (size_t i = 0; i < KEY_SIZE_WORDS; i++) {
        int b = key[i];
        for (int j = 0; j < 32; j++) {
            if (b & (1U << j)) {
                result.emplace_back((i << 5) + j);
            }
        }
    }
}

void setToByteArray(int *key, const std::vector<int> &bits) {
    memset(key, 0, REC_SIZE_WORDS * sizeof(int));
    for (auto b: bits) {
        size_t i = b >> 5;
        size_t j = b & 31;
        key[i] |= (1U << j);
    }
}

struct MyHash {
    size_t operator()(const std::vector<int> &vec) const {
        std::size_t seed = vec.size();
        for (auto x: vec) {
            x = ((x >> 16) ^ x) * 0x45d9f3b;
            x = ((x >> 16) ^ x) * 0x45d9f3b;
            x = (x >> 16) ^ x;
            seed ^= x + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

/*
int main1(int argc, char **argv) {
    libcuckoo::cuckoohash_map<std::vector<int>, bool, MyHash> sbjmap;
    uint32_t numQ, qs;
    std::string path = "/root/moments/15_32k/";
    FILE *queryFile = fopen((path + "queries.bin").c_str(), "rb");
    if (!queryFile) throw std::runtime_error("Cannot open file " + path + std::string("queries.bin"));
    fread(&qs, sizeof(uint32_t), 1, queryFile);
    fread(&numQ, sizeof(uint32_t), 1, queryFile);
    std::cout << "Qs = " << qs << "  numQ = " << numQ << std::endl;
    uint32_t *queries = (uint32_t *) calloc(numQ, qs * sizeof(uint32_t));
    auto readElems = fread(queries, qs * sizeof(uint32_t), numQ, queryFile);
    if (readElems != numQ) throw std::runtime_error("Incorrect elements read from query file");
    fclose(queryFile);

    std::cout << "Top 5" << std::endl;
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < qs; j++) {
            std::cout << queries[i * qs + j] << " ";
        }
        std::cout << std::endl;
    }
    std::cout << "Last 5" << std::endl;
    for (int i = numQ - 5; i < numQ; i++) {
        for (int j = 0; j < qs; j++) {
            std::cout << queries[i * qs + j] << " ";
        }
        std::cout << std::endl;
    }

    const int maxSubsetSize = 6;
    const int numThreads = 24;

    const size_t powersetSize = 1 << qs;
    std::vector<std::thread> threadVec;
    auto start = high_resolution_clock::now();
    for (size_t id = 0; id < numThreads; id++) {
        threadVec.emplace_back([id, &sbjmap, queries, &threadVec, numQ, qs, powersetSize] {
            std::vector<int> localchild;
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(id, &cpuset);
            int rc = pthread_setaffinity_np(threadVec[id].native_handle(), sizeof(cpu_set_t), &cpuset);
            const size_t countPerThread = numQ / numThreads;
            const size_t start = id * countPerThread;
            size_t end = (id + 1) * countPerThread;
            if (id == (numThreads - 1)) end = numQ;
            for (size_t qid = start; qid < end; qid++) {
                const uint32_t *parent = queries + (qid * qs);
                for (size_t i = 0; i < powersetSize; i++) {
                    size_t j = i;
                    size_t b = 0;
                    localchild.clear();
                    size_t count = 0;
                    int64_t prev = -1;

                    while (j > 0 && count < maxSubsetSize) {
                        if (j & 1) {
                            localchild.emplace_back(parent[b]);
                            count++;
                        }
                        j >>= 1;
                        b++;
                    }
                    if (j == 0) {
                        sbjmap.insert(localchild, true);
                    }
                }
            }
        });
    }
    for (auto &t: threadVec) t.join();
    auto end = high_resolution_clock::now();
    size_t duration = duration_cast<seconds>(end - start).count();
    std::cout << "Time for " << numQ << " sets using " << numThreads << " threads = " << duration << " seconds"
              << std::endl;
    auto tbl = sbjmap.lock_table();

    size_t myMax = 0;
    std::vector<std::vector<int>> test;
//    for(const auto& v: tbl){
//        if(v.first.size() > myMax) myMax = v.first.size();
//        if(v.first.size() == 2 && v.first[0] >= v.first[1]) {
//            test.emplace_back(v.first);
//        for(const auto b: v.first) std::cout << b << " ";
//        std::cout << std::endl;
//        }
//    }
//    std::sort(test.begin(), test.end());
    const int numMoments = tbl.size();
    std::cout << "Count = " << numMoments << std::endl;
    TypedCuboid typedMoments(nullptr, numMoments, nbits);
    typedMoments.realloc();
    auto it = tbl.cbegin();
    size_t count = 0;
    while (it != tbl.cend()) {
        setToByteArray(typedMoments.getKey(count), it->first);
        it++;
        count++;
    }
    FILE *momentsFile = fopen((path + "momentsempty.bin").c_str(), "wb");
    if (!momentsFile) throw std::runtime_error("Cannot open file " + path + std::string("momentsempty.bin"));
    auto numWrite = fwrite(typedMoments.ptr, REC_SIZE_WORDS * sizeof(int), numMoments, momentsFile);
    if (numWrite != numMoments) throw std::runtime_error("Wrong number of entries written\n");
    fclose(momentsFile);
    tbl.clear();
}*/

int main2(int argc, char **argv) {
    RowStore rowStore;
    uint64_t totalSum = 0;
    uint64_t primaryMoments[nbits];


    int NYCnbits[] = {nbits};
    int NYCsize[] = {92979827};

    unsigned char isSparseArray[] = {true};
    unsigned int idArray[1];
    std::string NYC_base = "cubedata/NYC_base/multicube_0.csuk";
    rowStore.readMultiCuboid(NYC_base.c_str(), NYCnbits, NYCsize, isSparseArray, idArray, 1);
    printf("Base loading complete \n ");
    SparseCuboidRow base(rowStore.read(idArray[0]));

    std::string pmfile = "cubedata/NYC_base/primarymoments.bin";
    FILE *fp = fopen(pmfile.c_str(), "rb");
    if (fp == NULL) throw std::runtime_error("Missing file " + pmfile);
    if (fread(&totalSum, sizeof(uint64_t), 1, fp) != 1)
        throw std::runtime_error("Cannot read total");
//    totalSum = 17;
    double invTotalSum = std::pow((double) totalSum, -1.0);
    if (fread(primaryMoments, sizeof(uint64_t), nbits, fp) != nbits)
        throw std::runtime_error("Fewer number of primary moments read");

    size_t numMoments = 145277295L;

    TypedCuboid typedMoments(nullptr, numMoments, nbits);
    typedMoments.realloc();

    TypedCuboid typedMoments_GPU(nullptr, numMoments, nbits);
    typedMoments_GPU.realloc();

    FILE *momentsfile = fopen("/root/moments/15_32k/momentsempty.bin", "rb");
    auto readElems = fread(typedMoments.ptr, REC_SIZE_WORDS * sizeof(int), numMoments, momentsfile);
    if (readElems != numMoments) throw std::runtime_error("Incorrect number of moments read");
    fclose(momentsfile);

//    size_t count = 0;
//    for (int i = 0; i < nbits; i++) {
//        for (int j = i + 1; j < nbits; j++) {
//            for (int k = j + 1; k < nbits; k++) {
//                std::vector<int> bits = {i, j, k};
//                setToByteArray(typedMoments.getKey(count), bits);
//                count++;
//                if (count >= numMoments) break;
//            }
//            if (count >= numMoments) break;
//        }
//        if (count >= numMoments) break;
//    }

    memcpy(typedMoments_GPU.ptr, typedMoments.ptr, typedMoments.numRows * REC_SIZE_WORDS * sizeof(int));
    TypedCuboid typedData(base);
    printf("Setting moments complete\n");

    computeCUDA(typedMoments_GPU, typedData);
//    exit(0);
    compute(typedMoments, typedData);
    printf("\n");
    bool allkeyequal = true;
    bool allvalequal = true;
    size_t checkInterval = std::max(1UL, numMoments / 100);
    std::vector<int> keyCPU, keyGPU;
    for (int i = 0; i < numMoments; i++) {
        //Check only for 25 samples
        if (i % (sampleFactor * checkInterval) > 0) continue;
        byteArrayToSet(typedMoments.getKey(i), keyCPU);
        byteArrayToSet(typedMoments_GPU.getKey(i), keyGPU);
        bool keyequals = (keyCPU == keyGPU);
        allkeyequal = allkeyequal && keyequals;

        bool valequals = (*typedMoments.getVal(i) == *typedMoments_GPU.getVal(i));
        allvalequal = allvalequal && valequals;
        if (true || !keyequals || !valequals) {
            printf("%d %d  i=%d \t ", keyequals, valequals, i);
            printf("Key CPU: {");
            for (auto b: keyCPU) printf(" %d ", b);
            printf("}  Value CPU: %lu \t", (size_t) *typedMoments.getVal(i));

            printf("Key GPU: {");
            for (auto b: keyGPU) printf(" %d ", b);
            printf("}  Value GPU: %lu \n", (size_t) *typedMoments_GPU.getVal(i));
        }
    }
    printf("ALL KEY EQUAL = %d , ALL VALUE EQUAL = %d \n", allkeyequal, allvalequal);

    FILE *momentsOut = fopen("/root/moments/15_32k/moments_filled.bin", "wb");
    fwrite(typedMoments_GPU.ptr, REC_SIZE_WORDS * sizeof(int), numMoments, momentsOut);
    fclose(momentsOut);
}

int main3() {
    size_t numMoments = 145277295L;
    TypedCuboid moments(nullptr, numMoments, nbits);
    moments.realloc();
    FILE *momentsfile = fopen("/root/moments/15_32k/moments_filled.bin", "rb");
    if (!momentsfile) throw std::runtime_error("Cannot open file /root/moments/15_32k/moments_filled.bin");
    auto read = fread(moments.ptr, REC_SIZE_WORDS * sizeof(int), numMoments, momentsfile);
    if (read != numMoments) throw std::runtime_error("Insufficient number of moments read");
    printf("Reading Moments Complete \n");
    SetTrie trie;
    trie.init(numMoments * 6);
    std::vector<int> set;
    size_t numMomentPercent = numMoments / 100 + 1;
    printf("Allocated memory for trie \n");
    for (size_t i = 0; i < numMoments; i++) {
        if(i % numMomentPercent == 0) printf("%d ", i/numMomentPercent);
        auto key = moments.getKey(i);
        auto val = *moments.getVal(i);
        byteArrayToSet(key, set);
        trie.insert(set, val);
    }
    printf("Trie count=%d maxsize=%d \n", trie.count, trie.maxSize);
    trie.saveToFile("/root/moments/15_32k/trie.bin");
}
int main() {
//    main2(0, 0);
//    main3();
    SetTrie trie;
    trie.loadFromFile("/root/moments/15_32k/trie.bin");
    std::vector<int> query = {115, 116, 117, 118, 119, 120, 121, 132, 133, 134, 197, 198, 199, 200, 201, 202, 203, 204, 205, 366};

    auto start = high_resolution_clock::now();
    std::map<int, value_t> momentResult;
    trie.getNormalizedSubset(query, momentResult, 0, 0, trie.nodes);
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end-start).count();
    printf("Duration = %lu us  Number of moments = %lu \n", duration, momentResult.size());

}