#include "moments.h"
#include <cmath>

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

std::vector<int> byteArrayToSet(int *key) {
    std::vector<int> result;
    for (size_t i = 0; i < KEY_SIZE_WORDS; i++) {
        int b = key[i];
        for (int j = 0; j < 32; j++) {
            if (b & (1U << j)) {
                result.emplace_back((i << 5) + j);
            }
        }
    }
    return result;
}

void setToByteArray(int *key, const std::vector<int> &bits) {
    memset(key, 0, KEY_SIZE_WORDS * sizeof(int));
    for (auto b: bits) {
        size_t i = b >> 5;
        size_t j = b & 31;
        key[i] |= (1U << j);
    }
}

int main(int argc, char **argv) {
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

    size_t numMoments = 30 * 1000;

    TypedCuboid typedMoments(nullptr, numMoments, nbits);
    typedMoments.realloc();

    TypedCuboid typedMoments_GPU(nullptr, numMoments, nbits);
    typedMoments_GPU.realloc();

    size_t count = 0;
    for (int i = 0; i < nbits; i++) {
        for (int j = i + 1; j < nbits; j++) {
            for(int k = j + 1; k < nbits; k++) {
                std::vector<int> bits = {i, j, k};
                setToByteArray(typedMoments.getKey(count), bits);
                count++;
                if(count >= numMoments) break;
            }
            if (count >= numMoments) break;
        }
        if (count >= numMoments) break;
    }
    memcpy(typedMoments_GPU.ptr, typedMoments.ptr, typedMoments.numRows * REC_SIZE_WORDS * sizeof(int));
    TypedCuboid typedData(base);
    printf("Setting moments complete\n");

    computeCUDA(typedMoments_GPU, typedData);
    compute(typedMoments, typedData);

    bool allkeyequal = true;
    bool allvalequal = true;
    size_t checkInterval = std::max(1UL, numMoments / 100);
    for (int i = 0; i < numMoments; i++) {
        //Check only for 25 samples
        if (i % (sampleFactor * checkInterval) > 0) continue;
        auto keyCPU = byteArrayToSet(typedMoments.getKey(i));
        auto keyGPU = byteArrayToSet(typedMoments_GPU.getKey(i));
        bool keyequals = (keyCPU == keyGPU);
        allkeyequal = allkeyequal && keyequals;

        bool valequals = (*typedMoments.getVal(i) == *typedMoments_GPU.getVal(i));
        allvalequal = allvalequal && valequals;
        if (!keyequals || !valequals) {
            printf("%d %d  \t ", keyequals, valequals);
            printf("Key CPU: {");
            for (auto b: keyCPU) printf(" %d ", b);
            printf("}  Value CPU: %lu \t", (size_t) *typedMoments.getVal(i));

            printf("Key GPU: {");
            for (auto b: keyGPU) printf(" %d ", b);
            printf("}  Value GPU: %lu \n", (size_t) *typedMoments_GPU.getVal(i));
        }
    }
    printf("ALL KEY EQUAL = %d , ALL VALUE EQUAL = %d \n", allkeyequal, allvalequal);
}

