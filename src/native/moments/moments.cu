#include "moments.h"
#include "mycudaheaders.h"
#include <assert.h>
#include <chrono>


using namespace std::chrono;

inline
cudaError_t checkCuda(cudaError_t result) {
    if (result != cudaSuccess) {
        fprintf(stderr, "CUDA Runtime Error: %s\n", cudaGetErrorString(result));
        assert(result == cudaSuccess);
    }
    return result;
}

//TODO Check if numMoments jstart jend need to be unsigned
__global__ void
computeKernel(int *moments, const int *data, const int numMoments, const int jstart,
               const int jend) {
    __shared__ int local[(ENTRIES_IN_BLOCK >> 1) * 33];
    __shared__ int tempvalues[ENTRIES_IN_BLOCK][
            VALUE_PER_MOMENT * 2 + 1]; // VALUES each of which can be at most 2 ints long
    int threadmoments[REC_SIZE_WORDS];
    const int blockMomentStart = blockIdx.x * ENTRIES_IN_BLOCK;
    const int yid = threadIdx.x >> 4; //REC_SIZE WORDS
    const int xid = threadIdx.x & 15;

    for (int i = yid;
         i < ENTRIES_IN_BLOCK && (blockMomentStart + i) < numMoments; i += (NUMCORES >> 4)) {
        local[(i << 4) + (i >> 1) + xid] = moments[(blockMomentStart + i) * REC_SIZE_WORDS + xid]; //add gap after 2 i
    }
    __syncthreads();
    int i = threadIdx.x / VALUE_PER_MOMENT; //multiple threads handle same moment
    int jy = threadIdx.x & (VALUE_PER_MOMENT - 1);
    for (int k = 0; k < KEY_SIZE_WORDS; k++) {
        threadmoments[k] = local[(i << 4) + (i >> 1) + k];
    }
    if (jy == 0) {
        *(T *) (threadmoments + KEY_SIZE_WORDS) = *(T *) (local + (i << 4) +(i >> 1) + KEY_SIZE_WORDS);
    } else
        *(T *) (threadmoments + KEY_SIZE_WORDS) = 0;
    __syncthreads();

    for (int joffset = jstart; joffset < jend; joffset += ENTRIES_IN_BLOCK) {
        //Copy DATA into shared memory
        for (int j = yid;
             j < ENTRIES_IN_BLOCK && (joffset + j) < jend; j += (NUMCORES >> 4)) {
            local[(j << 4) + (j >> 1) + xid] = data[(joffset + j) * REC_SIZE_WORDS + xid];
        }
        __syncthreads();
        if ((blockMomentStart + i) < numMoments) {
            for (int j = jy; j < ENTRIES_IN_BLOCK && (joffset + j) < jend; j += VALUE_PER_MOMENT) {
                int andRes = 1;
                for (int k = 0; k < KEY_SIZE_WORDS; k++) {
                    andRes &= ((threadmoments[k] & local[(j << 4) + (j >> 1) + k]) ==
                               threadmoments[k]);
                }
                *(T *) (threadmoments + KEY_SIZE_WORDS) += andRes * *(T *) (local + (j << 4) + (j >> 1) + KEY_SIZE_WORDS);
            }
        }
        __syncthreads();
    }
    if (i < ENTRIES_IN_BLOCK && (blockMomentStart + i) < numMoments) {
        //each jy can be atmost 2 ints
        *(T *) (tempvalues[i] + (jy << 1)) = *(T *) (threadmoments + KEY_SIZE_WORDS);
        __syncthreads();
        if (jy == 0) {
            T sum = 0;
            for (int jz = 0; jz < VALUE_PER_MOMENT; jz++)
                sum += *(T *) (tempvalues[i] + (jz << 1));
            *(T *) (moments + (blockMomentStart + i) * REC_SIZE_WORDS + KEY_SIZE_WORDS) = sum;
        }
    }
}

void computeCUDA(TypedCuboid &moments, const TypedCuboid &data) {

    const int halfmemorywords = (6UL * 1000 * 1000 * 1000) / sizeof(int);
    const int halfmemoryrecords = halfmemorywords / REC_SIZE_WORDS;

    int numDevices;
    cudaGetDeviceCount(&numDevices);
    printf("Number of GPU detected = %d\n", numDevices);
    int **gpu_moments = new int *[numDevices];
    int **gpu_data = new int *[numDevices];
    auto startTime = high_resolution_clock::now();
    //Allocate memory on all devices
    for (int devId = 0; devId < numDevices; devId++) {
        checkCuda(cudaSetDevice(devId));
        checkCuda(cudaMalloc(gpu_moments + devId, halfmemoryrecords * REC_SIZE_WORDS * sizeof(int)));
        checkCuda(cudaMalloc(gpu_data + devId, halfmemoryrecords * REC_SIZE_WORDS * sizeof(int)));
    }


    size_t stepCount = 0;
    cudaDeviceSetCacheConfig(cudaFuncCachePreferShared);
    size_t momentsOffsets[numDevices];
    size_t numMoments[numDevices];

    for (int devId = 0; devId < numDevices; devId++) {
        checkCuda(cudaSetDevice(devId));
        numMoments[devId] = std::min<size_t>(halfmemoryrecords, moments.numRows / numDevices);
        if (devId > 0)
            momentsOffsets[devId] = momentsOffsets[devId - 1] + numMoments[devId - 1];
        else
            momentsOffsets[0] = 0;

        printf("Device %d Moments from %lu to %lu (%lu moments) \n", devId, momentsOffsets[devId],
               momentsOffsets[devId] + numMoments[devId], numMoments[devId]);
        checkCuda(cudaMemcpy(gpu_moments[devId], moments.ptr + momentsOffsets[devId] * REC_SIZE_WORDS,
                             numMoments[devId] * REC_SIZE_WORDS * sizeof(int),
                             cudaMemcpyHostToDevice));
    }

    //For each block of data
    for (size_t dataoffset = 0; dataoffset < data.numRows; dataoffset += halfmemoryrecords) {

        //Copy the block of data to be computed to the devices. Same for all devices
        size_t numDataRemaining = std::min<size_t>(halfmemoryrecords, data.numRows - dataoffset);
        printf("NumDataRemaining = %lu \n", numDataRemaining);
        size_t numDataPerCent = numDataRemaining / 100 + 1;
        for (int devId = 0; devId < numDevices; devId++) {
            checkCuda(cudaSetDevice(devId));
            checkCuda(cudaMemcpy(gpu_data[devId], data.ptr + dataoffset * REC_SIZE_WORDS,
                                 numDataRemaining * REC_SIZE_WORDS * sizeof(int),
                                 cudaMemcpyHostToDevice));
        }
        for (size_t start = 0; start < numDataRemaining; start += numDataPerCent) {
            for (int devId = 0; devId < numDevices; devId++) {
                checkCuda(cudaSetDevice(devId));
                size_t thisdevicenumBlocks = numMoments[devId] / ENTRIES_IN_BLOCK + 1;
                computeKernel<<<thisdevicenumBlocks, NUMCORES>>>(gpu_moments[devId], gpu_data[devId],
                                                                  numMoments[devId], start, std::min(
                                start + numDataPerCent, numDataRemaining));
            }
            for (int devId = 0; devId < numDevices; devId++) {
                checkCuda(cudaSetDevice(devId));
                checkCuda(cudaDeviceSynchronize());
            }
            stepCount++;
            printf("Step %lu / 100 \n", stepCount);
        }
    }

    //Copy the result for the block of moments from respective devices
    for (int devId = 0; devId < numDevices; devId++) {
        checkCuda(cudaSetDevice(devId));
        checkCuda(cudaMemcpy(moments.ptr + momentsOffsets[devId] * REC_SIZE_WORDS, gpu_moments[devId],
                             numMoments[devId] * REC_SIZE_WORDS * sizeof(int),
                             cudaMemcpyDeviceToHost));
    }


    auto endTime = high_resolution_clock::now();
    auto duration = duration_cast<seconds>(endTime - startTime).count();
    printf("Computation on GPU took %lu seconds \n", duration);
    double rate = moments.numRows * 0.0864 / duration;
    printf("Rate = %f M/day \n", rate);
    for (int devId = 0; devId < numDevices; devId++) {
        checkCuda(cudaSetDevice(devId));
        checkCuda(cudaFree(gpu_moments[devId]));
        checkCuda(cudaFree(gpu_data[devId]));

    }
    delete[] gpu_moments;
    delete[] gpu_data;
}
