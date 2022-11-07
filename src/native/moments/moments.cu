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

//Y moments
//X data
__global__ void
optimizedKernel(int *const moments, const int *const data, const unsigned numMoments, const unsigned numData) {

    __shared__ int localmoments[FACTOR][TILE_SIZE][TILE_SIZE];
    __shared__ int localdata[FACTOR][TILE_SIZE][TILE_SIZE];

    size_t x = blockIdx.x * blockDim.x + threadIdx.x;
    size_t y = blockIdx.y * blockDim.y + threadIdx.y;


    for (size_t f = 0; f < FACTOR; f++) {
        if ((f * TILE_SIZE + x) * TILE_SIZE + threadIdx.y < numData * REC_SIZE_WORDS) {
            localdata[f][threadIdx.x][threadIdx.y] = data[(f * TILE_SIZE + x) * TILE_SIZE + threadIdx.y];
        } else {
            localdata[f][threadIdx.x][threadIdx.y] = 0;
        }
        if ((f * TILE_SIZE + y) * TILE_SIZE + threadIdx.x < numMoments * REC_SIZE_WORDS) {
            localmoments[f][threadIdx.y][threadIdx.x] = moments[(f * TILE_SIZE + y) * TILE_SIZE + threadIdx.x];
        } else {
            localmoments[f][threadIdx.y][threadIdx.x] = 0;
        }
    }
    __syncthreads();

    for (unsigned yf = 0; yf < FACTOR; yf++) {
        for (unsigned yz = 0; yz < RECS_IN_TILE; yz++) {
            T acc = 0;
            for (unsigned xf = 0; xf < FACTOR; xf++) {
                for (unsigned xz = 0; xz < RECS_IN_TILE; xz++) {
                    bool andcond = ((xf * TILE_SIZE + x) * TILE_SIZE + xz * REC_SIZE_WORDS + KEY_SIZE_WORDS <
                                    numData * REC_SIZE_WORDS) &&
                                   ((yf * TILE_SIZE + y) * TILE_SIZE + yz * REC_SIZE_WORDS + KEY_SIZE_WORDS <
                                    numMoments * REC_SIZE_WORDS);
                    for (unsigned k = 0; andcond && k < KEY_SIZE_WORDS; k++) {
                        andcond =
                                andcond &&
                                ((localmoments[yf][threadIdx.y][yz * REC_SIZE_WORDS + k] &
                                  localdata[xf][threadIdx.x][xz * REC_SIZE_WORDS + k]) ==
                                 localmoments[yf][threadIdx.y][yz * REC_SIZE_WORDS + k]);
                    }
                    if (andcond)
                        acc += *(T *) (&localdata[xf][threadIdx.x][xz * REC_SIZE_WORDS + KEY_SIZE_WORDS]);
                }
            }
            if (acc != 0) {
                atomicAdd(reinterpret_cast<T *>(moments + (yf * TILE_SIZE + y) * TILE_SIZE + yz * REC_SIZE_WORDS +
                                                KEY_SIZE_WORDS), acc);
            }
        }
    }
}

__global__ void
computeKernel(int *moments, const int *data, const unsigned numMoments, const unsigned jstart,
              const unsigned jend) {
    __shared__ int localmoments[ENTRIES_IN_BLOCK][REC_SIZE_WORDS + 1];
    __shared__ int localdata[ENTRIES_IN_BLOCK][REC_SIZE_WORDS + 1];

    const unsigned blockMomentStart = blockIdx.x * ENTRIES_IN_BLOCK;
    const unsigned yid = threadIdx.x >> 4;
    const unsigned xid = threadIdx.x & 15;

    for (unsigned int i = yid;
         i < ENTRIES_IN_BLOCK && (blockMomentStart + i) < numMoments; i += 8) {
        localmoments[i][xid] = moments[(blockMomentStart + i) * REC_SIZE_WORDS + xid];

    }
    __syncthreads();
    for (unsigned joffset = jstart; joffset < jend; joffset += ENTRIES_IN_BLOCK) {
        //Copy DATA into shared memory
        for (unsigned int j = yid;
             j < ENTRIES_IN_BLOCK && (joffset + j) < jend; j += 8) {
            localdata[j][xid] = data[(joffset + j) * REC_SIZE_WORDS + xid];
        }
        __syncthreads();
        for (unsigned int i = threadIdx.x;
             i < ENTRIES_IN_BLOCK && (blockMomentStart + i) < numMoments; i += CORES_PER_BLOCK) {
            for (unsigned int j = 0; j < ENTRIES_IN_BLOCK && (joffset + j) < jend; j++) {
                unsigned int k = 0;
                for (; k < KEY_SIZE_WORDS; k++) {
                    if ((localmoments[i][k] & localdata[j][k]) != localmoments[i][k]) {
                        break;
                    }
                }
                if (k == KEY_SIZE_WORDS) {
                    *(T *) (localmoments[i] + KEY_SIZE_WORDS) += *(T *) (localdata[j] + KEY_SIZE_WORDS);
                }
            }
        }
        __syncthreads();
    }
    for (unsigned int i = threadIdx.x;
         i < ENTRIES_IN_BLOCK && (blockMomentStart + i) < numMoments; i += CORES_PER_BLOCK) {
        *(T *) (moments + (blockMomentStart + i) * REC_SIZE_WORDS + KEY_SIZE_WORDS) = *(T *) (
                localmoments[i] +
                KEY_SIZE_WORDS);
    }

}

void computeCUDA(TypedCuboid &moments, const TypedCuboid &data) {
    const unsigned int numBlocks = moments.numRows / ENTRIES_IN_BLOCK + 1;
    const int halfmemorywords = (6UL * 1000 * 1000 * 1000) / sizeof(int);
    const int halfmemoryrecords = halfmemorywords / REC_SIZE_WORDS;

    int numDevices;
    cudaGetDeviceCount(&numDevices);
    numDevices = 1;
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


    size_t numMomentBlocks = (1 + moments.numRows / (numDevices * halfmemoryrecords));
    size_t numDataBlocks = (1 + data.numRows / halfmemoryrecords);
    size_t totalSteps = numDataBlocks * numMomentBlocks;
    size_t stepCount = 0;
    printf("Total steps = %lu\n", totalSteps);
    printf("Kernel blocks = %lu BlockSize = %lu\n", numBlocks, CORES_PER_BLOCK);
    //For each block of moments to be computed
    for (size_t momentoffset = 0;
         momentoffset < moments.numRows; momentoffset += numDevices * halfmemoryrecords) {
        int curMaxDevId = numDevices;
        //Copy the block of moments to be computed to respective devices
        for (int devId = 0; devId < numDevices; devId++) {
            size_t thisdeviceMomentsOffset = momentoffset + halfmemoryrecords * devId;
            if (thisdeviceMomentsOffset >= moments.numRows) {
                curMaxDevId = devId;
                break;
            }
            checkCuda(cudaSetDevice(devId));
            size_t numMomentsRemaining = std::min<size_t>(halfmemoryrecords, moments.numRows - thisdeviceMomentsOffset);
            printf("NumMomentsRemaining for device %d = %lu\n", devId, numMomentsRemaining);
            if (numMomentsRemaining > 0)
                checkCuda(cudaMemcpy(gpu_moments[devId], moments.ptr + thisdeviceMomentsOffset * REC_SIZE_WORDS,
                                     numMomentsRemaining * REC_SIZE_WORDS * sizeof(int),
                                     cudaMemcpyHostToDevice));
        }

        //For each block of data
        for (size_t dataoffset = 0; dataoffset < data.numRows; dataoffset += halfmemoryrecords) {

            //Copy the block of data to be computed to the devices. Same for all devices
            size_t numDataRemaining = std::min<size_t>(halfmemoryrecords, data.numRows - dataoffset);
            printf("NumDataRemaining = %lu \n", numDataRemaining);
            size_t numDataPerCent = numDataRemaining / 100 + 1;
            for (int devId = 0; devId < curMaxDevId; devId++) {
                checkCuda(cudaSetDevice(devId));
                checkCuda(cudaMemcpy(gpu_data[devId], data.ptr + dataoffset * REC_SIZE_WORDS,
                                     numDataRemaining * REC_SIZE_WORDS * sizeof(int),
                                     cudaMemcpyHostToDevice));
            }
            //Launch smaller kernels to track progress
            for (size_t start = 0; start < numDataRemaining; start += numDataPerCent) {
                for (int devId = 0; devId < curMaxDevId; devId++) {
                    checkCuda(cudaSetDevice(devId));
                    size_t thisdeviceMomentsOffset = momentoffset + halfmemoryrecords * devId;
                    size_t numMomentsRemaining = std::min<size_t>(halfmemoryrecords,
                                                                  moments.numRows - thisdeviceMomentsOffset);

                    computeKernel<<<numBlocks, CORES_PER_BLOCK>>>(gpu_moments[devId], gpu_data[devId], numMomentsRemaining, start, std::min(
                            start + numDataPerCent, numDataRemaining));
                }


//            for (int devId = 0; devId < curMaxDevId; devId++) {
//                size_t thisdeviceMomentsOffset = momentoffset + halfmemoryrecords * devId;
//                size_t numMomentsRemaining = std::min<size_t>(halfmemoryrecords,
//                                                              moments.numRows - thisdeviceMomentsOffset);
//                checkCuda(cudaSetDevice(devId));
//                dim3 threads_per_block(TILE_SIZE, TILE_SIZE);
//                dim3 blocks_per_grid(1, 1);
//                blocks_per_grid.y = std::ceil(static_cast<double>(numMomentsRemaining) /
//                                              static_cast<double>(TILE_SIZE * RECS_IN_TILE * FACTOR));
//                blocks_per_grid.x = std::ceil(static_cast<double>(numDataRemaining) /
//                                              static_cast<double>(TILE_SIZE * RECS_IN_TILE * FACTOR));
//                printf("BLOCKS = %d %d \n", blocks_per_grid.x, blocks_per_grid.y);
//                optimizedKernel<<<blocks_per_grid, threads_per_block>>>(gpu_moments[devId], gpu_data[devId], numMomentsRemaining, numDataRemaining);
//            }
                for (int devId = 0; devId < curMaxDevId; devId++) {
                    checkCuda(cudaSetDevice(devId));
                    checkCuda(cudaDeviceSynchronize());
                    stepCount++;
                    printf("Step %lu / (%lu x %lu x 100) \n", stepCount, numMomentBlocks, numDataBlocks);
                }
            }
        }

        //Copy the result for the block of moments from respective devices
        for (int devId = 0; devId < curMaxDevId; devId++) {
            size_t thisdeviceMomentsOffset = momentoffset + halfmemoryrecords * devId;
            checkCuda(cudaSetDevice(devId));
            size_t numMomentsRemaining = std::min<size_t>(halfmemoryrecords, moments.numRows - thisdeviceMomentsOffset);
            checkCuda(cudaMemcpy(moments.ptr + thisdeviceMomentsOffset * REC_SIZE_WORDS, gpu_moments[devId],
                                 numMomentsRemaining * REC_SIZE_WORDS * sizeof(int),
                                 cudaMemcpyDeviceToHost));
        }
    }

    auto endTime = high_resolution_clock::now();
    auto duration = duration_cast<seconds>(endTime - startTime).count();
    printf("Computation on GPU took %lu seconds \n", duration);

    for (int devId = 0; devId < numDevices; devId++) {
        checkCuda(cudaSetDevice(devId));
        checkCuda(cudaFree(gpu_moments[devId]));
        checkCuda(cudaFree(gpu_data[devId]));
    }
    delete[] gpu_moments;
    delete[] gpu_data;
}
