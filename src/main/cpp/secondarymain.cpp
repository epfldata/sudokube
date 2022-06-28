// contains all the calls hat will be called by the core that will go into the CBackend
#include <stdio.h>
#include <string>
#include <cassert>
#include <chrono>
#include "Keys.h"
#include "Payload.h"

extern void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]);
extern void rehashToDense(std::string CubeID, unsigned int SourceCuboidID,  unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum);
extern void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID,  unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum);
extern void fetch(std::string CubeID, unsigned int CuboidID);

int main(int argc, char* argv[]) {
    unsigned int mask[] = {0,1,2,3,8,9,10,11};
    const unsigned int masksum = sizeof(mask)/sizeof(mask[0]);

    auto start = std::chrono::steady_clock::now();

    const char* CubeID = "cube1";

    rehashToSparse(CubeID, 0, 1, mask, masksum);

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    double total_exec_time = elapsed_seconds.count();
    printf("total_exec_time: %f\n", total_exec_time);


    return 0;
}


