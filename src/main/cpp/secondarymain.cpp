// contains all the calls hat will be called by the core that will go into the CBackend
#include <stdio.h>
#include <string>
#include "Keys.h"
#include "Payload.h"

extern void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]);
extern void rehashToDense(std::string CubeID, unsigned int SourceCuboidID,  unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum);
extern void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID,  unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum);
extern void fetch(std::string CubeID, unsigned int CuboidID);

int main(int argc, char* argv[]) {
    unsigned int mask[] = {0,1,2,3,8,9,10,11};
    const unsigned int masksum = sizeof(mask)/sizeof(mask[0]);
    rehashToSparse("cube1", 0, 1, mask, masksum);
    return 0;
}


