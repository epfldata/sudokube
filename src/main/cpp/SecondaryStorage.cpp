#include <string>
#include <filesystem>
#include "Keys.h"

using CuboidIDType = int64_t;
using MaskOffsetType = int64_t;
using IsDenseType = int8_t;
size_t MetadataDize = sizeof(IsDenseType) + sizeof(size_t);
unsigned int BufferSize = 1024;

inline byte *getColumn(byte **Array, size_t Idx, size_t OldColumnSize) {
    byte *Column = (byte *)Array + OldColumnSize * Idx;
    return Column;
}

void print_column_bit(int pos, byte *column) {
    int b = (column[pos / 8] >> (pos % 8)) % 2;
    if(b) printf("1");
    else  printf("0");
}

// returns True if the given bit is 1
bool getBit(byte* Column, int Position) {
    unsigned int BytePos = Position >> 3;  
    unsigned int BitPos = Position & 0x7;

    return Column[BytePos] & 1 << BitPos;
}

void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]) {
    
}

// TODO: ADD A FLAG FOR TEMPORARY FOR QUERIES
void rehashToDense(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum) {
    // add a cache
    // MAKE IT MODULAR FOR DIFFERENT LOADING POLICIES

    // read metadata aout the source cuboid 
    std::string CubeDirPath = "../../../../../dataset/" + CubeID + std::filesystem::path::preferred_separator;
    std::string CuboidDirPath = CubeDirPath +"cuboid" + std::to_string(SourceCuboidID) + std::filesystem::path::preferred_separator;

    std::string MetadataFilePath = CubeDirPath + "/metadata";

    byte *Metadata = (byte*)calloc(MetadataDize, sizeof(byte));

    FILE* MetadataFile = fopen (MetadataFilePath.c_str(), "r");
    assert(MetadataFile != NULL);
    fread(Metadata, sizeof(byte), MetadataDize, MetadataFile);
    fclose(MetadataFile);
    
    const size_t OldRowsCount = *(size_t*)(Metadata + sizeof(IsDenseType));
    // unsigned int OldColumnSize = bitsToBytes(OldRowsCount);

    printf("IsDense: %hhd\n", *(IsDenseType*)(Metadata));
    printf("OldRowsCount: %zu\n", OldRowsCount);
    // printf("OldColumnSize: %d\n", OldColumnSize);



    // unsigned int BufferSize = bitsToBytes(BufferRowsCount);
    // for (int i = 0; i < OldRowsCount/BufferRowsCount) {}

    const size_t BufferRowsCount = ((BufferSize / (bitsToBytes(MaskSum) + sizeof(value_t))) >> 3 ) << 3;
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);


    printf("BufferRowsCount: %zu\n", BufferRowsCount);

    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) /BufferRowsCount;

    for (int i = 0; i < 1; i++) {
        // read columns
        byte **IntermediateColStore = (byte**)calloc(MaskSum, BufferColumnSize);
        for(int i=0; i<MaskSum; i++){
            std::string ColReadFileName = CuboidDirPath + "col" + std::to_string(Mask[i]);
            FILE* ColReadFile = fopen (ColReadFileName.c_str(), "r");
            assert(ColReadFile != NULL);
            fseek(ColReadFile, i*BufferColumnSize, SEEK_SET);
            fread(getColumn(IntermediateColStore, i, BufferColumnSize), sizeof(byte), BufferColumnSize, ColReadFile);
            fclose (ColReadFile);
        }

        for (size_t i = 0; i<BufferRowsCount; i++) { 
            printf("Col Key: ");
            for(int j = MaskSum-1; j >= 0; j--)
                print_column_bit(i, getColumn(IntermediateColStore, j, BufferColumnSize));
            printf("\n");
        }
    }



    // // read columns
    // byte **IntermediateColStore = (byte**)calloc(MaskSum, OldColumnSize);
    // for(int i=0; i<MaskSum; i++){
    //     std::string FileName = CuboidDirPath + "col" + std::to_string(Mask[i]);
    //     FILE* ColReadFile = fopen (FileName.c_str(), "r");
    //     assert(ColReadFile != NULL);
    //     fread(getColumn(IntermediateColStore, i, OldColumnSize), sizeof(byte), OldColumnSize, ColReadFile);
    //     fclose (ColReadFile);
    // }

    // // read values
    // value_t *ColValStore = (value_t *) calloc(OldRowsCount, sizeof(value_t));
    // std::string FileName = CuboidDirPath + "values";
    // FILE *ReadValueFile = fopen(FileName.c_str(), "r");
    // assert(ReadValueFile != NULL);
    // fread(ColValStore, sizeof(value_t), OldRowsCount, ReadValueFile);
    // fclose(ReadValueFile);

    // // new store initialization 
    // size_t NewSize = 1LL << MaskSum;
    // value_t *NewColStore = (value_t *) calloc(NewSize, sizeof(value_t));
    // assert(NewColStore);

    // memset(NewColStore, 0, sizeof(value_t) * NewSize);

    // int NewKeySize = bitsToBytes(MaskSum);
    // size_t *NewKeys = (size_t *) calloc(OldRowsCount, sizeof(size_t));

    // // rehash
    // for (size_t r = 0; r < OldRowsCount; r++) {
    //     size_t NewKey = 0;
    //     for (int j = 0; j < MaskSum; j++){ 
    //         NewKey += (1<<j) * getBit(getColumn(IntermediateColStore, j, OldColumnSize), r);
    //     }
    //     NewColStore[NewKey] += ColValStore[r];
    // }

    

    // merge
    // flush old col stores, and retreive the other part from the disk

    // write it to file

    // update the metadata file

    // mask offset + sum
    // actual mask
    
}

void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum) {
    
}