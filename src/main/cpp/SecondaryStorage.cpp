#include "SecondaryStorage.h"

void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]) {
    
}

// TODO: ADD A FLAG FOR TEMPORARY FOR QUERIES
// Assume resulting detination cuboid always fits in memory
void rehashToDense(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], const unsigned int MaskSum) {
    // add a cache
    // MAKE IT MODULAR FOR DIFFERENT LOADING POLICIES

    // read metadata about the source cuboid 
    std::string CubeDirPath = "../../../../../dataset/" + CubeID + std::filesystem::path::preferred_separator;
    std::string CuboidDirPath = CubeDirPath +"cuboid" + std::to_string(SourceCuboidID) + std::filesystem::path::preferred_separator;

    std::string MetadataFilePath = CubeDirPath + "/metadata";

    byte *Metadata = (byte*)calloc(MetadataDize, sizeof(byte));

    FILE* MetadataFile = fopen (MetadataFilePath.c_str(), "r");
    assert(MetadataFile != NULL);
    fread(Metadata, sizeof(byte), MetadataDize, MetadataFile);
    fclose(MetadataFile);
    
    const size_t OldRowsCount = *(size_t*)(Metadata + sizeof(IsDenseType));
    unsigned int OldColumnSize = bitsToBytes(OldRowsCount);

    printf("IsDense: %hhd\n", *(IsDenseType*)(Metadata));
    printf("OldRowsCount: %zu\n", OldRowsCount);
    printf("OldColumnSize: %d\n", OldColumnSize);

    const size_t BufferRowsCount = ((BufferSize / (bitsToBytes(MaskSum) + sizeof(value_t))) >> 3 ) << 3;
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);

    //------------------------------------- BUFFER -------------------------------------//

    printf("BufferRowsCount: %zu\n", BufferRowsCount);
    printf("BufferColumnSize: %d\n", BufferColumnSize);

    // start rehashing the cuboid stored on disk
    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) /BufferRowsCount;
    byte **IntermediateKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
    value_t *IntermediateValStore = (value_t *)calloc(BufferRowsCount, sizeof(value_t));

    // new store initialization 
    size_t DestinationRowsCount = 1LL << MaskSum;
    value_t *DestinationValStore = (value_t *) calloc(DestinationRowsCount, sizeof(value_t));
    assert(DestinationValStore);

    int DestinationKeySize = bitsToBytes(MaskSum);
    size_t *DestinationKeys = (size_t *) calloc(OldRowsCount, sizeof(size_t));

    for (int i = 0; i < RehashIterations; i++) {
        // read columns
        memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);
        readKeys(MaskSum, Mask, i, CuboidDirPath, BufferRowsCount, IntermediateKeyStore, BufferColumnSize);

        // read values
        memset(IntermediateValStore, 0, BufferRowsCount*sizeof(value_t));
        readValues(CuboidDirPath, i, BufferRowsCount, IntermediateValStore);

        #ifdef DEBUG
        for (size_t i = 0; i<BufferRowsCount; i++) { 
            printf("Col Key: ");
            for(int j = MaskSum-1; j >= 0; j--)
                print_column_bit(i, getColumn(IntermediateKeyStore, j, BufferColumnSize));
            printf("\tValue: %lld\n", IntermediateValStore[i]);
        }
        printf("BREAK\n");
        #endif

        // rehash
        for (size_t r = 0; r < BufferRowsCount; r++) {
            size_t DestinationKey = 0;
            for (int j = 0; j < MaskSum; j++){ 
                DestinationKey += (1<<j) * getBit(getColumn(IntermediateKeyStore, j, BufferColumnSize), r);
            }
            DestinationValStore[DestinationKey] += IntermediateValStore[r];
        }
    }

    // generate sparse keys
    const unsigned int DestinationKeyBits = MaskSum;
    unsigned int DestinationColumnSize = bitsToBytes(DestinationRowsCount);
    byte **DestinationKeyStore = (byte**)calloc(DestinationKeyBits, DestinationColumnSize);

    for (size_t r = 0; r<DestinationRowsCount; r++) { 
        value_t IntKey = r;  // CAUTION; KEYS BIGGER THAN 64 BITS WILL NOT WORK
        std::bitset<sizeof(value_t)*8> bit_r = std::bitset<sizeof(value_t)*8>(IntKey);
        for(int i = 0; i < DestinationKeyBits; i++) {
            if (bit_r[i]) {
                byte* Col = getColumn(DestinationKeyStore, i, DestinationColumnSize);
                setBit(Col, r);
            }
        }
    }

    #ifdef DEBUG
    printf("\nNew Key Value Pairs\n");
    // const unsigned int DestinationKeyBits = sizeof(*Mask)/sizeof(Mask[0]);;
    for(int i=0; i<DestinationRowsCount; i++) {
        printf("Int Key: %d", i);
        // printf("\tKey: ");
        // for(int j = DestinationKeyBits-1; j >= 0; j--)
        //     print_column_bit(i, getColumn(DestinationKeyStore, j, DestinationColumnSize));
        printf("\tValue: %lld \n",  DestinationValStore[i]);
    }
    #endif

    //------------------------------------- BUFFER -------------------------------------//


    //------------------------------------- NO BUFFER -------------------------------------//
    // read columns
    // byte **IntermediateKeyStore = (byte**)calloc(MaskSum, OldColumnSize);
    // for(int i=0; i<MaskSum; i++){
    //     std::string FileName = CuboidDirPath + "col" + std::to_string(Mask[i]);
    //     FILE* ColReadFile = fopen (FileName.c_str(), "r");
    //     assert(ColReadFile != NULL);
    //     fread(getColumn(IntermediateKeyStore, i, OldColumnSize), sizeof(byte), OldColumnSize, ColReadFile);
    //     fclose (ColReadFile);
    // }

    // // read values
    // value_t *ColValStore = (value_t *) calloc(OldRowsCount, sizeof(value_t));
    // std::string FileName = CuboidDirPath + "values";
    // FILE *ReadValueFile = fopen(FileName.c_str(), "r");
    // assert(ReadValueFile != NULL);
    // fread(ColValStore, sizeof(value_t), OldRowsCount, ReadValueFile);
    // fclose(ReadValueFile);

    // #ifdef DEBUG
    // for (size_t i = 0; i<OldRowsCount; i++) { 
    //     printf("Col Key: ");
    //     for(int j = MaskSum-1; j >= 0; j--)
    //         print_column_bit(i, getColumn(IntermediateKeyStore, j, OldColumnSize));
    //     printf("\tValue: %lld\n", ColValStore[i]);
    //     if ((i+1)%BufferRowsCount == 0) printf("BREAK\n");
    // }
    // #endif


    // // new store initialization 
    // size_t DestinationRowsCount = 1LL << MaskSum;
    // value_t *NewColStore = (value_t *) calloc(DestinationRowsCount, sizeof(value_t));
    // assert(NewColStore);

    // memset(NewColStore, 0, sizeof(value_t) * DestinationRowsCount);

    // int DestinationKeySize = bitsToBytes(MaskSum);
    // size_t *DestinationKeys = (size_t *) calloc(OldRowsCount, sizeof(size_t));

    // // rehash
    // for (size_t r = 0; r < OldRowsCount; r++) {
    //     size_t DestinationKey = 0;
    //     for (int j = 0; j < MaskSum; j++){ 
    //         DestinationKey += (1<<j) * getBit(getColumn(IntermediateKeyStore, j, OldColumnSize), r);
    //     }
    //     NewColStore[DestinationKey] += ColValStore[r];
    // }

    // #ifdef DEBUG
    // printf("\nNew Key Value Pairs\n");
    // const unsigned int DestinationKeyBits = sizeof(*Mask)/sizeof(Mask[0]);;
    // for(int i=0; i<DestinationRowsCount; i++) {
    //     printf("Int Key: %d", i);
    //     // printf("\tKey: ");
    //     // for(int j = DestinationKeyBits-1; j >= 0; j--)
    //     //     print_column_bit(i, getColumn(DestinationKeyStore, j, DestinationColumnSize));
    //     printf("\tValue: %lld \n",  NewColStore[i]);
    // }
    // #endif

    //------------------------------------- NO BUFFER -------------------------------------//

    

    // merge
    // flush old col stores, and retreive the other part from the disk

    // write it to file

    // update the metadata file

    // mask offset + sum
    // actual mask
}

void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum) {
    
}

void fetch(std::string CubeID, unsigned int CuboidID);