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

    byte *Metadata = (byte*)calloc(MetadataSizeOnDisk, sizeof(byte));

    FILE* MetadataFile = fopen (MetadataFilePath.c_str(), "r");
    assert(MetadataFile != NULL);
    fread(Metadata, sizeof(byte), MetadataSizeOnDisk, MetadataFile);
    fclose(MetadataFile);
    
    const size_t OldRowsCount = *(size_t*)(Metadata + sizeof(IsDenseType));
    unsigned int OldColumnSize = bitsToBytes(OldRowsCount);

    printf("IsDense: %hhd\n", *(IsDenseType*)(Metadata));
    printf("OldRowsCount: %zu\n", OldRowsCount);
    printf("OldColumnSize: %d\n", OldColumnSize);

    const size_t BufferRowsCount = ((BufferSize / (bitsToBytes(MaskSum) + sizeof(value_t))) >> 3 ) << 3;
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);

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

    
}

void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum) {
    std::string CubeDirPath = "../../../../../dataset/" + CubeID + std::filesystem::path::preferred_separator;
    std::string CuboidDirPath = CubeDirPath + "cuboid" + std::to_string(SourceCuboidID) + std::filesystem::path::preferred_separator;

    Metadata Metadata = getMetadata(CubeDirPath, MaskSum);
    
    const size_t OldRowsCount = Metadata.OldRowsCount;
    unsigned int OldColumnSize = Metadata.OldColumnSize;
    const size_t BufferRowsCount = Metadata.BufferRowsCount;
    unsigned int BufferColumnSize = Metadata.BufferColumnSize;
    unsigned int KeySize = Metadata.KeySize;

    // start rehashing the cuboid stored on disk
    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) /BufferRowsCount;
    byte **IntermediateKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
    value_t *IntermediateValStore = (value_t *)calloc(BufferRowsCount, sizeof(value_t));

    byte *KeysArray = (byte*)calloc(BufferRowsCount, KeySize);

    for (int i = 0; i < RehashIterations; i++) {
        // read columns
        memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);
        readKeys(MaskSum, Mask, i, CuboidDirPath, BufferRowsCount, IntermediateKeyStore, BufferColumnSize);

        // read values
        memset(IntermediateValStore, 0, BufferRowsCount*sizeof(value_t));
        readValues(CuboidDirPath, i, BufferRowsCount, IntermediateValStore);

        // convert to row format for sorting
        // TO DO: CAN THIS BE IMPROVED?
        memset(KeysArray, 0, BufferRowsCount*KeySize);

        for(int c = 0; c < MaskSum; c++) {
            for (size_t r = 0; r < BufferRowsCount; r++) {         
                if (getBit(getColumn(IntermediateKeyStore, c, BufferColumnSize), r)) {
                    byte* Key = KeysArray + r*KeySize;
                    setBit(Key, c);
                }
            }
        }

        // sorting
        #ifdef DEBUG
        printf("\nUnsorted Keys\n");
        printKeyValuePairs(BufferRowsCount, MaskSum, KeySize, KeysArray, IntermediateValStore);
        #endif

        // sort the Key and Value Arrays
        quickSort(KeysArray, IntermediateValStore, 0, BufferRowsCount - 1, KeySize);

        // reset the col store to make it unusable
        memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);

        #ifdef DEBUG
        printf("\nSorted Keys\n");
        printKeyValuePairs(BufferRowsCount, MaskSum, KeySize, KeysArray, IntermediateValStore);
        #endif

        // merge the keys currently in the buffer
        size_t NewArrayLength = MergeKeysInPlace(BufferRowsCount, KeySize, KeysArray, IntermediateValStore);

        #ifdef DEBUG
        printf("\nMerged Keys\n");
        printKeyValuePairs(NewArrayLength, MaskSum, KeySize, KeysArray, IntermediateValStore);
        #endif
    }
    
    // merge
    // flush old col stores, and retreive the other part from the disk

    // write it to file

    // update the metadata file

    // mask offset + sum
    // actual mask
   
}

void fetch(std::string CubeID, unsigned int CuboidID);