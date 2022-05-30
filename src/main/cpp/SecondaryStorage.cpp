#include "SecondaryStorage.h"
#include "ByteQuickSort.h"

void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]) {
    
}

// TODO: ADD A FLAG FOR TEMPORARY FOR QUERIES
// Assume resulting detination cuboid always fits in memory
void rehashToDense(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], const unsigned int MaskSum) {
    // add a cache
    // MAKE IT MODULAR FOR DIFFERENT LOADING POLICIES

    // read metadata about the source cuboid 
    const unsigned int ValueSize = sizeof(value_t);

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

    const size_t BufferRowsCount = ((BufferSize / (bitsToBytes(MaskSum) + ValueSize)) >> 3 ) << 3;
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);

    printf("BufferRowsCount: %zu\n", BufferRowsCount);
    printf("BufferColumnSize: %d\n", BufferColumnSize);

    // start rehashing the cuboid stored on disk
    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) /BufferRowsCount;
    byte **IntermediateKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
    value_t *IntermediateValStore = (value_t *)calloc(BufferRowsCount, ValueSize);

    // new store initialization 
    size_t DestinationRowsCount = 1LL << MaskSum;
    value_t *DestinationValStore = (value_t *) calloc(DestinationRowsCount, ValueSize);
    assert(DestinationValStore);

    int DestinationKeySize = bitsToBytes(MaskSum);
    size_t *DestinationKeys = (size_t *) calloc(OldRowsCount, sizeof(size_t));

    

    for (int i = 0; i < RehashIterations; i++) {
        // read columns
        memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);
        readKeys(MaskSum, Mask, i, CuboidDirPath, BufferRowsCount, IntermediateKeyStore, BufferColumnSize);

        // read values
        memset(IntermediateValStore, 0, BufferRowsCount*ValueSize);
        readValues(CuboidDirPath, i, BufferRowsCount, IntermediateValStore);

        #ifdef DEBUG
        for (size_t i = 0; i<BufferRowsCount; i++) { 
            printf("Col Key: ");
            for(int j = MaskSum-1; j >= 0; j--)
                printColumnBit(i, getColumn(IntermediateKeyStore, j, BufferColumnSize));
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
        std::bitset<ValueSize*8> bit_r = std::bitset<ValueSize*8>(IntKey);
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
        //     printColumnBit(i, getColumn(DestinationKeyStore, j, DestinationColumnSize));
        printf("\tValue: %lld \n",  DestinationValStore[i]);
    }
    #endif

    
}

void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], unsigned int MaskSum) {
    std::string CubeDirPath = "../../../../../dataset/" + CubeID + std::filesystem::path::preferred_separator;
    std::string SourceCuboidDirPath = CubeDirPath + "cuboid" + std::to_string(SourceCuboidID) + std::filesystem::path::preferred_separator;

    Metadata Metadata = getMetadata(CubeDirPath, MaskSum);
    
    const size_t OldRowsCount = Metadata.RowsCount;
    unsigned int OldColumnSize = Metadata.ColumnSize; // size of a column in the bit column tore 
    
    const size_t PageRowsCount = Metadata.PageRowsCount;
    const size_t BufferRowsCount = PageRowsCount * BufferPages / 2; // half the buffer pages will be left for the output, as well as column to row key conversion
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);
    
    unsigned int DestinationKeySize = Metadata.DestinationKeySize;
    const unsigned int ValueSize = Metadata.ValueSize;

    // start rehashing the cuboid stored on disk
    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) / BufferRowsCount; // ceil(OldRowsCount /BufferRowsCount)

    #ifdef DEBUG
        printf("\nRehash Iterations : %d\n", RehashIterations);
    #endif
    // TODO: Add a hybrid algorithm

    // create the destination cuboid
    std::string DestCuboidDirPath = CubeDirPath + "cuboid" + std::to_string(DestinationCuboidID) + std::filesystem::path::preferred_separator;
    std::filesystem::create_directories(DestCuboidDirPath);

    std::string TempRunFileNamePrefix = DestCuboidDirPath + "run";
    std::string TempRunFileNameSuffix = ".temp";

    for (int i = 0; i < RehashIterations; i++) {
        // read columns
        // memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);
        byte **IntermediateKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
        readKeys(MaskSum, Mask, i, SourceCuboidDirPath, BufferRowsCount, IntermediateKeyStore, BufferColumnSize);
        
        // read values
        // memset(IntermediateValStore, 0, BufferRowsCount*ValueSize);
        value_t *IntermediateValStore = (value_t *)calloc(BufferRowsCount, ValueSize);
        readValues(SourceCuboidDirPath, i, BufferRowsCount, IntermediateValStore);
       
        // convert to row format for sorting
        byte *KeysArray = (byte*)calloc(BufferRowsCount, DestinationKeySize);
        // TO DO: IS THERE A BETTER WAY TO ZIP?
        // memset(KeysArray, 0, BufferRowsCount*DestinationKeySize);

        for(int c = 0; c < MaskSum; c++) {
            for (size_t r = 0; r < BufferRowsCount; r++) {         
                if (getBit(getColumn(IntermediateKeyStore, c, BufferColumnSize), r)) {
                    byte* Key = KeysArray + r*DestinationKeySize;
                    setBit(Key, c);
                }
            }
        }

        // sorting
        #ifdef DEBUG
        // printf("\nUnsorted Keys\n");
        // printKeyValuePairs(BufferRowsCount, MaskSum, DestinationKeySize, KeysArray, IntermediateValStore);
        #endif

        // sort the Key and Value Arrays
        quickSort(KeysArray, IntermediateValStore, 0, BufferRowsCount - 1, DestinationKeySize);

        // reset the col store to make it unusable
        // memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);

        // free the col store keys
        free(IntermediateKeyStore);

        #ifdef DEBUG
        // printf("\nSorted Keys\n");
        // printKeyValuePairs(BufferRowsCount, MaskSum, DestinationKeySize, KeysArray, IntermediateValStore);
        #endif

        // merge the keys currently in the buffer and write them to a new Key Value Array that has key value pairs together
        byte *KeyValueArray = (byte*)calloc(BufferRowsCount, DestinationKeySize + ValueSize);
        size_t NewArrayLength = MergeKeysInPlace(BufferRowsCount, DestinationKeySize, ValueSize, KeysArray, IntermediateValStore, KeyValueArray);

        // free key array and value array
        free(KeysArray); free(IntermediateValStore);

        #ifdef DEBUG
        // printf("\nMerged Keys\n");
        // printKeyValuePairsFromKeyValArray(NewArrayLength, MaskSum, DestinationKeySize, ValueSize, KeyValueArray);
        #endif

        // write the runs to the disk
        /* 
        ArrayLength
        Key Value
        Key Value
        Key Value
        ...
        */
        // '0_' for the 0 th pass
        std::string TempRunFileName = TempRunFileNamePrefix + "0_" + std::to_string(i) + TempRunFileNameSuffix;

        FILE* TempRunFle;
        TempRunFle = fopen (TempRunFileName.c_str(), "wb");
        fwrite(&NewArrayLength, sizeof(size_t), 1, TempRunFle);
        fwrite(KeyValueArray, DestinationKeySize + ValueSize, NewArrayLength, TempRunFle);
        fclose(TempRunFle);

        #ifdef DEBUG
        printf("\nRead Merged Keys\n");
        FILE *ReadTempRunFile = fopen(TempRunFileName.c_str(), "r");
        assert(ReadTempRunFile != NULL);

        size_t ReadNewArrayLength;
        fread(&ReadNewArrayLength, sizeof(size_t), 1, ReadTempRunFile);
        printf("Array Length: %zu\n", ReadNewArrayLength);

        byte *ReadKeyValueArray = (byte*)calloc(ReadNewArrayLength, DestinationKeySize + ValueSize);
        fread(ReadKeyValueArray, DestinationKeySize + ValueSize, ReadNewArrayLength, ReadTempRunFile);
        fclose(ReadTempRunFile);

        printKeyValuePairsFromKeyValArray(ReadNewArrayLength, MaskSum, DestinationKeySize, ValueSize, ReadKeyValueArray);
        free(ReadKeyValueArray); 
        #endif
    }
    
    ExternalMerge(RehashIterations, TempRunFileNamePrefix, TempRunFileNameSuffix, PageRowsCount, DestinationKeySize, ValueSize, MaskSum);
    // merge
    // flush old col stores, and retreive the other part from the disk

    // write it to file

    // update the metadata file

    // mask offset + sum
    // actual mask
}

void fetch(std::string CubeID, unsigned int CuboidID);