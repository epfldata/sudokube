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

    Metadata SourceMetadata = getMetadata(CubeDirPath, SourceCuboidID, MaskSum);
    
    const size_t OldRowsCount = SourceMetadata.RowsCount;
    unsigned int OldColumnSize = SourceMetadata.ColumnSize; // size of a column in the bit column store 
    
    const size_t PageRowsCount = SourceMetadata.PageRowsCount;
    const size_t BufferRowsCount = PageRowsCount * BufferPages / 2; // half the buffer pages will be left for the output, as well as column to row key conversion
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);
    
    unsigned int DestinationKeySize = SourceMetadata.DestinationKeySize;
    const unsigned int ValueSize = SourceMetadata.ValueSize;

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

        FILE* TempRunFile;
        TempRunFile = fopen (TempRunFileName.c_str(), "wb");
        fwrite(&NewArrayLength, sizeof(size_t), 1, TempRunFile);
        fwrite(KeyValueArray, DestinationKeySize + ValueSize, NewArrayLength, TempRunFile);
        fclose(TempRunFile);

        free(KeyValueArray);

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
    
    // external merge
    unsigned int PassNumber = ExternalMerge(RehashIterations, TempRunFileNamePrefix, TempRunFileNameSuffix, PageRowsCount, DestinationKeySize, ValueSize, MaskSum);
    printf("PassNumber: %d", PassNumber);

    // convert the row store to column store again

    // read the final output run file
    std::string RowRunFileName = TempRunFileNamePrefix + std::to_string(PassNumber) + "_" + std::to_string(0) + TempRunFileNameSuffix;
    printf("RowRunFileName: %s", RowRunFileName.c_str());
    FILE *RowRunFile = fopen(RowRunFileName.c_str(), "rb");
     
    // get the number of rows
    size_t NewRowsCount;
    fread(&NewRowsCount, sizeof(size_t), 1, RowRunFile);

    // calculate number of row to column conversion iterations
    unsigned int ConvertIterations = (NewRowsCount + BufferRowsCount - 1) / BufferRowsCount; // ceil(NewRowsCount /BufferRowsCount)

    // input key value array
    byte *RowKeyValueArray = (byte*)calloc(BufferRowsCount, DestinationKeySize + ValueSize);

    // output column store
    byte **OutputKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
    value_t *OutputValStore = (value_t *)calloc(BufferRowsCount, ValueSize);

    // open output files
    std::vector<FILE*> OutputFilePointers;

    for (int col = 0; col < MaskSum; col++) {
        std::string ColWriteFileName = DestCuboidDirPath + "col" + std::to_string(col);
        FILE* ColWriteFile = fopen (ColWriteFileName.c_str(), "wb");
        assert(ColWriteFile != NULL);
        OutputFilePointers.push_back(ColWriteFile);
    }

    std::string OutputValueFileName = DestCuboidDirPath + "values";
    FILE *OutputValueFile = fopen(OutputValueFileName.c_str(), "wb");
    assert(OutputValueFile != NULL);

    for (int i = 0; i < ConvertIterations; i++) {
        size_t RowsToRead = BufferRowsCount;
        if (i == ConvertIterations - 1) RowsToRead = NewRowsCount - (BufferRowsCount * i);
        
        memset(RowKeyValueArray, 0, (DestinationKeySize + ValueSize) * BufferRowsCount);
        fread(RowKeyValueArray, DestinationKeySize + ValueSize, RowsToRead, RowRunFile);

        memset(OutputValStore, 0, ValueSize * BufferRowsCount);
        for (int j = 0; j < MaskSum; j++) {
            memset(getColumn(OutputKeyStore, j, BufferColumnSize), 0, BufferColumnSize);
        }

        // convert to column stores
        convertKeyValArrayToColStore(RowsToRead, RowKeyValueArray, OutputKeyStore, OutputValStore, DestinationKeySize, ValueSize, MaskSum, BufferColumnSize);
    
        writeKeys(MaskSum, OutputKeyStore, OutputFilePointers, BufferColumnSize);
        writeValues(OutputValStore, OutputValueFile, BufferRowsCount);
    }

    for (int col = 0; col < MaskSum; col++) {
        fclose(OutputFilePointers[col]);
    }

    fclose(OutputValueFile);

    fclose(RowRunFile);
    std::filesystem::remove(RowRunFileName);

    // update metadata
    IsDenseType DestinationIsDense = 0;
    writeMetadata(CubeDirPath, DestinationCuboidID, DestinationIsDense, NewRowsCount);

    #ifdef DEBUG
    // READ COL STORE
    size_t NewColSize = bitsToBytes(NewRowsCount);
    byte **readcolstore = (byte**)calloc(MaskSum, NewColSize);

    for(int i=0; i<MaskSum; i++){
        std::string filename = DestCuboidDirPath + "col" + std::to_string(i);
        FILE* colreadfile = fopen (filename.c_str(), "r");
        assert(colreadfile != NULL);
        fread(getColumn(readcolstore, i, NewColSize), sizeof(byte), NewColSize, colreadfile);
        fclose (colreadfile);
    }

    // READ COL STORE VALUES
    value_t *readvaluestore = (value_t *) calloc(NewRowsCount, sizeof(value_t));
    FILE *readvaluefile = fopen(OutputValueFileName.c_str(), "r");
    assert(readvaluefile != NULL);
    fread(readvaluestore, sizeof(value_t), NewRowsCount, readvaluefile);
    fclose(readvaluefile);

    printf("Old Key Value Pairs\n");
    for (size_t i = 0; i<NewRowsCount; i++) { 
        printf("\tCol Read Key: ");
        for(int j = MaskSum-1; j >= 0; j--)
            printColumnBit(i, getColumn(readcolstore, j, NewColSize));

        printf("\tCol Read Value: %lld\n", readvaluestore[i]);
    }
    #endif
    // delete key value file
    // update the metadata file

    // mask offset + sum
    // actual mask
}

void fetch(std::string CubeID, unsigned int CuboidID);