#include "SecondaryStorage.h"
#include "ByteIntroSort.h"

std::string DatasetDirPath = "/Users/jayatila/Documents/dataset/";

byte *getKey(byte *array, size_t idx, size_t recSize) {
    byte *key = array + recSize * idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

value_t *getVal(byte *array, size_t idx, size_t recSize) {
    value_t *val = (value_t *) (array + (idx + 1) * recSize - sizeof(value_t));
//    printf("array = %d idx = %d recSize = %d val = %d\n", array, idx, recSize, val);
    return val;
}

void mkRandomSecondaryStorage (std::string CubeID, unsigned int NBits, unsigned int Rows) {
    const size_t oldKeyBits = NBits;
    size_t maxOldRows = 1<<oldKeyBits;
    unsigned int oldKeySize = bitsToBytes(oldKeyBits);
    unsigned int oldRecSize = oldKeySize + sizeof(value_t);
    byte *store = (byte *) calloc(maxOldRows, oldRecSize);

    srand(8);
    for (size_t i = 0; i<Rows; i++) { 
        value_t randVal = (value_t)rand()%10;
        size_t randKey = (size_t)rand()%1000;
        memcpy(getKey(store, i, oldRecSize), &randKey, oldKeySize);
        memcpy(getVal(store, i, oldRecSize), &randVal, sizeof(value_t));
    }

    const size_t oldRows = Rows;

    store = (byte *) realloc(store, oldRows*oldRecSize);

    //col source generation
    unsigned int oldColumnSize = bitsToBytes(oldRows);
    // printf("Old Rows Size: %u\n", oldColumnSize);

    // stores values
    value_t *oldValueStore = (value_t *) calloc(oldRows, sizeof(value_t));

    // stores keys 
    byte **oldColStore = (byte**)calloc(oldKeyBits, oldColumnSize);

    for (size_t r = 0; r<oldRows; r++) { 
        memcpy(&oldValueStore[r], getVal(store, r, oldRecSize), sizeof(value_t));
        byte *key = getKey(store, r, oldRecSize);
        value_t intKey;  // CAUTION; KEYS BIGGER THAN 64 BITS WILL NOT WORK
        memcpy(&intKey, key, oldKeySize); 
        std::bitset<sizeof(value_t)*8> bit_r = std::bitset<sizeof(value_t)*8>(intKey);
        for(int i = 0; i < oldKeyBits; i++) {
            if (bit_r[i]) {
                byte* col = getColumn(oldColStore, i, oldColumnSize);
                setBit(col, r);
            }
        }
    }

    std::string CubeDirPath = DatasetDirPath + CubeID + "/";
    std::string CuboidDirPath = CubeDirPath + "cuboid0/" ;
    printf("%s\n", CuboidDirPath.c_str());
    #ifdef DEBUG
    printf("%s\n", CuboidDirPath.c_str());
    #endif
    
    std::filesystem::create_directories(CuboidDirPath);

    // SAVE COL STORE
    for(int i=0; i<oldKeyBits; i++){
        std::string filename = CuboidDirPath + "col" + std::to_string(i);
        FILE* colfile;
        colfile = fopen(filename.c_str(), "wb");
        fwrite(getColumn(oldColStore, i, oldColumnSize), sizeof(byte), oldColumnSize, colfile);
        fclose(colfile);
    }

    // SAVE COL STORE VALUES
    FILE* valfile;
    std::string valfilename = CuboidDirPath + "values";
    valfile = fopen (valfilename.c_str(), "wb");
    fwrite(oldValueStore, sizeof(value_t), oldRows, valfile);
    fclose(valfile);

    IsDenseType IsDense = 0;

    size_t MetadataDize = sizeof(IsDenseType) + sizeof(size_t);
    
    byte *Metadata = (byte*)calloc(MetadataDize, sizeof(byte));
    memcpy(Metadata, &IsDense, sizeof(IsDenseType));
    memcpy(Metadata + sizeof(IsDenseType), &oldRows, sizeof(size_t));

    printf("IsDense: %hhd\n", *(IsDenseType*)(Metadata));
    printf("Rows: %zu\n", *(size_t*)(Metadata + sizeof(IsDenseType)));

    std::string MetadataFileName = CubeDirPath + "metadata";

    FILE* MetadataFile;
    MetadataFile = fopen (MetadataFileName.c_str(), "wb");
    fwrite(Metadata, sizeof(byte), MetadataDize, MetadataFile);
    fclose(MetadataFile);
}

void writeBaseCuboid(std::string CubeID, std::pair<byte[], long> KeyValuePairs[]) {
    
}

// TODO: ADD A FLAG FOR TEMPORARY FOR QUERIES
// Assume resulting detination cuboid always fits in memory
/*
void rehashToDense(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int Mask[], const unsigned int MaskSum) {
    // add a cache
    // MAKE IT MODULAR FOR DIFFERENT LOADING POLICIES

    // read metadata about the source cuboid 
    const unsigned int ValueSize = sizeof(value_t);

    std::string CubeDirPath = DatasetDirPath + CubeID + std::filesystem::path::preferred_separator;
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
}*/

void rehashToSparse(std::string CubeID, unsigned int SourceCuboidID, unsigned int DestinationCuboidID, unsigned int* Mask, unsigned int MaskSum) {    
    std::string CubeDirPath = DatasetDirPath + CubeID + std::filesystem::path::preferred_separator;
    std::string SourceCuboidDirPath = CubeDirPath + "cuboid" + std::to_string(SourceCuboidID) + std::filesystem::path::preferred_separator;

    Metadata SourceMetadata = getMetadata(CubeDirPath, SourceCuboidID, MaskSum);
    
    const size_t OldRowsCount = SourceMetadata.RowsCount;
    unsigned int OldColumnSize = SourceMetadata.ColumnSize; // size of a column in the bit column store 
    
    const size_t PageRowsCount = SourceMetadata.PageRowsCount;
    size_t BufferRowsCount = PageRowsCount * BufferPages / 2; // half the buffer pages will be left for the output, as well as column to row key conversion
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);
    
    unsigned int DestinationKeySize = SourceMetadata.DestinationKeySize;
    const unsigned int RecSize = SourceMetadata.RecSize;

    // start rehashing the cuboid stored on disk
    unsigned int RehashIterations = (OldRowsCount + BufferRowsCount - 1) / BufferRowsCount; // ceil(OldRowsCount /BufferRowsCount)

    #ifdef DEBUG
        printf("\nMaskSum : %d\n", MaskSum);
        printf("Mask: {");
        for (int m=0; m<MaskSum-1; m++) {
            printf("%d, ", Mask[m]);
        }
        printf("%d}\n", Mask[MaskSum-1]);
        printf("Buffer Rows Count : %zu\n", BufferRowsCount);
        printf("Rehash Iterations : %d\n", RehashIterations);
    #endif
    // TODO: Add a hybrid algorithm

    // create the destination cuboid
    std::string DestCuboidDirPath = CubeDirPath + "cuboid" + std::to_string(DestinationCuboidID) + std::filesystem::path::preferred_separator;
    std::filesystem::create_directories(DestCuboidDirPath);

    std::string TempRunFileNamePrefix = DestCuboidDirPath + "run";
    std::string TempRunFileNameSuffix = ".temp";

    for (int i = 0; i < RehashIterations; i++) {
        if (i == RehashIterations - 1) {
            BufferRowsCount = OldRowsCount - ((RehashIterations - 1) * BufferRowsCount);
            printf("\nBuffer Rows Count : %zu\n", BufferRowsCount);
        }
        // read columns
        // memset(IntermediateKeyStore, 0, MaskSum*BufferColumnSize);
        byte **IntermediateKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
        readKeys(MaskSum, Mask, i, SourceCuboidDirPath, BufferRowsCount, IntermediateKeyStore, BufferColumnSize);
        
        // read values
        // memset(IntermediateValStore, 0, BufferRowsCount*ValueSize);
        value_t *IntermediateValStore = (value_t *)calloc(BufferRowsCount, sizeof(value_t));
        readValues(SourceCuboidDirPath, i, BufferRowsCount, IntermediateValStore);

        // write them to a new Key Value Array that has key value pairs together
        byte *KeyValueArray = (byte*)calloc(BufferRowsCount, RecSize);

        // set keys and values 
        for (size_t r = 0; r < BufferRowsCount; r++) {
            memcpy(getValueFromKeyValArray(KeyValueArray, r, RecSize), getValueFromValueArray(IntermediateValStore, r), sizeof(value_t));
            for(int c = 0; c < MaskSum; c++) {       
                if (getBit(getColumn(IntermediateKeyStore, c, BufferColumnSize), r)) {
                    byte* Key = getKeyFromKeyValArray(KeyValueArray, r, RecSize);
                    setBit(Key, c);
                }
            }
        }

        // free the col store keys and values
        free(IntermediateKeyStore); free(IntermediateValStore);

        // sorting
        #ifdef DEBUG
        printf("\nUnsorted Keys\n");
        printKeyValuePairsFromKeyValArray(BufferRowsCount, MaskSum, DestinationKeySize, sizeof(value_t), KeyValueArray);
        #endif

        // sort the Key Value Array
        byteIntrosort(KeyValueArray, 0, BufferRowsCount - 1, RecSize);

        #ifdef DEBUG
        printf("\nSorted Keys\n");
        printKeyValuePairsFromKeyValArray(BufferRowsCount, MaskSum, DestinationKeySize, sizeof(value_t), KeyValueArray);
        #endif

        // merge the keys currently in the buffer and write them to a new Key Value Array that has key value pairs together
        size_t MergedArrayLength = MergeKeysInPlace(BufferRowsCount, RecSize, KeyValueArray);
        
        KeyValueArray = (byte *)realloc(KeyValueArray, MergedArrayLength * RecSize);

        #ifdef DEBUG
        printf("\nMerged Keys\n");
        printKeyValuePairsFromKeyValArray(MergedArrayLength, MaskSum, DestinationKeySize, sizeof(value_t), KeyValueArray);
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
        fwrite(&MergedArrayLength, sizeof(size_t), 1, TempRunFile);
        fwrite(KeyValueArray, RecSize, MergedArrayLength, TempRunFile);
        fclose(TempRunFile);

        free(KeyValueArray);

        #ifdef DEBUG
        printf("\nRead Merged Keys\n");
        FILE *ReadTempRunFile = fopen(TempRunFileName.c_str(), "r");
        assert(ReadTempRunFile != NULL);

        size_t ReadMergedArrayLength;
        fread(&ReadMergedArrayLength, sizeof(size_t), 1, ReadTempRunFile);
        printf("Array Length: %zu\n", ReadMergedArrayLength);

        byte *ReadKeyValueArray = (byte*)calloc(ReadMergedArrayLength, RecSize);
        fread(ReadKeyValueArray, RecSize, ReadMergedArrayLength, ReadTempRunFile);
        fclose(ReadTempRunFile);

        printKeyValuePairsFromKeyValArray(ReadMergedArrayLength, MaskSum, DestinationKeySize, sizeof(value_t), ReadKeyValueArray);
        free(ReadKeyValueArray); 
        #endif
    }
    
    // external merge
    unsigned int PassNumber = ExternalMerge(RehashIterations, TempRunFileNamePrefix, TempRunFileNameSuffix, PageRowsCount, DestinationKeySize, sizeof(value_t), MaskSum);

    #ifdef DEBUG
    printf("\nPassNumber: %d\n", PassNumber);
    #endif

    // convert the row store to column store again

    // read the final output run file
    std::string RowRunFileName = TempRunFileNamePrefix + std::to_string(PassNumber) + "_" + std::to_string(0) + TempRunFileNameSuffix;
    FILE *RowRunFile = fopen(RowRunFileName.c_str(), "rb");
     
    // get the number of rows
    size_t NewRowsCount;
    fread(&NewRowsCount, sizeof(size_t), 1, RowRunFile);

    // calculate number of row to column conversion iterations
    unsigned int ConvertIterations = (NewRowsCount + BufferRowsCount - 1) / BufferRowsCount; // ceil(NewRowsCount /BufferRowsCount)

    // input key value array
    byte *RowKeyValueArray = (byte*)calloc(BufferRowsCount, DestinationKeySize + sizeof(value_t));

    // output column store
    byte **OutputKeyStore = (byte**)calloc(MaskSum, BufferColumnSize);
    value_t *OutputValStore = (value_t *)calloc(BufferRowsCount, sizeof(value_t));

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
        
        memset(RowKeyValueArray, 0, (DestinationKeySize + sizeof(value_t)) * BufferRowsCount);
        fread(RowKeyValueArray, DestinationKeySize + sizeof(value_t), RowsToRead, RowRunFile);

        memset(OutputValStore, 0, sizeof(value_t) * BufferRowsCount);
        for (int j = 0; j < MaskSum; j++) {
            memset(getColumn(OutputKeyStore, j, BufferColumnSize), 0, BufferColumnSize);
        }

        // convert to column stores
        convertKeyValArrayToColStore(RowsToRead, RowKeyValueArray, OutputKeyStore, OutputValStore, DestinationKeySize, sizeof(value_t), MaskSum, BufferColumnSize);
    
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

    printf("Key Value Pairs\n");
    for (size_t i = 0; i<NewRowsCount; i++) { 
        printf("\tKey: ");
        for(int j = MaskSum-1; j >= 0; j--)
            printColumnBit(i, getColumn(readcolstore, j, NewColSize));

        printf("\tValue: %ld\n", readvaluestore[i]);
    }
    #endif
}

void fetch(std::string CubeID, unsigned int CuboidID);