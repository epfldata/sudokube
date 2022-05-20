#include <string>
#include <filesystem>
#include "Keys.h"

using CuboidIDType = int64_t;
using MaskOffsetType = int64_t;
using IsDenseType = int8_t;
size_t MetadataSizeOnDisk = sizeof(IsDenseType) + sizeof(size_t);

unsigned int PageSize = 128;//4*1024;
unsigned int BufferPages = 16;
unsigned int BufferSize = BufferPages*PageSize;

struct Metadata {
    const size_t RowsCount;
    unsigned int ColumnSize; 
    const size_t PageRowsCount;
    unsigned int DestinationKeySize;
    const unsigned int ValueSize;
};

inline byte *getColumn(byte **Array, size_t Idx, size_t OldColumnSize) {
    byte *Column = (byte *)Array + OldColumnSize * Idx;
    return Column;
}

inline void print_column_bit(int pos, byte *column) {
    int b = (column[pos / 8] >> (pos % 8)) % 2;
    if(b) printf("1");
    else  printf("0");
}

// returns True if the given bit is 1
inline bool getBit(byte* Array, int Position) {
    unsigned int BytePos = Position >> 3;  
    unsigned int BitPos = Position & 0x7;

    return Array[BytePos] & 1 << BitPos;
}

// sets the bit to 1
inline void setBit(byte*& Array, int Position) {
    unsigned int BytePos = Position >> 3;  
    unsigned int BitPos = Position & 0x7;

    Array[BytePos] |= 1 << BitPos;
}

// reads keys from column store
inline void readKeys(unsigned int MaskSum, unsigned int Mask[], int Iteration, std::string CuboidDirPath, const size_t BufferRowsCount, byte **IntermediateKeyStore, unsigned int BufferColumnSize) {
    for(int j=0; j<MaskSum; j++){
        std::string ColReadFileName = CuboidDirPath + "col" + std::to_string(Mask[j]);
        FILE* ColReadFile = fopen (ColReadFileName.c_str(), "r");
        assert(ColReadFile != NULL);
        fseek(ColReadFile, Iteration*BufferRowsCount/8, SEEK_SET);
        fread(getColumn(IntermediateKeyStore, j, BufferColumnSize), sizeof(byte), BufferColumnSize, ColReadFile);
        fclose (ColReadFile);
    }
}

inline void readValues(std::string CuboidDirPath, int Iteration, const size_t BufferRowsCount, value_t *IntermediateValStore) {
    std::string ValueFileName = CuboidDirPath + "values";
    FILE *ReadValueFile = fopen(ValueFileName.c_str(), "r");
    assert(ReadValueFile != NULL);
    fseek(ReadValueFile, Iteration*BufferRowsCount*sizeof(value_t), SEEK_SET);
    fread(IntermediateValStore, sizeof(value_t), BufferRowsCount, ReadValueFile);
    fclose(ReadValueFile);
}

inline Metadata getMetadata(std::string CubeDirPath, const unsigned int MaskSum) {
    std::string MetadataFilePath = CubeDirPath + "/metadata";

    byte *MetadataOnDisk = (byte*)calloc(MetadataSizeOnDisk, sizeof(byte));

    FILE* MetadataFile = fopen (MetadataFilePath.c_str(), "r");
    assert(MetadataFile != NULL);
    fread(MetadataOnDisk, sizeof(byte), MetadataSizeOnDisk, MetadataFile);
    fclose(MetadataFile);

    const size_t OldRowsCount = *(size_t*)(MetadataOnDisk + sizeof(IsDenseType));
    unsigned int OldColumnSize = bitsToBytes(OldRowsCount);

    unsigned int DestinationKeySize = bitsToBytes(MaskSum);
    const unsigned int ValueSize = sizeof(value_t);
    
    // number of key value pairs that can fit in a page at a time
    const size_t PageRowsCount = ((PageSize / (DestinationKeySize + ValueSize)) >> 3 ) << 3;
    
    Metadata Metadata = {OldRowsCount, OldColumnSize, PageRowsCount, DestinationKeySize, ValueSize};

    printf("IsDense: %hhd\n", *(IsDenseType*)(MetadataOnDisk));
    printf("OldRowsCount: %zu\n", OldRowsCount);
    printf("OldColumnSize: %d\n", OldColumnSize);

    printf("PageRowsCount: %zu\n", PageRowsCount);

    free(MetadataOnDisk);

    return Metadata;
}

// Quick Sort 
byte *getKeyFromKeysArray(byte *array, size_t idx, size_t keySize) {
    byte *key = array + keySize * idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

value_t *getValueFromValueArray(value_t *array, size_t idx) {
    value_t *value = array + idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return value;
}

// A utility function to swap two elements
void swapKeys(byte *KeysArray, value_t *ValuesArray, int i, int j, unsigned int KeySize) {
	byte *TempKey = (byte *)calloc(1, KeySize);
    value_t *TempVal = (value_t *)calloc(1, sizeof(value_t));
    
    memcpy(TempKey, getKeyFromKeysArray(KeysArray, i, KeySize), KeySize);
    memcpy(TempVal, getValueFromValueArray(ValuesArray, i), sizeof(value_t));
    
    memcpy(getKeyFromKeysArray(KeysArray, i, KeySize), getKeyFromKeysArray(KeysArray,j, KeySize), KeySize);
    memcpy(getValueFromValueArray(ValuesArray, i), getValueFromValueArray(ValuesArray, j), sizeof(value_t));
    
    memcpy(getKeyFromKeysArray(KeysArray, j, KeySize), TempKey, KeySize);
    memcpy(getValueFromValueArray(ValuesArray, j), TempVal, sizeof(value_t));
	
    free(TempKey);
    free(TempVal);
}

/* This function takes last element as pivot, places
the pivot element at its correct position in sorted
array, and places all smaller (smaller than pivot)
to left of pivot and all greater elements to right
of pivot */
int partition(byte *KeysArray, value_t *ValuesArray, int Start, int End, unsigned int KeySize) {
    
    byte *pivot = getKeyFromKeysArray(KeysArray, End, KeySize);
	int i =  Start - 1; // Index of smaller element and indicates the right position of pivot found so far

	for (int j = Start; j <= End - 1; j++) {
		// If current element is smaller than the pivot
        if (memcmp(getKeyFromKeysArray(KeysArray, j, KeySize), pivot, KeySize) < 0) {
            i++;
            swapKeys(KeysArray, ValuesArray, i, j, KeySize);
        }
	}
    swapKeys(KeysArray, ValuesArray, i+1, End, KeySize);
	return (i + 1);
}

/* The main function that implements QuickSort
Array --> array to be sorted, Start --> starting index,
End --> ending index */
void quickSort(byte *KeysArray, value_t *ValuesArray, int Start, int End, unsigned int KeySize) {
	if  (Start < End) {
		// KeysArray[PartitionIndex] is now at right place
		int PartitionIndex = partition(KeysArray, ValuesArray, Start, End, KeySize);

		// Separately sort elements before partition and after partition
		quickSort(KeysArray, ValuesArray, Start, PartitionIndex  - 1, KeySize);
		quickSort(KeysArray, ValuesArray, PartitionIndex  + 1, End, KeySize);
	}
}

inline void printKeyValuePairs(const size_t RowsCount, unsigned int MaskSum, unsigned int KeySize, byte *KeysArray, value_t *ValuesArray) {
    for (size_t i = 0; i<RowsCount; i++) { 
        print_key(MaskSum, getKeyFromKeysArray(KeysArray, i, KeySize));
        printf(" : %lld", *getValueFromValueArray(ValuesArray, i));
        printf("\n");
    }
}

byte *getKeyFromKeyValArray(byte *array, size_t idx, size_t keySize, size_t valSize) {
    byte *key = array + (keySize+valSize) * idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

byte *getValueFromKeyValArray(byte *array, size_t idx, size_t keySize, size_t valSize) {
    byte *val = array + (keySize+valSize) * idx + keySize;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return val;
}

inline void printKeyValuePairsFromKeyValArray(const size_t RowsCount, unsigned int MaskSum, unsigned int KeySize, unsigned int ValueSize, byte *KeyValueArray) {
    for (size_t i = 0; i<RowsCount; i++) { 
        print_key(MaskSum, getKeyFromKeyValArray(KeyValueArray, i, KeySize, ValueSize));
        printf(" : %lld", (value_t)*getValueFromKeyValArray(KeyValueArray, i, KeySize, ValueSize));
        printf("\n");
    }
}

inline size_t MergeKeysInPlace(const size_t BufferRowsCount, unsigned int KeySize, unsigned int ValueSize, byte *KeysArray, value_t *ValuesArray, byte* KeyValueArray) {
    size_t j = 0; // index of last elemet
    memcpy(getKeyFromKeyValArray(KeyValueArray, j, KeySize, ValueSize), getKeyFromKeysArray(KeysArray, 0, KeySize), KeySize);
    memcpy(getValueFromKeyValArray(KeyValueArray, j, KeySize, ValueSize), getValueFromValueArray(ValuesArray, 0), ValueSize);
    for (size_t i = 1; i<BufferRowsCount; i++) {
        if (memcmp(getKeyFromKeyValArray(KeyValueArray, j, KeySize, ValueSize), getKeyFromKeysArray(KeysArray, i, KeySize), KeySize) == 0) {
            *getValueFromKeyValArray(KeyValueArray, j, KeySize, ValueSize) += *getValueFromValueArray(ValuesArray, i);
        } else {
            j++;
            memcpy(getKeyFromKeyValArray(KeyValueArray, j, KeySize, ValueSize), getKeyFromKeysArray(KeysArray, i, KeySize), KeySize);
            memcpy(getValueFromKeyValArray(KeyValueArray, j, KeySize, ValueSize), getValueFromValueArray(ValuesArray, i), ValueSize);
        }
    }
    return j + 1; // returns the new length of the array after merge
}

inline void ExternalMerge(unsigned int NumberOfRuns, std::string RunFileNamePrefix, std::string RunFileNameSuffix, const size_t PageRowsCount, unsigned int KeySize, unsigned int ValueSize, unsigned int MaskSum) {
    // K = (BufferPages-1) - Way Merge
    unsigned int K = BufferPages - 1;
    printf("\nPage Rows Count: %zu\n", PageRowsCount);

    while (NumberOfRuns > 1) {
        for (int runs = 0; runs < NumberOfRuns; runs += K) {
            int RunsUpperBound = std::min(runs+K, NumberOfRuns);
            int NumberOfRuns = RunsUpperBound-runs;
            size_t *RemainingRowsInRun = (size_t *)calloc(NumberOfRuns, sizeof(size_t));
            byte *KeyValuePages = (byte*)calloc(NumberOfRuns, PageSize);
            
            for (int run = 0; run < NumberOfRuns; run++) {
                std::string RunFileName = RunFileNamePrefix + std::to_string(runs+run) + RunFileNameSuffix;
                FILE *RunFile = fopen(RunFileName.c_str(), "r");
                assert(RunFile != NULL);

                size_t RunRowsCount;
                fread(RemainingRowsInRun + (run), sizeof(size_t), 1, RunFile);

                size_t RowsToRead = std::min(RunRowsCount, PageRowsCount);
                fread(KeyValuePages+run*PageSize, KeySize + ValueSize, RowsToRead, RunFile);

                #ifdef DEBUG
                printf("\nRunRowsCount: %zu\n", RemainingRowsInRun[run]);
                printKeyValuePairsFromKeyValArray(RowsToRead, MaskSum, KeySize, ValueSize, KeyValuePages+run*PageSize);
                #endif
            }

            free(RemainingRowsInRun);
            free(KeyValuePages);
        }
        NumberOfRuns = 0;
    }

        // while (number of runs in the previous pass > 1):
        //     while there are runs to be merged from the previous pass:
        //         Choose next B - 1 runs.
        //         Read each run into an input buffer; page at a time with double buffering.
        //         Merge the runs and write to the output buffer;
        //         write output buffer to disk one page at a time.

}
