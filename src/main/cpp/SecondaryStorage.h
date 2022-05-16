#include <string>
#include <filesystem>
#include "Keys.h"

using CuboidIDType = int64_t;
using MaskOffsetType = int64_t;
using IsDenseType = int8_t;
size_t MetadataSizeOnDisk = sizeof(IsDenseType) + sizeof(size_t);
unsigned int BufferSize = 1024;

struct Metadata {
    const size_t OldRowsCount;
    unsigned int OldColumnSize; 
    const size_t BufferRowsCount; 
    unsigned int BufferColumnSize;
    unsigned int KeySize;
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

    unsigned int KeySize = bitsToBytes(MaskSum);
    
    // number of rows that can fit in the buffer at a time
    const size_t BufferRowsCount = ((BufferSize / (bitsToBytes(MaskSum) + sizeof(value_t))) >> 3 ) << 3;
    unsigned int BufferColumnSize = bitsToBytes(BufferRowsCount);
    
    Metadata Metadata = {OldRowsCount, OldColumnSize, BufferRowsCount, BufferColumnSize, KeySize};

    printf("IsDense: %hhd\n", *(IsDenseType*)(MetadataOnDisk));
    printf("OldRowsCount: %zu\n", OldRowsCount);
    printf("OldColumnSize: %d\n", OldColumnSize);

    printf("BufferRowsCount: %zu\n", BufferRowsCount);
    printf("BufferColumnSize: %d\n", BufferColumnSize);

    return Metadata;
}

// Quick Sort 
byte *getKeyForSort(byte *array, size_t idx, size_t keySize) {
    byte *key = array + keySize * idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

value_t *getValueForSort(value_t *array, size_t idx) {
    value_t *value = array + idx;
    // printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return value;
}

// A utility function to swap two elements
void swapKeys(byte *KeysArray, value_t *ValuesArray, int i, int j, unsigned int KeySize) {
	byte *TempKey = (byte *)calloc(1, KeySize);
    value_t *TempVal = (value_t *)calloc(1, sizeof(value_t));
    
    memcpy(TempKey, getKeyForSort(KeysArray, i, KeySize), KeySize);
    memcpy(TempVal, getValueForSort(ValuesArray, i), sizeof(value_t));
    
    memcpy(getKeyForSort(KeysArray, i, KeySize), getKeyForSort(KeysArray,j, KeySize), KeySize);
    memcpy(getValueForSort(ValuesArray, i), getValueForSort(ValuesArray, j), sizeof(value_t));
    
    memcpy(getKeyForSort(KeysArray, j, KeySize), TempKey, KeySize);
    memcpy(getValueForSort(ValuesArray, j), TempVal, sizeof(value_t));
	
    free(TempKey);
    free(TempVal);
}

/* This function takes last element as pivot, places
the pivot element at its correct position in sorted
array, and places all smaller (smaller than pivot)
to left of pivot and all greater elements to right
of pivot */
int partition(byte *KeysArray, value_t *ValuesArray, int Start, int End, unsigned int KeySize) {
    
    byte *pivot = getKeyForSort(KeysArray, End, KeySize);
	int i =  Start - 1; // Index of smaller element and indicates the right position of pivot found so far

	for (int j = Start; j <= End - 1; j++) {
		// If current element is smaller than the pivot
        if (memcmp(getKeyForSort(KeysArray, j, KeySize), pivot, KeySize) < 0) {
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

inline void printKeyValuePairs(const size_t BufferRowsCount, unsigned int MaskSum, unsigned int KeySize, byte *KeysArray, value_t *ValuesArray) {
    for (size_t i = 0; i<BufferRowsCount; i++) { 
        print_key(MaskSum, getKeyForSort(KeysArray, i, KeySize));
        printf(" : %lld", *getValueForSort(ValuesArray, i));
        printf("\n");
    }
}

inline size_t MergeKeysInPlace(const size_t BufferRowsCount, unsigned int KeySize, byte *KeysArray, value_t *ValuesArray) {
    size_t j = 0; // index of last value
    for (size_t i = 1; i<BufferRowsCount; i++) {
        if (memcmp(getKeyForSort(KeysArray, i, KeySize), getKeyForSort(KeysArray, j, KeySize), KeySize) == 0) {
            *getValueForSort(ValuesArray, j) += *getValueForSort(ValuesArray, i);
        } else {
            j++;
            memcpy(getValueForSort(ValuesArray, j), getValueForSort(ValuesArray, i), sizeof(value_t));
            memcpy(getKeyForSort(KeysArray, j, KeySize), getKeyForSort(KeysArray, i, KeySize), KeySize);
        }
    }
    return j;
}
