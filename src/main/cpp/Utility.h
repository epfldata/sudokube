#include <algorithm>  

#ifndef UTILITY_H
#define UTILITY_H

typedef unsigned char byte;
typedef int64_t value_t; 

using CuboidIDType = int64_t;
using MaskOffsetType = int64_t;
using IsDenseType = int8_t;
size_t MetadataSizeOnDisk = sizeof(IsDenseType) + sizeof(size_t);

unsigned int PageSize = 4*1024; // 128; //128;//4*1024;
unsigned int BufferPages = 64;
unsigned int BufferSize = BufferPages*PageSize;

inline unsigned int bitsToBytes(unsigned int bits) {
    return (bits + 7) >> 3;
}

inline void print_key(int n_bits, byte *key) {
    for(int pos = n_bits - 1; pos >= 0; pos--) {
        int b = (key[pos / 8] >> (pos % 8)) % 2;
        if(b) printf("1");
        else  printf("0");
        if(pos % 8 == 0)
            printf(" ");
    }
}

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

bool memoryIsAllZeroes(byte* Array, size_t const Bytes) {
    return std::all_of(Array, Array + Bytes, [](byte Byte) { return Byte == 0; } );
}

byte *getKeyFromKeyValArray(byte *Array, size_t Index, size_t RecSize) {
    byte *Key = Array + (RecSize) * Index;
    return Key;
}

value_t *getValueFromKeyValArray(byte *Array, size_t Index, size_t RecSize) {
    value_t *Value = (value_t*)(Array + (RecSize) * Index + (RecSize - sizeof(value_t)));
    return Value;
}

inline void printKeyValuePairs(const size_t RowsCount, unsigned int MaskSum, unsigned int KeySize, byte *KeysArray, value_t *ValuesArray) {
    for (size_t i = 0; i<RowsCount; i++) { 
        print_key(MaskSum, getKeyFromKeysArray(KeysArray, i, KeySize));
        printf(" : %ld", *getValueFromValueArray(ValuesArray, i));
        printf("\n");
    }
}


#endif