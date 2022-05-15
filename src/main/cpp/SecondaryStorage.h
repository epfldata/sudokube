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

inline void print_column_bit(int pos, byte *column) {
    int b = (column[pos / 8] >> (pos % 8)) % 2;
    if(b) printf("1");
    else  printf("0");
}

// returns True if the given bit is 1
inline bool getBit(byte* Column, int Position) {
    unsigned int BytePos = Position >> 3;  
    unsigned int BitPos = Position & 0x7;

    return Column[BytePos] & 1 << BitPos;
}

// sets the bit to 1
void setBit(byte*& column, int position) {
    unsigned int byte_pos = position >> 3;  
    unsigned int bit_pos = position & 0x7;

    column[byte_pos] |= 1 << bit_pos;
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
