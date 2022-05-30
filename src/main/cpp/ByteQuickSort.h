#ifndef QUICKSORT_H
#define QUICKSORT_H

#include "Utility.h"

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
of pivot

int memcmp ( const void * ptr1, const void * ptr2, size_t num );
<0	the first byte that does not match in both memory blocks has a lower value in ptr1 than in ptr2 (if evaluated as unsigned char values)
0	the contents of both memory blocks are equal
>0	the first byte that does not match in both memory blocks has a greater value in ptr1 than in ptr2 (if evaluated as unsigned char values)
 */
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

#endif