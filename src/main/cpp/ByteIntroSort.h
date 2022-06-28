#ifndef INTROSORT_H
#define INTROSORT_H

#include <math.h>  
#include "Utility.h"

inline bool byteKeyGreaterThan(byte* Key1, byte* Key2, unsigned int KeySize) {
    for (int i = KeySize-1; i >= 0; i--) {
        int MemCmpVal = memcmp(Key1 + i, Key2 + i, 1);
        if (MemCmpVal > 0) return true;
        else if (MemCmpVal < 0) return false;    
    }
    return false;
}

inline bool byteKeyLessThan(byte* Key1, byte* Key2, unsigned int KeySize) {
    for (int i = KeySize-1; i >= 0; i--) {
        int MemCmpVal = memcmp(Key1 + i, Key2 + i, 1);
        if (MemCmpVal < 0) return true;
        else if (MemCmpVal > 0) return false;  
    }
    return false;
}

int medianOfThree(byte *KeyValArray, int a, int b, int c, unsigned int RecSize) {
    byte *aKey = getKeyFromKeyValArray(KeyValArray, a, RecSize);
    byte *bKey = getKeyFromKeyValArray(KeyValArray, b, RecSize);
    byte *cKey = getKeyFromKeyValArray(KeyValArray, c, RecSize);

    unsigned int KeySize = RecSize - sizeof(value_t);
    if (byteKeyGreaterThan(aKey, bKey, KeySize) ^ byteKeyGreaterThan(aKey, cKey, KeySize)) 
        return a;
    else if (byteKeyLessThan(bKey, aKey, KeySize) ^ byteKeyLessThan(bKey, cKey, KeySize)) 
        return b;
    else
        return c;
}

// A utility function to swap two elements
void swapKeys(byte *KeyValArray, int i, int j, unsigned int RecSize) {
	byte *TempKeyVal = (byte *)calloc(1, RecSize);
    
    memcpy(TempKeyVal, getKeyFromKeyValArray(KeyValArray, i, RecSize), RecSize);
    
    memcpy(getKeyFromKeyValArray(KeyValArray, i, RecSize), getKeyFromKeyValArray(KeyValArray,j, RecSize), RecSize);
    
    memcpy(getKeyFromKeyValArray(KeyValArray, j, RecSize), TempKeyVal, RecSize);
	
    free(TempKeyVal);
}

/* Function to sort an array using insertion sort*/
void byteInsertionSort(byte *KeyValArray, int Start, int End, unsigned int RecSize) {

    unsigned int KeySize = RecSize - sizeof(value_t);
	for (int i = Start + 1; i < End+1; i++) {
        byte *CurrentKeyVal = (byte *)calloc(1, RecSize);

        memcpy(CurrentKeyVal, getKeyFromKeyValArray(KeyValArray, i, RecSize), RecSize);
		
        //int key = arr[i];
		int j = i-1;

		while (j >= Start && byteKeyGreaterThan(getKeyFromKeyValArray(KeyValArray, j, RecSize), CurrentKeyVal, KeySize)) {
			memcpy(getKeyFromKeyValArray(KeyValArray, j+1, RecSize), getKeyFromKeyValArray(KeyValArray, j, RecSize), RecSize);

			j = j-1;
		}
		memcpy(getKeyFromKeyValArray(KeyValArray, j+1, RecSize), CurrentKeyVal, RecSize);
        free(CurrentKeyVal);
    }
}

// A recursive method to heapify a subtree with the root at given index
// This method assumes that the subtrees are already heapified
void byteHeapify(byte *KeyValArray, int Start, int End, unsigned int RecSize, int i) {
    // Initialize largest as root
    int LargestElementIndex = i;
    int LeftChildIndex = 2 * (i-Start) + 1 + Start;
    int RightChildIndex = 2 * (i-Start) + 2 + Start;
    int Size = End - Start + 1;

    unsigned int KeySize = RecSize - sizeof(value_t);

    byte* LargestElementKey = getKeyFromKeyValArray(KeyValArray, LargestElementIndex, RecSize);
    byte* LeftChildKey = getKeyFromKeyValArray(KeyValArray, LeftChildIndex, RecSize);
    if (LeftChildIndex < Size && byteKeyGreaterThan(LeftChildKey, LargestElementKey, KeySize)) LargestElementIndex = LeftChildIndex;
  
    LargestElementKey = getKeyFromKeyValArray(KeyValArray, LargestElementIndex, RecSize);
    byte* RightChildKey = getKeyFromKeyValArray(KeyValArray, RightChildIndex, RecSize);
    if (RightChildIndex < Size && byteKeyGreaterThan(RightChildKey, LargestElementKey, KeySize)) LargestElementIndex = RightChildIndex;

    if (LargestElementIndex != i) {
        swapKeys(KeyValArray, LargestElementIndex, i, RecSize);
        byteHeapify(KeyValArray, Start, End, RecSize, LargestElementIndex);
    }
}

// main function for heap sort
void byteHeapSort(byte *KeyValArray, int Start, int End, unsigned int RecSize) {
  
    int Size = End - Start + 1;
    // build max heap
    for (int i = Start + Size / 2 - 1; i >= Start; i--) {
        byteHeapify(KeyValArray, Start, End, RecSize, i);
    }

    // printf("Max Heap Built\n");
    // printKeyValuePairs(Size, 8, KeySize, KeysArray, ValuesArray);

  
    // heap sort
    for (int i = End; i >= Start; i--) {
        // swap the largest key with the end of the heap
        swapKeys(KeyValArray, Start, i, RecSize);
        // printf("After %d th Swap\n", i);
        // printKeyValuePairs(Size, 8, KeySize, KeysArray, ValuesArray);

        // heapify new root node while removing the largest element now at the end from heap
        byteHeapify(KeyValArray, Start, i-1, RecSize, Start);
        // printf("After %d th Heapify\n", i);
        // printKeyValuePairs(Size, 8, KeySize, KeysArray, ValuesArray);
    }
}

/* This function takes median of three as pivot, places
the pivot element at its correct position in sorted
array, and places all smaller (smaller than pivot)
to left of pivot and all greater elements to right
of pivot
 */
int partition(byte *KeyValArray, int Start, int End, unsigned int RecSize) {
    
    // get the pivot index
    int PivotIndex = medianOfThree(KeyValArray, Start, Start + ((End-Start)/2), End, RecSize);
    // put the pivot to the end of the array
    swapKeys(KeyValArray, PivotIndex, End, RecSize);

    // get the pivot key (now at end)
    byte *pivot = getKeyFromKeyValArray(KeyValArray, End, RecSize);
	int i =  Start - 1; // Index of smaller element and indicates the right position of pivot found so far

    unsigned int KeySize = RecSize - sizeof(value_t);
	for (int j = Start; j <= End - 1; j++) {
		// If current element is smaller than the pivot
        if (byteKeyLessThan(getKeyFromKeyValArray(KeyValArray, j, RecSize), pivot, KeySize)) {
            i++;
            swapKeys(KeyValArray, i, j, RecSize);
        }
	}
    swapKeys(KeyValArray, i+1, End, RecSize);
	return (i + 1);
}

// utility function to perform intro sort
void byteIntrosortUtil(byte *KeyValArray, int Start, int End, unsigned int RecSize, int DepthLimit) {
	// Count the number of elements
	int Size = End - Start;


	// if partition size is low then do insertion sort
	if (Size < 16) {
		byteInsertionSort(KeyValArray, Start, End, RecSize);
        return;
	}

	// if the depth is zero use heapsort
	if (DepthLimit == 0) {
		byteHeapSort(KeyValArray, Start, End, RecSize);
        return;
	}

    // perform quick sort

	// put KeysArray[PartitionIndex] at right place
    int PartitionIndex = partition(KeyValArray, Start, End, RecSize);

    // Separately sort elements before partition and after partition
    byteIntrosortUtil(KeyValArray, Start, PartitionIndex  - 1, RecSize, DepthLimit - 1);
    byteIntrosortUtil(KeyValArray, PartitionIndex  + 1, End, RecSize, DepthLimit - 1);
    return;
}

/* Implementation of introsort*/
void byteIntrosort(byte *KeyValArray, int Start, int End, unsigned int RecSize) {
	int DepthLimit = 2 * log(End-Start);

	// Perform a recursive Introsort
	byteIntrosortUtil(KeyValArray, Start, End, RecSize, DepthLimit);
    return;
}

#endif