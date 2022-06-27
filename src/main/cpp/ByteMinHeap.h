#ifndef MINHEAP_H
#define MINHEAP_H

#include "Utility.h"
#include <string.h>
#include <limits.h>

typedef unsigned int pagenumbertype;

 // utility function to swap two keys
void swapKeysMinHeap(byte *Array, int i, int j, unsigned int KeySize);

// returns the key at index i
byte *getKeyFromKeyPageArray(byte *Array, size_t i, size_t KeySize);

// returns the page at index i
pagenumbertype* getPageFromKeyPageArray(byte *Array, size_t i, size_t KeySize);

// compare keys
/* 
>0	i>j
0 	i=j
<0	i<j
*/
int compareKeys(byte* Array, size_t i, size_t j, size_t KeySize);

// min heap class
class MinHeap {
	byte *Array; // pointer to array of elements in heap [Key + Page]
	unsigned int KeySize; // size of the key 
	unsigned int Capacity; // maximum possible size of min heap
	unsigned int Size; // Current number of elements in min heap
public:
	// Constructor
	MinHeap(unsigned int Capacity, unsigned int KeySize);

	// Inserts a new key
	void push(byte* Key, pagenumbertype Page);

	// to heapify a subtree with the root at given index
	void heapify(int i);

	// to get index of the parent
	int parent(int i) { return (i-1)/2; }

	// to get index of left child of node at index i
	int left(int i) { return (2*i + 1); }

	// to get index of right child of node at index i
	int right(int i) { return (2*i + 2); }

	// to extract the root which is the minimum element, return its page number
	pagenumbertype pop();

	// Returns the minimum key (key at root) from min heap
	byte* getMin() { return getKeyFromKeyPageArray(Array,0,KeySize); }

    // returns the current size of the minheap
    unsigned int getSize() { return Size; }

	// Deletes a key stored at index i
	void deleteKey(int i);

	
};

// Constructor: Builds a heap from a given array a[] of given size
MinHeap::MinHeap(unsigned int Capacity, unsigned int KeySize) {
	this->Size = 0;
	this->Capacity = Capacity;
	this->KeySize= KeySize;
	this->Array = (byte *)calloc(Capacity, KeySize + sizeof(pagenumbertype));
}

// Inserts a new key 
void MinHeap::push(byte* Key, pagenumbertype Page) {
	if (Size == Capacity) {
		printf("\nOverflow: Could not push\n");
		return;
	}

	// First insert the new key at the end
	Size++;
	int i = Size - 1;
	memcpy(getKeyFromKeyPageArray(Array, i, KeySize), Key, KeySize);
	memcpy(getPageFromKeyPageArray(Array, i, KeySize), &Page, sizeof(pagenumbertype));

	// Fix the min heap property if it is violated
	while (i != 0 && compareKeys(Array, parent(i), i, KeySize)>0) {
		swapKeysMinHeap(Array, parent(i), i, KeySize);
		i = parent(i);
	}
}

// remove minimum element (or root) from min heap and return the page number
pagenumbertype MinHeap::pop() {
	if (Size <= 0) {
		return UINT_MAX;
	}
		
	if (Size == 1) {
		Size--;
		return *getPageFromKeyPageArray(Array, 0, KeySize);
	}

	// Store the minimum value, and remove it from heap
	pagenumbertype Root = *getPageFromKeyPageArray(Array, 0, KeySize);
	
	memcpy(getKeyFromKeyPageArray(Array, 0, KeySize), getKeyFromKeyPageArray(Array, Size-1, KeySize), KeySize + sizeof(pagenumbertype));
	Size--;
	heapify(0);

	return Root;
}

// A recursive method to heapify a subtree with the root at given index
// This method assumes that the subtrees are already heapified
void MinHeap::heapify(int i) {
	int l = left(i);
	int r = right(i);
	int smallest = i;
	if (l < Size && compareKeys(Array, l, smallest, KeySize) < 0) smallest = l;
	if (r < Size && compareKeys(Array, r, smallest, KeySize) < 0) smallest = r;
	if (smallest != i) {
		swapKeysMinHeap(Array, i, smallest, KeySize);
		heapify(smallest);
	}
}

// utility function to swap two keys
void swapKeysMinHeap(byte *Array, int i, int j, unsigned int KeySize) {

	byte *TempKeyPage = (byte *)calloc(1, KeySize + sizeof(pagenumbertype));
    
    memcpy(TempKeyPage, getKeyFromKeyPageArray(Array, i, KeySize), KeySize + sizeof(pagenumbertype));
    
    memcpy(getKeyFromKeyPageArray(Array, i, KeySize), getKeyFromKeyPageArray(Array, j, KeySize), KeySize + sizeof(pagenumbertype));
    
    memcpy(getKeyFromKeyPageArray(Array, j, KeySize), TempKeyPage,  KeySize + sizeof(pagenumbertype));
	
    free(TempKeyPage);
}

// returns the key at index i
byte *getKeyFromKeyPageArray(byte *Array, size_t i, size_t KeySize) {
    byte *Key = Array + (KeySize + sizeof(pagenumbertype)) * i;
    return Key;
}

// returns the page at index i
pagenumbertype* getPageFromKeyPageArray(byte *Array, size_t i, size_t KeySize) {
    pagenumbertype* Page = (pagenumbertype*)(Array + (KeySize + sizeof(pagenumbertype)) * i + KeySize);
    return Page;
}

// compare keys
int compareKeys(byte* Array, size_t i, size_t j, size_t KeySize) {
		/*
		int memcmp ( const void * ptr1, const void * ptr2, size_t num );
			n<0	the first byte that does not match in both memory blocks has a lower value in ptr1 than in ptr2 (if evaluated as unsigned char values)
			n=0	the contents of both memory blocks are equal
			n>0	the first byte that does not match in both memory blocks has a greater value in ptr1 than in ptr2 (if evaluated as unsigned char values)
		*/
		int n = memcmp(getKeyFromKeyPageArray(Array, i, KeySize), getKeyFromKeyPageArray(Array, j, KeySize), KeySize);
		return n;
} 


#endif