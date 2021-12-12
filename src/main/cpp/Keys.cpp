#include<stdio.h>
#include<assert.h>
#include "Keys.h"
#include <cstring>

void print_key(int n_bits, byte *key) {
  for(int pos = n_bits - 1; pos >= 0; pos--) {
    int b = (key[pos / 8] >> (pos % 8)) % 2;
    if(b) printf("1");
    else  printf("0");
    if(pos % 8 == 0)
        printf(" ");
  }
}

bool compare_keys(const byte *k1, const byte *k2, int numkeybytes)  {
    //Warning : Do not use unsigned here. i>= 0 always true
  for(int i = numkeybytes - 1; i >= 0; i--) {
    if(k1[i] < k2[i]) return true;
    if(k1[i] > k2[i]) return false;
  }
  return false;
}

void fromLong(unsigned char *key, unsigned long long _lkey, int numkeybytes) {
  int pos = 0;
  unsigned long long lkey = _lkey;

  // key must be all zeros!!!
  // Even though this algo does not break in case key isn't initialized to
  // zeroes, other algorithms may -- think what happens if _lkey is small
  // and some relevant bytes of key don't get written to.
  memset(key, 0, numkeybytes);

/*
  while(lkey > 0) {
    int b = lkey % 2;
    key[pos / 8] |= b << (pos % 8);
    lkey = lkey >> 1;
    pos ++;
  }
*/
  while(lkey > 0) {
    key[pos] = lkey % 256;
    lkey = lkey / 256;
    pos++;
  }
}

unsigned long long toLong(int n_bits, byte *key) {
  unsigned long long r = 0;
  for(int pos = 0; pos < n_bits; pos++) {
    int b = (key[pos / 8] >> (pos % 8)) % 2;
    r |= b << pos;
  }
  return r;
}

unsigned long long project_key_toLong2(unsigned int masksum, byte *from_key, unsigned int *maskpos) {
    unsigned long long r = 0;
    for(int wpos = 0; wpos < masksum; wpos++) {
        int rpos = maskpos[wpos];
        int b = (from_key[rpos / 8] >> (rpos % 8)) % 2;
        r |= b << wpos;
    }
    return r;
}
unsigned long long project_key_toLong(unsigned int masklen, byte *from_key, unsigned int *mask)
{
    unsigned long long r = 0;
    int wpos = 0;
    for(int rpos = 0; rpos < masklen; rpos++) if(mask[rpos]) {
            int b = (from_key[rpos / 8] >> (rpos % 8)) % 2;
            r |= b << wpos;
            wpos ++;
        }

   return r;
}


void project_key(unsigned int n_bits, unsigned int toBytes, byte *from_key, unsigned int *mask, byte* to_key)
{
    //we assume to_keys has length at least n_bits/8 + 1 bytes

  // IMPORTANT: to_key must be all zeroes!
    memset(to_key, 0, toBytes);

  //printf("pk: ");
  //print_key(n_bits, from_key); printf(" ");

  int wpos = 0;
  for(int rpos = 0; rpos < n_bits; rpos++) if(mask[rpos]) {
    int b = (from_key[rpos / 8] >> (rpos % 8)) % 2;
    to_key[wpos / 8] |= b << (wpos % 8);
    wpos ++; 
  }

  //print_key(wpos, to_key); printf("\n");
}


