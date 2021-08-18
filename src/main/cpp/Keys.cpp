#include<stdio.h>
#include<assert.h>
#include "Keys.h"


void print_key(int n_bits, unsigned char *key) {
  for(int pos = n_bits - 1; pos >= 0; pos--) {
    int b = (key[pos / 8] >> (pos % 8)) % 2;
    if(b) printf("1");
    else  printf("0");
  }
}

bool compare_keys(key_type &k1, key_type &k2) {
  for(int i = KEY_BYTES - 1; i >= 0; i--) {
    if(k1[i] < k2[i]) return true;
    if(k1[i] > k2[i]) return false;
  }
  return false;
}

void fromLong(unsigned char *key, unsigned long long _lkey) {
  int pos = 0;
  unsigned long long lkey = _lkey;

  // key must be all zeros!!!
  // Even though this algo does not break in case key isn't initialized to
  // zeroes, other algorithms may -- think what happens if _lkey is small
  // and some relevant bytes of key don't get written to.
  for(int i = 0; i < KEY_BYTES; i++) key[i] = 0;

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

unsigned long long toLong(int n_bits, unsigned char *key) {
  unsigned long long r = 0;
  for(int pos = 0; pos < n_bits; pos++) {
    int b = (key[pos / 8] >> (pos % 8)) % 2;
    r |= b << pos;
  }
  return r;
}

void project_key(int n_bits, key_type &from_key, int *mask, key_type &to_key)
{
  // IMPORTANT: to_key must be all zeroes!
  for(int i = 0; i < KEY_BYTES; i++) to_key[i] = 0;

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


