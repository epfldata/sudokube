#include<stdio.h>
#include<time.h>

#include "Payload.h"
extern int r_add(void *p, long size);
extern int drehash(int n_bits, int d_id, int d_bits, int *mask, int masklen);


int main(void) {
  unsigned long d_bits = 30;
  unsigned long long size = 1LL << d_bits;

  const int J = 1;

  payload *store[J];
  int d_id[J];

  for(int j = 0; j < J; j++) {
    clock_t t1 = clock();

    store[j] = (payload *)calloc(size, sizeof(payload));

    printf("Storage size: %ld * %lld bytes = %lld MB\n", sizeof(payload),
      size, sizeof(payload) * (1LL << (d_bits - 20)));

    for(unsigned long i = 0; i < size; i++) {
      store[j][i].init_empty();
      store[j][i].sm = 5;
    }

    d_id[j] = r_add(store[j], d_bits);

    clock_t t2 = clock();

    printf("Cube creation completed. (%g seconds)\n",
      (double)(t2 - t1) / CLOCKS_PER_SEC);
  }


  for(int j = 0; j < J; j ++) {
    clock_t t3 = clock();

    int d_bits2 = 25;
    int mask[d_bits];
    for(int i = 0; i < d_bits; i++)
      if(i < d_bits2) mask[i] = 1;
      else mask[i] = 0;

    int d_id2 = drehash(d_bits, d_id[j], d_bits2, mask, d_bits);
    clock_t t4 = clock();

    printf("Time spent rehashing: %g seconds\n",
      (double)(t4 - t3) / CLOCKS_PER_SEC);
  }

  return 0;
}


