#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

typedef unsigned char t;

const unsigned long long size = 8ULL*1024ULL*1024ULL;
const unsigned long long steps = 100;
t b[size * steps];


int main()
{
  FILE* fp;
  fp = fopen("foofile", "wb");
  for (unsigned long long j = 0; j < steps; ++j) {
    int sw = fwrite(b + size * j, 1, size * sizeof(t), fp);
    if(sw == 0) {
      printf("Read Error: %s\n", strerror(errno));
      exit(1);
    }
  }
  fclose(fp);

  printf("Write finished.\n");

  fp = fopen("foofile", "r");
  assert(fp != NULL);

  for (unsigned long long j = 0; j < steps; ++j) {
    fread(b + size * j, 1, size * sizeof(t), fp);
  }
  fclose(fp);

  return 0;
}

