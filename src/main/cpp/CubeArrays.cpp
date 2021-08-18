#include <vector>
#include <algorithm>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include "Keys.h"
#include "Payload.h"

//#define VERBOSE


// SparseRecord
struct rec {
  key_type key;
  payload val;

  void copy_from(rec &other) { memcpy(this, &other, sizeof(rec)); }
};



std::vector<void *>  registry(0);

/* In the case of a sparse cuboid, size is the number of rows.
   Conversion to bytes: size * sizeof(rec)
   In the case of a dense  cuboid, size is the number of dimension bits.
   Conversion to bytes: 2^size * sizeof(payload)
*/
std::vector<long> sz_registry(0);

// append cuboid to registry, used both in dense and sparse case.
int r_add(void *p, long size)
{
  registry.push_back(p);
  sz_registry.push_back(size);
  return registry.size() - 1;
}


/* creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
*/
int mk(int n_bits)
{
  assert(n_bits <= KEY_BYTES * 8);
  return r_add(new std::vector<rec>(), 0);
}

/* appends one record to an appendable (not yet frozen) sparse representation.

   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
*/
void add(int s_id, int n_bits, key_type &key, int v)
{
  rec myrec;
  memcpy(myrec.key, key, sizeof(key_type));
  myrec.val.init_atomic(v);

  std::vector<rec> *p = (std::vector<rec> *)registry[s_id];
  p->push_back(myrec); // makes a copy of myrec
  sz_registry[s_id] ++;
}

void freeze(int s_id)
{
  std::vector<rec> *store = (std::vector<rec> *)registry[s_id];
  unsigned long long rows = store->size();
  rec *newstore = (rec *)calloc(rows, sizeof(rec));

  for(int r = 0; r < rows; r++)
    memcpy(&(newstore[r]), &((*store)[r]), sizeof(rec));

  delete store;
  registry[s_id] = newstore;
}


int sz(int id) { return sz_registry[id]; }

void sparse_print(int s_id, int n_bits) {
  rec *store = (rec *)registry[s_id];

  for(unsigned i = 0; i < sz(s_id); i++) {
    print_key(n_bits, store[i].key);
    printf(" ");
    store[i].val.print();
    printf("\n");
  }
}

void dense_print(int d_id, int n_bits) {
  payload *store = (payload *)registry[d_id];

  for(unsigned i = 0; i < (1 << n_bits); i++) {
    printf("%u ", i);
    store[i].print();
    printf("\n");
  }
}


void *read_cb(const char *filename, int byte_size) {
  unsigned char *b = (unsigned char *)malloc(byte_size);
  FILE *fp = fopen(filename, "r");
  assert(fp != NULL);
  fread(b, 1, byte_size, fp);
  fclose(fp);
  return b;
}

int readSCuboid(const char *filename, int n_bits, int size) {
  printf("readSCuboid(\"%s\", %d, %d)\n", filename, n_bits, size);

  unsigned long long byte_size = size * sizeof(rec);
  return r_add(read_cb(filename, byte_size), size);
}

int readDCuboid(const char *filename, int n_bits, int size) {
  printf("readDCuboid(\"%s\", %d, %d)\n", filename, n_bits, size);

  // FIXME! size == 2^n_bits
  // it seems sz_registry is incompatible with Scala Cuboid.size

  unsigned long long byte_size = (1LL << n_bits) * sizeof(payload);

  return r_add(read_cb(filename, byte_size), size);
}

void write_cb(const char *filename, int id, unsigned long long byte_size) {
  printf("write_cb(\"%s\", %d): %lld bytes\n", filename, id, byte_size);

  FILE* fp = fopen(filename, "wb");
  assert(fp != NULL);

  int sw = fwrite(registry[id], 1, byte_size, fp);
  if(sw == 0) {
    printf("Write Error: %s\n", strerror(errno));
    exit(1);
  }
  fclose(fp);
}

void writeSCuboid(const char *filename, int s_id) {
  unsigned long long byte_size = sz_registry[s_id] * sizeof(rec);
  write_cb(filename, s_id, byte_size);
}

void writeDCuboid(const char *filename, int d_id) {
  unsigned long long byte_size = (1LL << sz_registry[d_id]) * sizeof(payload);
  write_cb(filename, d_id, byte_size);
}




// must only be called for a dense array
payload *fetch(int d_id) {
  return (payload *)registry[d_id];
} 


bool compare(rec &r1, rec &r2) { return compare_keys(r1.key, r2.key); } 


// rehashing ops

int srehash(int s_id, int *mask, int masklen)
{
  #ifdef VERBOSE
  printf("Begin srehash(%d, ", s_id);
  for(int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
  printf(", %d)\n", masklen);
  #endif

  long rows     = sz_registry[s_id];
  rec *store    = (rec *)registry[s_id];
  rec *newstore = (rec *)calloc(rows, sizeof(rec));

  for(unsigned long r = 0; r < rows; r++) {
    project_key(masklen, store[r].key, mask, newstore[r].key);
    memcpy(&(newstore[r].val), &(store[r].val), sizeof(payload));

    #ifdef VERBOSE
    print_key(masklen, store[r].key);
    printf(" -> (");
    print_key(masklen, newstore[r].key);
    printf(", %d)\n", newstore[r].val.sm);
    #endif
  }

  std::sort(newstore, newstore + rows, compare);

  unsigned long watermark = 0;
  for(unsigned long r = 0; r < rows; r ++) if(watermark < r) {
    if(! compare(newstore[watermark], newstore[r])) { // same keys
      //printf("Merge %lu into %lu\n", r, watermark);
      newstore[watermark].val.merge_in(newstore[r].val);
    }
    else {
      watermark ++;
      if(watermark < r) {
        //printf("Copy %lu into %lu\n", r, watermark);
        newstore[watermark].copy_from(newstore[r]);
      }
    }
  }

  int new_rows = watermark + 1;
  //printf("End srehash (compressing from %lu to %d)\n", rows, new_rows);

  // TODO: Now reallocate and copy into a memory region that fits just
  // new_rows many rows.

  unsigned id = r_add(newstore, new_rows);

  #ifdef VERBOSE
  char filename[80];
  sprintf(filename, "sudokubelet_%08d", id);
  FILE *fp = fopen(filename, "w");
  (void)fwrite(newstore, sizeof(rec), new_rows, fp);
  fclose(fp);
  #endif

  return id;
}

int drehash(int n_bits, int d_id, int d_bits, int *mask, int masklen)
{
  #ifdef VERBOSE
  printf("Begin drehash(%d, %d, %d, ", n_bits, d_id, d_bits);
  for(int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
  printf(", %d)\n", masklen);
  printf("Input:\n");
  dense_print(d_id, n_bits);
  printf("\n");
  #endif

  assert(n_bits == masklen);
  unsigned long long size    = 1LL << masklen;
  unsigned long long newsize = 1LL << d_bits;
  payload *store             = (payload *)registry[d_id];
  payload *newstore          = (payload *)calloc(newsize, sizeof(payload));
  assert(newstore);

  // all intervals are initially invalid -- no constraint
  for(unsigned long long r = 0; r < newsize; r++)
    newstore[r].init_empty();

  for(unsigned long long r = 0; r < size; r++) {
    key_type src_key, dest_key;
    fromLong(src_key, r);
/*
    unsigned long long r_cp = toLong(n_bits, src_key);
    if(r != r_cp) printf("ARGH %llu %llu\n", r, r_cp);
    assert(r == r_cp);
*/
    // print_key(masklen, src_key);
    project_key(masklen, src_key, mask, dest_key);
    unsigned long long i = toLong(d_bits, dest_key);

/*
    printf(" r=%llu src_key[0]=%u i=%lld ns[i]/old=", r, src_key[0], i);
    newstore[i].print();
    printf(" store[r]=");
    store[r].print();
    printf(" ns[i]/new=");
*/

    newstore[i].merge_in(store[r]);
/*
    newstore[i].print();
    printf("\n");
*/
  }

  int d_id2 = r_add(newstore, d_bits);

  #ifdef VERBOSE
  printf("Result:\n");
  dense_print(d_id2, d_bits);
  printf("\n");
  printf("End drehash\n");
  #endif

  return d_id2;
}

int s2drehash(int s_id, int d_bits, int *mask, int masklen)
{
  #ifdef VERBOSE
  printf("Begin s2drehash(%d, %d, ", s_id, d_bits);
  for(int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
  printf(", %d)\n", masklen);
  printf("Input:\n");
  sparse_print(s_id, masklen);
  printf("\n");
  #endif

  long rows                  = sz_registry[s_id];
  unsigned long long newsize = 1LL << d_bits;
  rec *store                 = (rec *)registry[s_id];
  payload *newstore          = (payload *)calloc(newsize, sizeof(payload));
  assert(newstore);

  // all intervals are initially invalid -- no constraint
  for(unsigned long long r = 0; r < newsize; r++)
    newstore[r].init_empty();

  for(int r = 0; r < rows; r++) {
    key_type dest_key;
    //print_key(masklen, store[r].key);
    project_key(masklen, store[r].key, mask, dest_key);
    unsigned long long i = toLong(d_bits, dest_key);
    newstore[i].merge_in(store[r].val);
    //printf(" %lld %d\n", i, newstore[i].sm);
  }

  int d_id = r_add(newstore, d_bits);

  #ifdef VERBOSE
  printf("Result:\n");
  dense_print(d_id, d_bits);
  printf("End s2drehash\n");
  #endif

  return d_id;
}

int d2srehash(int n_bits, int d_id, int *mask, int masklen)
{
  unsigned long long size = 1LL << masklen; // 1LL << n_bits
  long newrows            = size;
  payload *store          = (payload *)registry[d_id];
  rec *newstore           = (rec *)calloc(newrows, sizeof(rec));

  printf("WARNING: d2srehash -- aggregation not implemented.\n");

  for(unsigned long long r = 0; r < size; r++) {
    key_type src_key;
    fromLong(src_key, r);
    project_key(masklen, src_key, mask, newstore[r].key);
    newstore[r].val.copy_from(store[r]);
  }

  return r_add(newstore, newrows);
}


