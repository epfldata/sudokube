#include <vector>
#include <algorithm>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <mutex>
#include "Keys.h"
#include "Payload.h"
#include <cstring>
//#define VERBOSE


// SparseRecord
struct initrec {
    std::string key;
    unsigned int val;

};

/*
struct rec {
  key_type key;
  payload val;

  void copy_from(rec &other) { memcpy(this, &other, sizeof(rec)); }
};
*/

inline byte* getKey(byte** array, int idx, int recSize) {
    byte* key = (byte *)array + recSize * idx;
//    printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}
inline int* getVal(byte** array, int idx, int recSize) {
    int* val = (int *) ((byte *)array + (idx+1)* recSize - sizeof(int));
//    printf("array = %d idx = %d recSize = %d val = %d\n", array, idx, recSize, val);
    return val;
}

inline int bitsToBytes(int bits) {
    return bits/8 + 1;
}

std::vector<void *>  registry(0);

/* In the case of a sparse cuboid, size is the number of rows.
   Conversion to bytes: size * sizeof(rec)
   In the case of a dense  cuboid, size is the number of dimension bits.
   Conversion to bytes: 2^size * sizeof(payload)
*/
std::vector<long> sz_registry(0);
std::vector<short> keysz_registry(0);

std::mutex registryMutex;
// append cuboid to registry, used both in dense and sparse case.
int r_add(void *p, long size, short keysize)
{
  std::unique_lock<std::mutex> lock(registryMutex);

  registry.push_back(p);
  sz_registry.push_back(size);
  keysz_registry.push_back(keysize);
  return registry.size() - 1;
}


/* creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
*/
int mk(int n_bits)
{
  return r_add(new std::vector<initrec>(), 0, bitsToBytes(n_bits));
}

/* appends one record to an appendable (not yet frozen) sparse representation.

   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
*/
void add(int s_id, int n_bits, byte *key, int v)
{
  initrec myrec;
  int numbytes = bitsToBytes(n_bits);
  myrec.key.reserve(numbytes);
  memcpy(&myrec.key[0], key, numbytes);
  myrec.val = v;

  std::vector<initrec> *p = (std::vector<initrec> *)registry[s_id];
  p->push_back(myrec); // makes a copy of myrec
  sz_registry[s_id] ++;
}

void freeze(int s_id)
{
  std::vector<initrec> *store = (std::vector<initrec> *)registry[s_id];
  short keySize = keysz_registry[s_id];
  unsigned long long rows = store->size();
  size_t recSize = keySize + sizeof(int);
  byte **newstore = (byte **)calloc(rows, recSize);
  size_t sizeMB = rows * recSize/(1000 * 1000);
  if(sizeMB > 100) fprintf(stderr, "\nfreeze calloc : %lu MB\n", sizeMB);

  for(int r = 0; r < rows; r++) {
      memcpy(getKey(newstore, r, recSize), (*store)[r].key.data(), keySize);
      memcpy(getVal(newstore, r, recSize), &((*store)[r].val), sizeof(int));
  }

#ifdef VERBOSE
    printf("\nFREEZE keySize = %d  recSize = %d\n", keySize, recSize);
    for(unsigned i = 0; i < rows; i++) {
        print_key(10, getKey(newstore, i,  recSize)); //hard coded 20 bits
        printf(" ");
        printf(" %u ", *getVal(newstore, i,  recSize));
        printf("\n");
    }
#endif
  delete store;
  registry[s_id] = newstore;
}


int sz(int id) { return sz_registry[id]; }
inline int keysz(int id) { return keysz_registry[id];}
inline int recsz(int id) { return keysz_registry[id] + sizeof(int);}

void sparse_print(int s_id, int n_bits) {
  byte **store = (byte **)registry[s_id];

  for(unsigned i = 0; i < sz(s_id); i++) {
    print_key(n_bits, getKey(store, i,  recsz(s_id)));
    printf(" ");
    printf(" %u ", *getVal(store, i,  recsz(s_id)));
    printf("\n");
  }
}

void dense_print(int d_id, int n_bits) {
  int *store = (int *)registry[d_id];

  for(unsigned i = 0; i < (1 << n_bits); i++) {
    printf("%u ", i);
    printf("%u ", store[i]);
    printf("\n");
  }
}


void *read_cb(const char *filename, unsigned long long byte_size) {
  unsigned char *b = (unsigned char *)malloc(byte_size);
  FILE *fp = fopen(filename, "r");
  assert(fp != NULL);
  fread(b, 1, byte_size, fp);
  fclose(fp);
  return b;
}

int readSCuboid(const char *filename, int n_bits, int size) {
  printf("readSCuboid(\"%s\", %d, %d)\n", filename, n_bits, size);
  int keySize = bitsToBytes(n_bits);
  int recSize = keySize + sizeof(int);
  size_t byte_size = size * recSize;
  int s_id = r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    sparse_print(s_id, n_bits);
#endif
  return s_id ;
}

int readDCuboid(const char *filename, int n_bits, int size) {
  printf("readDCuboid(\"%s\", %d, %d)\n", filename, n_bits, size);

  // FIXME! size == 2^n_bits
  // it seems sz_registry is incompatible with Scala Cuboid.size
 int keySize = bitsToBytes(n_bits);
  size_t byte_size = (1LL << n_bits) * sizeof(int);
  int d_id = r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    dense_print(d_id, n_bits);
#endif
  return d_id;
}

void write_cb(const char *filename, int id, size_t byte_size) {
  printf("write_cb(\"%s\", %d): %lu bytes\n", filename, id, byte_size);

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
  size_t byte_size = sz_registry[s_id] * recsz(s_id);
  write_cb(filename, s_id, byte_size);
}

void writeDCuboid(const char *filename, int d_id) {
  size_t byte_size = (1LL << sz_registry[d_id]) * sizeof(int);
  write_cb(filename, d_id, byte_size);
}




// must only be called for a dense array
int *fetch(int d_id) {
  return (int *)registry[d_id];
}


// rehashing ops

int srehash(int s_id, int *mask, int masklen)
{
  #ifdef VERBOSE
  printf("Begin srehash(%d, ", s_id);
  for(int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
  printf(", %d)\n", masklen);
  #endif

  long rows     = sz_registry[s_id];
  byte **store    = (byte **)registry[s_id];


  const int tempKeySize = 40;

  struct tempRec {
      byte key[tempKeySize];
      int val;
  };

  const int tempRecSize = sizeof(tempRec);

  tempRec* tempstore = (tempRec* )calloc(rows, tempRecSize);
  int recSize = recsz(s_id);
  int keySize = keysz(s_id);
  for(unsigned long r = 0; r < rows; r++) {
    project_key(masklen, getKey(store, r, recSize), mask, getKey((byte **)tempstore, r, tempRecSize));
    memcpy(getVal((byte **)tempstore, r, tempRecSize), getVal(store, r, recSize), sizeof(int));
    #ifdef VERBOSE
    print_key(masklen, getKey(store, r, recSize));
    printf(" -> (");
    print_key(masklen, getKey((byte **)tempstore, r, tempRecSize));
    printf(", %d)\n", *getVal((byte **)tempstore, r, tempRecSize));
    #endif
  }


  std::sort(tempstore, tempstore + rows, [keySize](const tempRec& k1, const tempRec& k2){ return compare_keys((const byte *) &(k1.key), (const byte*)&(k2.key), keySize);});
#ifdef VERBOSE
  printf("AFter sorting\n");
    for(unsigned long r = 0; r < rows; r++) {
        print_key(masklen, getKey((byte **)tempstore, r, tempRecSize));
        printf(", %d)\n", *getVal((byte **)tempstore, r, tempRecSize));
    }
#endif
  unsigned long watermark = 0;
  for(unsigned long r = 0; r < rows; r ++) if(watermark < r) {
      byte* key1 = getKey((byte **)tempstore, watermark, tempRecSize);
      byte* key2 = getKey((byte **) tempstore, r, tempRecSize);
      bool cmp = compare_keys(key1,key2, keySize);
#ifdef VERBOSE
      printf(" Comparing ");
      print_key(masklen, key1);
      printf(" and ");
      print_key(masklen, key2);
      printf("  ==> %d \n", cmp);
#endif
    if(!cmp) { // same keys
    //printf("Merge %lu into %lu\n", r, watermark);
        *getVal((byte **)tempstore, watermark, tempRecSize) += *getVal((byte **)tempstore , r, tempRecSize);
    }
    else {
      watermark ++;
      if(watermark < r) {
          //printf("Copy %lu into %lu\n", r, watermark);
        memcpy(getKey((byte **)tempstore, watermark, tempRecSize), getKey((byte **)tempstore, r, tempRecSize), tempRecSize);

      }
    }
  }

 int fromDim = masklen;
  int toDim = 0;
  for(int i = 0; i < masklen; i++)
      if(mask[i]) toDim ++;

  int newKeySize = bitsToBytes(toDim);
  int newRecSize = newKeySize + sizeof(int);
  int new_rows = watermark + 1;
  //printf("End srehash (compressing from %lu to %d)\n", rows, new_rows);

  byte** new_store = (byte **)calloc(new_rows, newRecSize);
  size_t numMB = new_rows * newRecSize/(1000 * 1000);
  if(numMB > 100) fprintf(stderr, "\nsrehash calloc : %lu MB\n",numMB);
  for(int i = 0; i < new_rows; i++) {
      memcpy(getKey(new_store, i, newRecSize), getKey((byte **)tempstore, i, tempRecSize), newKeySize);
      memcpy(getVal(new_store, i, newRecSize), getVal((byte **)tempstore, i, tempRecSize), sizeof(int));
  }
  free(tempstore);
  unsigned id = r_add(new_store, new_rows, newKeySize);

  #ifdef VERBOSE
    sparse_print(id, toDim);
//  char filename[80];
//  sprintf(filename, "sudokubelet_%08d", id);
//  FILE *fp = fopen(filename, "w");
//  (void)fwrite(newstore, sizeof(rec), new_rows, fp);
//  fclose(fp);
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
  int *store             = (int *)registry[d_id];
  int *newstore          = (int *)calloc(newsize, sizeof(int));
  assert(newstore);
  size_t numMB =  newsize * sizeof(int)/(1000 * 1000);
  if(numMB > 100) fprintf(stderr, "\ndrehash calloc : %lu MB\n",numMB);

  // all intervals are initially invalid -- no constraint
  memset(newstore,0, sizeof(int) * newsize);


   int newKeySize = bitsToBytes(d_bits);
   int oldKeySize = bitsToBytes(n_bits);
  for(unsigned long long r = 0; r < size; r++) {
    byte src_key[oldKeySize], dest_key[oldKeySize];
    fromLong(src_key, r, oldKeySize);
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

    newstore[i] += store[r];
/*
    newstore[i].print();
    printf("\n");
*/
  }

  int d_id2 = r_add(newstore, d_bits, newKeySize);

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
  byte **store                 = (byte **)registry[s_id];
  int *newstore          = (int *)calloc(newsize, sizeof(int));
  assert(newstore);
  size_t numMB =   newsize * sizeof(int)/(1000 * 1000);
  if(numMB > 100) fprintf(stderr, "\ns2drehash calloc : %lu MB\n", numMB);
  // all intervals are initially invalid -- no constraint
  memset(newstore, 0, sizeof(int)* newsize);
  int keySize = keysz(s_id);
  int recSize = recsz(s_id);

  for(int r = 0; r < rows; r++) {
    byte dest_key[keySize];
//    print_key(masklen, getKey(store, r, recSize));
    project_key(masklen, getKey(store, r, recSize), mask, dest_key);
    unsigned long long i = toLong(d_bits, dest_key);
    newstore[i] += *getVal(store,r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
  }

  int d_id = r_add(newstore, d_bits, bitsToBytes(d_bits));

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
  int *store          = (int *)registry[d_id];
  int d_bits = 0;
  for(int i = 0; i < masklen; i++)
      if(mask[i]) d_bits++;


  int newKeySize = bitsToBytes(d_bits);
  int oldKeySize = bitsToBytes(n_bits);
  int newRecSize = newKeySize + sizeof(int);

  byte **newstore           = (byte **)calloc(newrows, newRecSize);
  size_t numMB = newrows * sizeof(newRecSize)/(1000 * 1000);
  if(numMB > 100) fprintf(stderr, "\nd2srehash calloc : %lu MB\n", numMB);
  fprintf(stderr, "WARNING: d2srehash -- aggregation not implemented.\n");

  for(unsigned long long r = 0; r < size; r++) {
    byte src_key[oldKeySize];
    fromLong(src_key, r, oldKeySize);
    project_key(masklen, src_key, mask, getKey(newstore, r, newKeySize));
    *getVal(newstore, r, newRecSize) += store[r];
  }

  return r_add(newstore, newrows, newKeySize);
}


