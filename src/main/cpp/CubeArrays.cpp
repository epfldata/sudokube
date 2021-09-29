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



const int tempKeySize = 40;

struct tempRec {
    byte key[tempKeySize];
    int val;
};

/*
struct rec {
  key_type key;
  payload val;

  void copy_from(rec &other) { memcpy(this, &other, sizeof(rec)); }
};
*/

inline byte *getKey(byte **array, int idx, int recSize) {
    byte *key = (byte *) array + recSize * idx;
//    printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

inline int *getVal(byte **array, int idx, int recSize) {
    int *val = (int *) ((byte *) array + (idx + 1) * recSize - sizeof(int));
//    printf("array = %d idx = %d recSize = %d val = %d\n", array, idx, recSize, val);
    return val;
}

inline int bitsToBytes(int bits) {
    return bits / 8 + 1;
}


struct {
    std::vector<void *> ptr_registry;
    //Currently we support only number of rows < IntMax
    //Stores numrows for both dense and sparse cuboids
    std::vector<int> numrows_registry;
    std::vector<short> keysz_registry;
    std::mutex registryMutex;

    int r_add(void *p, int size, short keysize) {
        std::unique_lock<std::mutex> lock(registryMutex);
//TODO: It is possible that some thread might access when these vectors are resized.
        ptr_registry.push_back(p);
        numrows_registry.push_back(size);
        keysz_registry.push_back(keysize);
        return ptr_registry.size() - 1;
    }

    void multi_r_add(byte *p_array[], int size_array[], int n_bits_array[], int id_array[], int numCuboids) {
        std::unique_lock<std::mutex> lock(registryMutex);
        for (int i = 0; i < numCuboids; i++) {
            ptr_registry.push_back(p_array[i]);
            numrows_registry.push_back(size_array[i]);
            keysz_registry.push_back(bitsToBytes(n_bits_array[i]));
            id_array[i] = ptr_registry.size() - 1;
        }
    }

    void* readPtr(int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return ptr_registry[id];
    }
    unsigned int readSize(int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return numrows_registry[id];
    }

    void read(int id, void *&ptr, unsigned int &size, short &keySize) {
        std::unique_lock<std::mutex> lock(registryMutex);
        ptr = ptr_registry[id];
        size = numrows_registry[id];
        keySize = keysz_registry[id];
    }

} globalRegistry;

// append cuboid to registry, used both in dense and sparse case.

/* creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
*/
int mk(int n_bits) {
    return globalRegistry.r_add(new std::vector<tempRec>(), 0, bitsToBytes(n_bits));
}

/* appends one record to an appendable (not yet frozen) sparse representation.

   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
*/
void add(int s_id, int n_bits, byte *key, int v) {
    tempRec myrec;
    int numbytes = bitsToBytes(n_bits);
    memcpy(&myrec.key[0], key, numbytes);
    myrec.val = v;

    //SBJ: Currently no other thread should be running. So no locks
    std::vector<tempRec> *p = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    p->push_back(myrec); // makes a copy of myrec
    globalRegistry.numrows_registry[s_id]++;
}

void freeze(int s_id) {
    //SBJ: No other threads. No locks
    std::vector<tempRec> *store = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    short keySize = globalRegistry.keysz_registry[s_id];
    unsigned int rows = store->size();
    size_t recSize = keySize + sizeof(int);
    byte **newstore = (byte **) calloc(rows, recSize);
    size_t sizeMB = rows * recSize / (1000 * 1000);
    if (sizeMB > 100) fprintf(stderr, "\nfreeze calloc : %lu MB\n", sizeMB);

    for (int r = 0; r < rows; r++) {
        memcpy(getKey(newstore, r, recSize), &(*store)[r].key[0], keySize);
        memcpy(getVal(newstore, r, recSize), &((*store)[r].val), sizeof(int));
    }

#ifdef VERBOSE
    printf("\nFREEZE keySize = %d  recSize = %d\n", keySize, recSize);
    for (unsigned i = 0; i < rows; i++) {
        print_key(10, getKey(newstore, i, recSize)); //hard coded 10 bits
        printf(" ");
        printf(" %u ", *getVal(newstore, i, recSize));
        printf("\n");
    }
#endif
    delete store;
    globalRegistry.ptr_registry[s_id] = newstore;
}


int sz(int id) { return globalRegistry.readSize(id); }

void sparse_print(int s_id, int n_bits) {

    void *ptr;
    unsigned int size;
    short keySize;
    globalRegistry.read(s_id, ptr, size, keySize);
    byte **store = (byte **) ptr;
    int recSize = keySize + sizeof(int);

    for (unsigned i = 0; i < size; i++) {
        print_key(n_bits, getKey(store, i, recSize));
        printf(" ");
        printf(" %u ", *getVal(store, i, recSize));
        printf("\n");
    }
}

void dense_print(int d_id, int n_bits) {
    void *ptr;
    unsigned int size;
    short keySize;
    globalRegistry.read(d_id, ptr, size, keySize);
    int *store = (int *)ptr;

    for (unsigned i = 0; i < size; i++) {
        printf("%u ", i);
        printf("%u ", store[i]);
        printf("\n");
    }
}

void readMultiCuboid(const char *filename, int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                     int id_array[], int numCuboids) {
    printf("readMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "r");
    assert(fp != NULL);
    byte **buffer_array = new byte*[numCuboids];
    for (int i = 0; i < numCuboids; i++) {
        bool sparse = isSparse_array[i];
        int n_bits = n_bits_array[i];
        int size = size_array[i];
        if (sparse) {
            int keySize = bitsToBytes(n_bits);
            int recSize = keySize + sizeof(int);
            size_t byte_size = size * recSize;
            byte *buffer = (byte *) malloc(byte_size);
            fread(buffer, 1, byte_size, fp);
            buffer_array[i] = buffer;
#ifdef VERBOSE
            printf("Read Sparse Cuboid i=%d,  nbits=%d, size=%d, keySize=%d, bufferSize=%lu\n", i, n_bits, size,
                   keySize, byte_size);
            for (unsigned i = 0; i < size; i++) {
                print_key(n_bits, getKey((byte **) buffer, i, recSize));
                printf(" ");
                printf(" %u ", *getVal((byte **) buffer, i, recSize));
                printf("\n");
            }
#endif
        } else {
            size_t byte_size = (1LL << n_bits) * sizeof(int);
            byte *buffer = (byte *) malloc(byte_size);
            fread(buffer, 1, byte_size, fp);
            buffer_array[i] = buffer;
#ifdef VERBOSE
            printf("Read Dense Cuboid i=%d, nbits=%d, size=%d byte_size=%lu\n", i, n_bits, size, byte_size);
            for (unsigned i = 0; i < (1 << n_bits); i++) {
                printf("%u ", i);
                printf("%u ", buffer[i]);
                printf("\n");
            }
#endif
        }
    }
    globalRegistry.multi_r_add(buffer_array, size_array, n_bits_array, id_array, numCuboids);
    delete[] buffer_array;
}

void *read_cb(const char *filename, unsigned long long byte_size) {
    unsigned char *b = (unsigned char *) malloc(byte_size);
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
    int s_id = globalRegistry.r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    sparse_print(s_id, n_bits);
#endif
    return s_id;
}

int readDCuboid(const char *filename, int n_bits, int size) {
    printf("readDCuboid(\"%s\", %d, %d)\n", filename, n_bits, size);

    assert(size == 1 << n_bits);
    int keySize = bitsToBytes(n_bits);
    size_t byte_size = size * sizeof(int);
    int d_id = globalRegistry.r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    dense_print(d_id, n_bits);
#endif
    return d_id;
}

void write_cb(const char *filename, void *data, int id, size_t byte_size) {
    printf("write_cb(\"%s\", %d): %lu bytes\n", filename, id, byte_size);

    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);

    int sw = fwrite(data, 1, byte_size, fp);
    if (sw == 0) {
        printf("Write Error: %s\n", strerror(errno));
        exit(1);
    }
    fclose(fp);
}

void writeSCuboid(const char *filename, int s_id) {
    void* data;
    unsigned int size;
    short keySize;
    globalRegistry.read(s_id, data, size, keySize);
    int recSize = keySize + sizeof(int);
    size_t byte_size = size * recSize;
    write_cb(filename, data, s_id, byte_size);
}

void writeDCuboid(const char *filename, int d_id) {
    void* data;
    unsigned int size;
    short keySize;
    globalRegistry.read(d_id, data, size, keySize);
    size_t byte_size = size * sizeof(int);
    write_cb(filename, data, d_id, byte_size);
}


void writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[], int numCuboids) {
    printf("writeMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);
    for (int i = 0; i < numCuboids; i++) {
        size_t byte_size = 0;
        int id = ids[i];
        bool sparse = isSparse_array[i];

        void* data;
        unsigned int size;
        short keySize;
        globalRegistry.read(id, data, size, keySize);

        if (sparse) {
            int recSize = keySize + sizeof(int);
            byte_size = size * recSize;
#ifdef VERBOSE
            printf("Write Sparse Cuboid i=%d byte_size=%lu \n", i, byte_size);
#endif
        } else {
            byte_size = size * sizeof(int);
#ifdef VERBOSE
            printf("Write Dense Cuboid i=%d byte_size=%lu \n", i, byte_size);
#endif
        }
        int sw = fwrite(data, 1, byte_size, fp);
        if (sw == 0) {
            printf("Write Error: %s\n", strerror(errno));
            exit(1);
        }
    }
    fclose(fp);
}


// must only be called for a dense array
int *fetch(int d_id, unsigned int &size) {
    short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);
    return (int *) ptr;
}


// rehashing ops

int srehash(int s_id, int *mask, int masklen) {
#ifdef VERBOSE
    printf("Begin srehash(%d, ", s_id);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
#endif
    short keySize;
    unsigned int rows;
    void* ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    byte **store = (byte **) ptr;
    int recSize = keySize + sizeof(int);


    const int tempRecSize = sizeof(tempRec);

    tempRec *tempstore = (tempRec *) calloc(rows, tempRecSize);

    for (unsigned long r = 0; r < rows; r++) {
        project_key(masklen, getKey(store, r, recSize), mask, getKey((byte **) tempstore, r, tempRecSize));
        memcpy(getVal((byte **) tempstore, r, tempRecSize), getVal(store, r, recSize), sizeof(int));
#ifdef VERBOSE
        print_key(masklen, getKey(store, r, recSize));
        printf(" -> (");
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %d)\n", *getVal((byte **) tempstore, r, tempRecSize));
#endif
    }


    std::sort(tempstore, tempstore + rows, [keySize](const tempRec &k1, const tempRec &k2) {
        return compare_keys((const byte *) &(k1.key), (const byte *) &(k2.key), keySize);
    });
#ifdef VERBOSE
    printf("AFter sorting\n");
    for (unsigned long r = 0; r < rows; r++) {
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %d)\n", *getVal((byte **) tempstore, r, tempRecSize));
    }
#endif
    unsigned long watermark = 0;
    for (unsigned long r = 0; r < rows; r++)
        if (watermark < r) {
            byte *key1 = getKey((byte **) tempstore, watermark, tempRecSize);
            byte *key2 = getKey((byte **) tempstore, r, tempRecSize);
            bool cmp = compare_keys(key1, key2, keySize);
#ifdef VERBOSE
            printf(" Comparing ");
            print_key(masklen, key1);
            printf(" and ");
            print_key(masklen, key2);
            printf("  ==> %d \n", cmp);
#endif
            if (!cmp) { // same keys
                //printf("Merge %lu into %lu\n", r, watermark);
                *getVal((byte **) tempstore, watermark, tempRecSize) += *getVal((byte **) tempstore, r, tempRecSize);
            } else {
                watermark++;
                if (watermark < r) {
                    //printf("Copy %lu into %lu\n", r, watermark);
                    memcpy(getKey((byte **) tempstore, watermark, tempRecSize),
                           getKey((byte **) tempstore, r, tempRecSize), tempRecSize);

                }
            }
        }

    int fromDim = masklen;
    int toDim = 0;
    for (int i = 0; i < masklen; i++)
        if (mask[i]) toDim++;

    int newKeySize = bitsToBytes(toDim);
    int newRecSize = newKeySize + sizeof(int);
    int new_rows = watermark + 1;
    //printf("End srehash (compressing from %lu to %d)\n", rows, new_rows);

    byte **new_store = (byte **) calloc(new_rows, newRecSize);
    size_t numMB = new_rows * newRecSize / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\nsrehash calloc : %lu MB\n", numMB);
    for (int i = 0; i < new_rows; i++) {
        memcpy(getKey(new_store, i, newRecSize), getKey((byte **) tempstore, i, tempRecSize), newKeySize);
        memcpy(getVal(new_store, i, newRecSize), getVal((byte **) tempstore, i, tempRecSize), sizeof(int));
    }
    free(tempstore);
    unsigned id = globalRegistry.r_add(new_store, new_rows, newKeySize);

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

int drehash(int n_bits, int d_id, int d_bits, int *mask, int masklen) {
#ifdef VERBOSE
    printf("Begin drehash(%d, %d, %d, ", n_bits, d_id, d_bits);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
    printf("Input:\n");
    dense_print(d_id, n_bits);
    printf("\n");
#endif

    assert(n_bits == masklen);
    unsigned int size;
    short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);
    assert(size == (1 << n_bits));
    int *store = (int *) ptr;

    unsigned int newsize = 1 << d_bits;

    int *newstore = (int *) calloc(newsize, sizeof(int));
    assert(newstore);
    size_t numMB = newsize * sizeof(int) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ndrehash calloc : %lu MB\n", numMB);

    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(int) * newsize);


    int newKeySize = bitsToBytes(d_bits);
    int oldKeySize = bitsToBytes(n_bits);
    for (unsigned long long r = 0; r < size; r++) {
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

    int d_id2 = globalRegistry.r_add(newstore, newsize, newKeySize);

#ifdef VERBOSE
    printf("Result:\n");
    dense_print(d_id2, d_bits);
    printf("\n");
    printf("End drehash\n");
#endif

    return d_id2;
}

int s2drehash(int s_id, int d_bits, int *mask, int masklen) {
#ifdef VERBOSE
    printf("Begin s2drehash(%d, %d, ", s_id, d_bits);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
    printf("Input:\n");
    sparse_print(s_id, masklen);
    printf("\n");
#endif

    unsigned int rows;
    void* ptr;
    short keySize;
    globalRegistry.read(s_id, ptr, rows, keySize);
    int recSize = keySize + sizeof(int);

    byte **store = (byte **) ptr;

    unsigned int newsize = 1LL << d_bits;
    int *newstore = (int *) calloc(newsize, sizeof(int));
    assert(newstore);

    size_t numMB = newsize * sizeof(int) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehash calloc : %lu MB\n", numMB);
    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(int) * newsize);

    for (int r = 0; r < rows; r++) {
        byte dest_key[keySize];
//    print_key(masklen, getKey(store, r, recSize));
        project_key(masklen, getKey(store, r, recSize), mask, dest_key);
        unsigned long long i = toLong(d_bits, dest_key);
        newstore[i] += *getVal(store, r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
    }

    int d_id = globalRegistry.r_add(newstore, newsize, bitsToBytes(d_bits));

#ifdef VERBOSE
    printf("Result:\n");
    dense_print(d_id, d_bits);
    printf("End s2drehash\n");
#endif

    return d_id;
}

int d2srehash(int n_bits, int d_id, int *mask, int masklen) {
    unsigned int size;
    short oldKeySize;
    void * ptr;

    globalRegistry.read(d_id, ptr, size, oldKeySize);
    int *store = (int *) ptr;
    assert(size ==  (1 << masklen));

    long newrows = size;

    int d_bits = 0;
    for (int i = 0; i < masklen; i++)
        if (mask[i]) d_bits++;


    int newKeySize = bitsToBytes(d_bits);
    int newRecSize = newKeySize + sizeof(int);

    byte **newstore = (byte **) calloc(newrows, newRecSize);
    size_t numMB = newrows * sizeof(newRecSize) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\nd2srehash calloc : %lu MB\n", numMB);
    fprintf(stderr, "WARNING: d2srehash -- aggregation not implemented.\n");

    for (unsigned long long r = 0; r < size; r++) {
        byte src_key[oldKeySize];
        fromLong(src_key, r, oldKeySize);
        project_key(masklen, src_key, mask, getKey(newstore, r, newKeySize));
        *getVal(newstore, r, newRecSize) += store[r];
    }

    return globalRegistry.r_add(newstore, newrows, newKeySize);
}


