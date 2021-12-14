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
#include <iostream>
//#define VERBOSE



const unsigned int tempKeySize = 40;

struct tempRec {
    byte key[tempKeySize];
    value_t val;
};


/**
 We support only nbits <= 320 and nrows < 2^31 currently. We still use size_t for numrows because the numbytes obtained
 by multiplying by record size may cross IntMax, and produces weird results when both recSize and numRows are unsigned ints.

 */


/*
struct rec {
  key_type key;
  payload val;

  void copy_from(rec &other) { memcpy(this, &other, sizeof(rec)); }
};
*/

inline byte *getKey(byte **array, size_t idx, size_t recSize) {
    byte *key = (byte *) array + recSize * idx;
//    printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

inline value_t *getVal(byte **array, size_t idx, size_t recSize) {
    value_t *val = (value_t *) ((byte *) array + (idx + 1) * recSize - sizeof(value_t));
//    printf("array = %d idx = %d recSize = %d val = %d\n", array, idx, recSize, val);
    return val;
}

inline unsigned int bitsToBytes(unsigned int bits) {
    return bits / 8 + 1;
}

thread_local unsigned short globalnumkeybytes = 0;
inline bool global_compare_keys(const tempRec& k1, const tempRec& k2)  {
    //DO NOT use unsigned !! i >= 0 always true
    for(short i = globalnumkeybytes - 1; i >= 0; i--) {
        if(k1.key[i] < k2.key[i]) return true;
        if(k1.key[i] > k2.key[i]) return false;
    }
    return false;
}

struct {
    std::vector<void *> ptr_registry;
    //Currently we support only number of rows < IntMax
    //Stores numrows for both dense and sparse cuboids
    std::vector<size_t> numrows_registry;
    std::vector<unsigned short> keysz_registry;
    std::mutex registryMutex;

    unsigned int r_add(void *p, size_t size, unsigned short keysize) {
        std::unique_lock<std::mutex> lock(registryMutex);
//TODO: It is possible that some thread might access when these vectors are resized.
        ptr_registry.push_back(p);
        numrows_registry.push_back(size);
        keysz_registry.push_back(keysize);
        return ptr_registry.size() - 1;
    }

    void multi_r_add(byte *p_array[], int size_array[],  int n_bits_array[], unsigned int id_array[], unsigned int numCuboids) {
        std::unique_lock<std::mutex> lock(registryMutex);
        for (unsigned int i = 0; i < numCuboids; i++) {
            ptr_registry.push_back(p_array[i]);
            //Note: Conversion from Int to size_t
            numrows_registry.push_back(size_array[i]);
            //Note: Conversion from Int to unsigned int
            keysz_registry.push_back(bitsToBytes(n_bits_array[i]));
            id_array[i] = ptr_registry.size() - 1;
        }
    }

    void *readPtr(unsigned int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return ptr_registry[id];
    }
    size_t readSize(unsigned int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return numrows_registry[id];
    }

    void read(unsigned int id, void *&ptr, size_t &size, unsigned short &keySize) {
        std::unique_lock<std::mutex> lock(registryMutex);
        ptr = ptr_registry[id];
        size = numrows_registry[id];
        keySize = keysz_registry[id];
    }

} globalRegistry;

unsigned int mkAll(unsigned int n_bits, size_t n_rows) {
    unsigned short keySize =  bitsToBytes(n_bits);
    size_t recSize = keySize + sizeof(value_t);
    byte **newstore = (byte **) calloc(n_rows, recSize);
    size_t sizeMB = n_rows * recSize / (1000 * 1000);
    if (sizeMB > 100) fprintf(stderr, "\nmkAll calloc : %lu MB\n", sizeMB);
    return globalRegistry.r_add(newstore,n_rows, keySize);
}

// append cuboid to registry, used both in dense and sparse case.

/* creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
*/
unsigned int mk(unsigned int n_bits) {
    if(n_bits >= (tempKeySize-1) * 8)
    fprintf(stderr, "Maximum supported bits exceeded");
    assert(n_bits < (tempKeySize-1) * 8);
    return globalRegistry.r_add(new std::vector<tempRec>(), 0, bitsToBytes(n_bits));
}

/* appends one record to an appendable (not yet frozen) sparse representation.

   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
*/
void add(unsigned int s_id, unsigned int n_bits, byte *key, value_t v) {
    tempRec myrec;
    unsigned int numbytes = bitsToBytes(n_bits);
    memcpy(&myrec.key[0], key, numbytes);
    myrec.val = v;

    //SBJ: Currently no other thread should be running. So no locks
    std::vector<tempRec> *p = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    p->push_back(myrec); // makes a copy of myrec
    globalRegistry.numrows_registry[s_id]++;
}

void add_i(size_t i, unsigned int s_id, byte *key, value_t v) {
    unsigned short keySize;
    size_t rows;
    void *ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    byte **store = (byte **) ptr;
    unsigned int recSize = keySize + sizeof(value_t);

    //can be multithreaded, but there should be no conflicts
    memcpy(getKey(store, i, recSize), key, keySize);
    memcpy(getVal(store, i, recSize), &v, sizeof(value_t));

}
void freeze(unsigned int s_id) {
    //SBJ: No other threads. No locks
    std::vector<tempRec> *store = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    unsigned short keySize = globalRegistry.keysz_registry[s_id];
    size_t rows = store->size();
    size_t recSize = keySize + sizeof(value_t);
    byte **newstore = (byte **) calloc(rows, recSize);
    size_t sizeMB = rows * recSize / (1000 * 1000);
    if (sizeMB > 100) fprintf(stderr, "\nfreeze calloc : %lu MB\n", sizeMB);

    for (size_t r = 0; r < rows; r++) {
        memcpy(getKey(newstore, r, recSize), &(*store)[r].key[0], keySize);
        memcpy(getVal(newstore, r, recSize), &((*store)[r].val), sizeof(value_t));
    }

#ifdef VERBOSE
    printf("\nFREEZE keySize = %d  recSize = %lu\n", keySize, recSize);
    for (unsigned i = 0; i < rows; i++) {
        print_key(10, getKey(newstore, i, recSize)); //hard coded 10 bits
        printf(" ");
        printf(" %lld ", *getVal(newstore, i, recSize));
        printf("\n");
    }
#endif
    delete store;
    globalRegistry.ptr_registry[s_id] = newstore;
}


size_t sz(unsigned int id) { return globalRegistry.readSize(id); }

size_t sNumBytes(unsigned int s_id) {
    unsigned short keySize;
    size_t rows;
    void *ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    unsigned int recSize = keySize + sizeof(value_t);
    return recSize * rows;

};
void sparse_print(unsigned int s_id, unsigned int n_bits) {

    void *ptr;
    size_t size;
    unsigned short keySize;
    globalRegistry.read(s_id, ptr, size, keySize);
    byte **store = (byte **) ptr;
    unsigned int recSize = keySize + sizeof(value_t);

    for (size_t i = 0; i < size; i++) {
        print_key(n_bits, getKey(store, i, recSize));
        printf(" ");
        printf(" %" PRId64, *getVal(store, i, recSize));
        printf("\n");
    }
}

void dense_print(unsigned int d_id, unsigned int n_bits) {
    void *ptr;
    size_t size;
    unsigned short keySize;
    globalRegistry.read(d_id, ptr, size, keySize);
    value_t *store = (value_t *) ptr;

    for (size_t i = 0; i < size; i++) {
        printf("%lu ", i);
        printf(" %" PRId64, store[i]);
        printf("\n");
    }
}

void readMultiCuboid(const char *filename,  int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                     unsigned int id_array[], unsigned int numCuboids) {
    printf("readMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "r");
    assert(fp != NULL);
    byte **buffer_array = new byte *[numCuboids];
    for (unsigned int i = 0; i < numCuboids; i++) {
        bool sparse = isSparse_array[i];

        //Note: Conversion from Int to unsigned int
        unsigned int n_bits = n_bits_array[i];
        //Note: Conversion from Int to size_t
        size_t size = size_array[i];


        if (sparse) {
            unsigned int keySize = bitsToBytes(n_bits);
            unsigned int recSize = keySize + sizeof(value_t);

            size_t byte_size = size * recSize;
            byte *buffer = (byte *) malloc(byte_size);
            size_t readBytes = fread(buffer, 1, byte_size, fp);
            if(readBytes != byte_size)
                fprintf(stderr, "size = %lu ,recSize=%u  -- %lu bytes read instead of %lu bytes because %s \n", size,recSize,  readBytes, byte_size, strerror(errno));
            assert(readBytes == byte_size);
            buffer_array[i] = buffer;
#ifdef VERBOSE
            printf("Read Sparse Cuboid i=%d,  nbits=%d, size=%d, keySize=%d, bufferSize=%lu\n", i, n_bits, size,
                   keySize, byte_size);
            for (unsigned i = 0; i < size; i++) {
                print_key(n_bits, getKey((byte **) buffer, i, recSize));
                printf(" ");
                printf(" %lld ", *getVal((byte **) buffer, i, recSize));
                printf("\n");
            }
#endif
        } else {
            size_t byte_size = (1LL << n_bits) * sizeof(value_t);
            byte *buffer = (byte *) malloc(byte_size);
            size_t readBytes = fread(buffer, 1, byte_size, fp);
            if(readBytes != byte_size)
                fprintf(stderr, "%lu bytes read instead of %lu bytes because %s \n", readBytes, byte_size, strerror(errno));
            assert(readBytes == byte_size);
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

void *read_cb(const char *filename, size_t byte_size) {
    unsigned char *b = (unsigned char *) malloc(byte_size);
    FILE *fp = fopen(filename, "r");
    assert(fp != NULL);
    fread(b, 1, byte_size, fp);
    fclose(fp);
    return b;
}

unsigned int readSCuboid(const char *filename, unsigned int n_bits, size_t  size) {
    printf("readSCuboid(\"%s\", %d, %lu)\n", filename, n_bits, size);
    unsigned int keySize = bitsToBytes(n_bits);
    unsigned int recSize = keySize + sizeof(value_t);
    size_t byte_size = size * recSize;
    unsigned int s_id = globalRegistry.r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    sparse_print(s_id, n_bits);
#endif
    return s_id;
}

unsigned int readDCuboid(const char *filename, unsigned int n_bits, size_t size) {
    printf("readDCuboid(\"%s\", %d, %lu)\n", filename, n_bits, size);

    assert(size == 1 << n_bits);
    unsigned int keySize = bitsToBytes(n_bits);
    size_t byte_size = size * sizeof(value_t);
    unsigned int d_id = globalRegistry.r_add(read_cb(filename, byte_size), size, keySize);
#ifdef VERBOSE
    dense_print(d_id, n_bits);
#endif
    return d_id;
}

void write_cb(const char *filename, void *data, unsigned int id, size_t byte_size) {
    printf("write_cb(\"%s\", %d): %lu bytes\n", filename, id, byte_size);

    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);

    size_t sw = fwrite(data, 1, byte_size, fp);
    if (sw != byte_size) {
        printf("Write Error: %s\n", strerror(errno));
        exit(1);
    }
    fclose(fp);
}

void writeSCuboid(const char *filename, unsigned int s_id) {
    void *data;
    size_t size;
    unsigned short keySize;
    globalRegistry.read(s_id, data, size, keySize);
    unsigned int recSize = keySize + sizeof(value_t);
    size_t byte_size = size * recSize;
    write_cb(filename, data, s_id, byte_size);
}

void writeDCuboid(const char *filename, unsigned int d_id) {
    void *data;
    size_t size;
    unsigned short keySize;
    globalRegistry.read(d_id, data, size, keySize);
    size_t byte_size = size * sizeof(value_t);
    write_cb(filename, data, d_id, byte_size);
}


void writeMultiCuboid(const char *filename, unsigned char isSparse_array[],  int ids[], unsigned int numCuboids) {
    printf("writeMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);
    for (unsigned int i = 0; i < numCuboids; i++) {
        size_t byte_size = 0;
        //Note: Conversion from Int to unsigned int
        unsigned int id = ids[i];
        bool sparse = isSparse_array[i];

        void *data;
        size_t size;
        unsigned short keySize;
        globalRegistry.read(id, data, size, keySize);

        if (sparse) {
            unsigned int recSize = keySize + sizeof(value_t);
            byte_size = size * recSize;
#ifdef VERBOSE
            printf("Write Sparse Cuboid i=%d byte_size=%lu \n", i, byte_size);
#endif
        } else {
            byte_size = size * sizeof(value_t);
#ifdef VERBOSE
            printf("Write Dense Cuboid i=%d byte_size=%lu \n", i, byte_size);
#endif
        }
        size_t sw = fwrite(data, 1, byte_size, fp);
        if (sw != byte_size) {
            printf("Write Error: %s. Written %lu bytes \n", strerror(errno), sw);
            exit(1);
        }
    }
    fclose(fp);
}


// must only be called for a dense array
value_t *fetch(unsigned int d_id, size_t &size) {
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);
    return (value_t *) ptr;
}


// rehashing ops


unsigned int srehash(unsigned int s_id, unsigned int *mask, unsigned int masklen) {
#ifdef VERBOSE
    printf("Begin srehash(%d, ", s_id);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
#endif
    unsigned short keySize;
    size_t rows;
    void *ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    byte **store = (byte **) ptr;
    unsigned int recSize = keySize + sizeof(value_t);


    unsigned int fromDim = masklen;
    unsigned int toDim = 0;
    for (unsigned int i = 0; i < masklen; i++)
        if (mask[i]) toDim++;

    unsigned int newKeySize = bitsToBytes(toDim);
    unsigned int newRecSize = newKeySize + sizeof(value_t);

    assert(newKeySize <= tempKeySize);

    const unsigned int tempRecSize = sizeof(tempRec);
    tempRec *tempstore = (tempRec *) calloc(rows, tempRecSize);

    size_t tempMB = rows * tempRecSize / (1000 * 1000);
    if (tempMB > 1000) fprintf(stderr, "\nsrehash temp calloc : %lu MB\n", tempMB);

    for (size_t r = 0; r < rows; r++) {
        project_key(masklen, tempKeySize, getKey(store, r, recSize), mask, getKey((byte **) tempstore, r, tempRecSize));
        memcpy(getVal((byte **) tempstore, r, tempRecSize), getVal(store, r, recSize), sizeof(value_t));
#ifdef VERBOSE
        print_key(masklen, getKey(store, r, recSize));
        printf(" -> (");
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %lld)\n", *getVal((byte **) tempstore, r, tempRecSize));
#endif
    }


    globalnumkeybytes = newKeySize; //TODO: Check if newKeySize is okay
    std::sort(tempstore, tempstore + rows, global_compare_keys);

#ifdef VERBOSE
    printf("AFter sorting\n");
    for (unsigned long r = 0; r < rows; r++) {
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %lld)\n", *getVal((byte **) tempstore, r, tempRecSize));
    }
#endif
    unsigned long watermark = 0;
    for (size_t r = 0; r < rows; r++)
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

    size_t  new_rows = watermark + 1;
    //printf("End srehash (compressing from %lu to %d)\n", rows, new_rows);

    byte **new_store = (byte **) calloc(new_rows, newRecSize);
    size_t numMB = new_rows * newRecSize / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\nsrehash calloc : %lu MB\n", numMB);
    for (size_t i = 0; i < new_rows; i++) {
        memcpy(getKey(new_store, i, newRecSize), getKey((byte **) tempstore, i, tempRecSize), newKeySize);
        memcpy(getVal(new_store, i, newRecSize), getVal((byte **) tempstore, i, tempRecSize), sizeof(value_t));
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

unsigned int drehash(unsigned int n_bits, unsigned int d_id, unsigned int d_bits,unsigned int *mask, unsigned int masklen) {
#ifdef VERBOSE
    printf("Begin drehash(%d, %d, %d, ", n_bits, d_id, d_bits);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
    printf("Input:\n");
    dense_print(d_id, n_bits);
    printf("\n");
#endif

    assert(n_bits == masklen);
    size_t size;
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);
    assert(size == (1LL << n_bits));
    value_t *store = (value_t *) ptr;

    unsigned int newsize = 1 << d_bits;

    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);
    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ndrehash calloc : %lu MB\n", numMB);

    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(value_t) * newsize);


    unsigned int newKeySize = bitsToBytes(d_bits);
    unsigned int oldKeySize = bitsToBytes(n_bits);
    for (size_t r = 0; r < size; r++) {
        byte src_key[oldKeySize], dest_key[oldKeySize];
        fromLong(src_key, r, oldKeySize);
/*
    unsigned long long r_cp = toLong(n_bits, src_key);
    if(r != r_cp) printf("ARGH %llu %llu\n", r, r_cp);
    assert(r == r_cp);
*/
        // print_key(masklen, src_key);
        project_key(masklen, newKeySize, src_key, mask, dest_key);
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

    unsigned int d_id2 = globalRegistry.r_add(newstore, newsize, newKeySize);

#ifdef VERBOSE
    printf("Result:\n");
    dense_print(d_id2, d_bits);
    printf("\n");
    printf("End drehash\n");
#endif

    return d_id2;
}

unsigned int s2drehash(unsigned int s_id, unsigned int d_bits, unsigned int *mask, unsigned int masklen) {
#ifdef VERBOSE
    printf("Begin s2drehash(%d, %d, ", s_id, d_bits);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
    printf("Input:\n");
    sparse_print(s_id, masklen);
    printf("\n");
#endif

    size_t rows;
    void *ptr;
    unsigned short keySize;
    globalRegistry.read(s_id, ptr, rows, keySize);
    unsigned int recSize = keySize + sizeof(value_t);

    byte **store = (byte **) ptr;

   size_t newsize = 1LL << d_bits;
    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);

    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehash calloc : %lu MB\n", numMB);
    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(value_t) * newsize);

    for (size_t r = 0; r < rows; r++) {
        byte dest_key[keySize];
//    print_key(masklen, getKey(store, r, recSize));
        project_key(masklen, keySize, getKey(store, r, recSize), mask, dest_key);
        unsigned long long i = toLong(d_bits, dest_key);
        newstore[i] += *getVal(store, r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
    }

    unsigned int d_id = globalRegistry.r_add(newstore, newsize, bitsToBytes(d_bits));

#ifdef VERBOSE
    printf("Result:\n");
    dense_print(d_id, d_bits);
    printf("End s2drehash\n");
#endif

    return d_id;
}

unsigned int d2srehash(unsigned int n_bits, unsigned int d_id, unsigned int *mask, unsigned int masklen) {
    size_t size;
    unsigned short oldKeySize;
    void *ptr;

    globalRegistry.read(d_id, ptr, size, oldKeySize);
    value_t *store = (value_t *) ptr;
    assert(size == (1 << masklen));

    size_t newrows = size;

    unsigned int d_bits = 0;
    for (unsigned int i = 0; i < masklen; i++)
        if (mask[i]) d_bits++;


    unsigned int newKeySize = bitsToBytes(d_bits);
    unsigned int newRecSize = newKeySize + sizeof(value_t);

    byte **newstore = (byte **) calloc(newrows, newRecSize);
    size_t numMB = newrows * newRecSize / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\nd2srehash calloc : %lu MB\n", numMB);
    fprintf(stderr, "WARNING: d2srehash -- aggregation not implemented.\n");

    for (size_t r = 0; r < size; r++) {
        byte src_key[oldKeySize];
        fromLong(src_key, r, oldKeySize);
        project_key(masklen, newKeySize, src_key, mask, getKey(newstore, r, newKeySize));
        *getVal(newstore, r, newRecSize) += store[r];
    }

    return globalRegistry.r_add(newstore, newrows, newKeySize);
}


int shybridhash(unsigned int s_id, unsigned int *mask, unsigned int masklen) {
    unsigned short keySize;
    size_t rows;
    void *ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    byte **store = (byte **) ptr;
    unsigned int recSize = keySize + sizeof(value_t);


    unsigned int masksum = 0;
    for (unsigned int i = 0; i < masklen; i++)
        if (mask[i]) masksum++;

    unsigned int maskpos[masksum];
    for (unsigned int i = 0, j = 0; i < masklen; i++)
        if (mask[i]) maskpos[j++] = i;

    unsigned int newKeySize = bitsToBytes(masksum);
    unsigned int newRecSize = newKeySize + sizeof(value_t);
    const unsigned int tempRecSize = sizeof(tempRec);

    size_t dense_rows = (1LL << masksum);
    size_t dense_size = dense_rows * sizeof(value_t);  //overflows when to_dim  > 61
    size_t temp_size = rows * tempRecSize;

    if (masksum >= 40 || temp_size < dense_size) {
        //do sorting based approach and return sparse cuboid.
        return srehash(s_id, mask, masklen);
    } else {
        //map to dense, and then remap to sparse if necessary

        value_t *dense_store = (value_t *) calloc(dense_rows, sizeof(value_t));

        assert(dense_store);

        size_t numMB = dense_size / (1000 * 1000);
        if (numMB > 1000) fprintf(stderr, "\nhybrid rehash dense calloc : %lu MB\n", numMB);
        // all intervals are initially invalid -- no constraint
        memset(dense_store, 0, dense_size);

        size_t sparse_rows = 0;
        for (size_t r = 0; r < rows; r++) {
//    print_key(masklen, getKey(store, r, recSize));
            unsigned long long i = project_key_toLong2(masksum, getKey(store, r, recSize), maskpos);
            auto newval = *getVal(store, r, recSize);
            if (dense_store[i] == 0 && newval > 0)
                sparse_rows++;
            dense_store[i] += newval;
//    printf(" %lld %d\n", i, newstore[i]);
        }
       size_t sparseSize = sparse_rows * newRecSize;

        if (sparseSize < 0.5 * dense_size) {
            //switch to sparse representation
            byte **sparse_store = (byte **) calloc(sparse_rows, newRecSize);
            size_t numMB = sparse_rows * newRecSize / (1000 * 1000);
            if (numMB > 1000) fprintf(stderr, "\nhybrid rehash sparse calloc : %lu MB\n", numMB);
            size_t w = 0;
            byte dest_key[newKeySize];
            for (size_t r = 0; r < dense_rows; r++) {
                if (dense_store[r] != 0) {
                    fromLong(dest_key, r, newKeySize);
                    memcpy(getKey(sparse_store, w, newRecSize), dest_key, newKeySize);
                    memcpy(getVal(sparse_store, w, newRecSize), dense_store + r, sizeof(value_t));
                    w++;
                }
            }
            assert(w == sparse_rows);
            free(dense_store);
            unsigned id = globalRegistry.r_add(sparse_store, sparse_rows, newKeySize);
            return id;
        } else {
            int d_id = globalRegistry.r_add(dense_store, dense_rows, newKeySize);
            return -d_id;  //SBJ: Negative value indicates that it is dense
        }
    }
}
