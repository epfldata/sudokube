#include <vector>
#include <algorithm>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <mutex>
#include "Keys.h"
#include "SetTrie.h"
#include "Payload.h"
#include <cstring>
#include <iostream>
//#include <chrono>
//using namespace std::chrono;
//#define VERBOSE


//STL requires a static type. So we use maximum possible key size. We support only 40*8 = 320 bits for those functions.
const unsigned int tempKeySize = 40;

struct tempRec {
    byte key[tempKeySize];
    value_t val;
};

SetTrie globalSetTrie;

/**
 We support only nbits <= 320 and nrows < 2^31 currently. We still use size_t for numrows because the numbytes obtained
 by multiplying by record size may cross IntMax, and produces weird results when both recSize and numRows are unsigned ints.

 */

//Used in sorting within global_compare_keys  to store the number of bytes (out of tempKeySize=40) that need to be compared
 thread_local unsigned short globalnumkeybytes = 0;

/*
 * Returns whether k1 < k2 . Assumes the bytes are in little endian (LS byte first). Requires globalnumkeybytes to be set.
 * TODO: Check why :: If inlined, weird behaviour( no sorting happens) when run multiple times within the same sbt instance. No issue for the first time.
 */
 bool global_compare_keys(const tempRec &k1, const tempRec &k2) {
    //DO NOT use unsigned !! i >= 0 always true
    for (short i = globalnumkeybytes - 1; i >= 0; i--) {
        if (k1.key[i] < k2.key[i]) return true;
        if (k1.key[i] > k2.key[i]) return false;
    }
    return false;
}


/**
 * Various registries storing in-memory cuboids and their meta-information.
 */

struct globalRegistry_t {
    /**pointer to dense or sparse cuboid*/
    std::vector<void *> ptr_registry;

    /**
     * Stores numrows for both dense and sparse cuboids
     * Currently we support only number of rows < IntMax */
    std::vector<size_t> numrows_registry;

    /** stores size of key in bytes for sparse cuboids. Not relevant for dense cuboids. May be possible to remove it. */
    std::vector<unsigned short> keysz_registry;

    /** for synchronization when multiple java threads try to update this registry simulataneously */
    std::mutex registryMutex;

    //Do not call when unfrozen cuboids are present
    void clear() {
        std::unique_lock<std::mutex> lock(registryMutex);
        for (auto ptr: ptr_registry)
            free(ptr);
        ptr_registry.clear();
        numrows_registry.clear();
        keysz_registry.clear();
    }

    /**
        Adds a dense or sparse cuboid to the registry
        @param p  Pointer to the array representing the dense or sparse cuboid
        @param size Number of rows in cuboid, relevant for sparse cuboids
        @param keysize Size of the key (in bytes), relevant for sparse cuboids
        @returns unique id for the added cuboid
    */
    unsigned int r_add(void *p, size_t size, unsigned short keysize) {
        std::unique_lock<std::mutex> lock(registryMutex);
//TODO: It is possible that some thread might access when these vectors are resized.
        ptr_registry.push_back(p);
        numrows_registry.push_back(size);
        keysz_registry.push_back(keysize);
        return ptr_registry.size() - 1;
    }

    /**
     * Adds several dense or sparse cuboids to the registry in a batch
     * @param p_array Array of pointers to dense/sparse cuboids
     * @param size_array Array of number of rows in each cuboid
     * @param n_bits_array Array of number of key bits of each cuboid
     * @param id_array Array of unique ids for each cuboid to be returned by this function
     * @param numCuboids Total number of cuboids in this batch, and the sizes of each of input arrays.
     */
    void multi_r_add(byte *p_array[], int size_array[], int n_bits_array[], unsigned int id_array[],
                     unsigned int numCuboids) {
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

    /** Thread safe access to pointer to cuboid */
    void *readPtr(unsigned int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return ptr_registry[id];
    }

    /**Thread safe access to number of rows of some cuboid */
    size_t readSize(unsigned int id) {
        std::unique_lock<std::mutex> lock(registryMutex);
        return numrows_registry[id];
    }

    /**
     * Thread safe access to a cuboid and meta information
     * @param id Unique id of the cuboid to be retrieved
     * @param ptr Output parameter storing pointer to cuboid
     * @param size Output parameter storing number of rows in the cuboid
     * @param keySize Output parameter storing keysize in bytes
     */
    void read(unsigned int id, void *&ptr, size_t &size, unsigned short &keySize) {
        std::unique_lock<std::mutex> lock(registryMutex);
        ptr = ptr_registry[id];
        size = numrows_registry[id];
        keySize = keysz_registry[id];
    }

};
globalRegistry_t globalRegistry;

void readRegistry(unsigned int id, void *&ptr, size_t &size, unsigned short &keySize) {
    globalRegistry.read(id, ptr, size, keySize);
}
/**
 * Resets the backend by unloading all cuboids from memory and clearing the registry
 */
void reset() {
    globalRegistry.clear();
}

/**
 * Initialize and pre-allocate new cuboid with a given size. The data is added using add_i method to add specific rows.
 * No need to freeze. Supports multi-threaded insertion of records to different positions using add_i.
 * @see add_i
 * @param n_bits Number of bits in the key
 * @param n_rows Number of rows in the cuboid
 * @return Unique id for the new cuboid
 */
unsigned int mkAll(unsigned int n_bits, size_t n_rows) {
    unsigned short keySize = bitsToBytes(n_bits);
    size_t recSize = keySize + sizeof(value_t);
    byte **newstore = (byte **) calloc(n_rows, recSize);
    size_t sizeMB = n_rows * recSize / (1000 * 1000);
    if (sizeMB > 100) fprintf(stderr, "\nmkAll calloc : %lu MB\n", sizeMB);
    return globalRegistry.r_add(newstore, n_rows, keySize);
}

/**
 * creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
   Does not support multi-threading.
   Supports cuboids with maximum of 320 key bits
*/
unsigned int mk(unsigned int n_bits) {
    if (n_bits >= (tempKeySize - 1) * 8)
        fprintf(stderr, "Maximum supported bits exceeded");
    assert(n_bits < (tempKeySize - 1) * 8);
    return globalRegistry.r_add(new std::vector<tempRec>(), 0, bitsToBytes(n_bits));
}

/**
 * appends one record to an appendable (not yet frozen) sparse representation initialized by mk().
   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
   Does not support multi-threading
*/
void add(unsigned int s_id, unsigned int n_bits, byte *key, value_t v) {
    tempRec myrec;
    unsigned int numbytes = bitsToBytes(n_bits);
    memcpy(&myrec.key[0], key, numbytes);
    myrec.val = v;

    //SBJ: Currently no other thread should be running. So no locks
    //Adding to std::vector requires static type. So using tempRec
    std::vector<tempRec> *p = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    p->push_back(myrec); // makes a copy of myrec
    globalRegistry.numrows_registry[s_id]++;
}

/**
 * Adds record at specified position to a cuboid initialized by mkAll()
 * Thread-safe, as long as there are no conflicts on the parameter i
 * @param i The index at which new record is to be added
 * @param s_id The id of the cuboid (returned by mkAll) to which record is to be added
 * @param key Record key
 * @param v Record value
 */
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

/**
 * Finalizes cuboid construction intiatied by mk(). Changes internal representation of cuboid that is compatible with rest of the system
 * Also sets the cuboid size. Only single thread may call it.
 * @param s_id Id of the cuboid (retured by mk()) to be finalized.
 */
void freezePartial(unsigned int s_id, unsigned int n_bits) {
    //SBJ: No other threads. No locks
    std::vector<tempRec> *store = (std::vector<tempRec> *) globalRegistry.ptr_registry[s_id];
    unsigned short keySize = bitsToBytes(n_bits);
    globalRegistry.keysz_registry[s_id] = keySize;
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

/**
 * Returns size (number of rows) of the cuboid with specified id.
 */
size_t sz(unsigned int id) { return globalRegistry.readSize(id); }

/**
 * Returns number of bytes of storage used by cuboid with specified id.
 * Valid for only sparse cuboids
 */
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

/**
 * Reads batch of cuboids from file and loads them into RAM
 * @param filename Name of file containing the cuboids
 * @param n_bits_array Number of key bits, one for each cuboid, in an array
 * @param size_array  Number of rows, one for each cuboid, in an array
 * @param isSparse_array Boolean representing whether a cuboid is sparse, one for each cuboid, in an array
 * @param id_array Output parameter : Unique id , one for each cuboid, in an array
 * @param numCuboids Number of cuboids in the file, and also sizes of the input array parameters.
 */
void readMultiCuboid(const char *filename, int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                     unsigned int id_array[], unsigned int numCuboids) {
    //printf("readMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
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
            if (readBytes != byte_size)
                fprintf(stderr, "File %s  :: size = %lu, recSize=%u  -- %lu bytes read instead of %lu bytes because %s \n", filename, size,
                        recSize, readBytes, byte_size, strerror(errno));
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
            if (readBytes != byte_size)
                fprintf(stderr, "File %s  :: %lu bytes read instead of %lu bytes because %s \n", filename, readBytes, byte_size,
                        strerror(errno));
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


/**
 * Writes a batch of cuboids to file
 * @param filename Name of file
 * @param isSparse_array Boolean value indicating whether the cuboid is sparse or dense, one per cuboid, in an array
 * @param ids  Id of cuboid to be written
 * @param numCuboids  Total number of cuboids in the batch, and the size of the arrays.
 */
void writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[], unsigned int numCuboids) {
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


/**
 * Retrieves the contents of a dense cuboid as an  array.
 * Must only be called on a dense cuboid
 * @param d_id Id of the dense cuboid
 * @param size Output parameter storing the number of rows in the cuboid
 * @return Pointer to array containing the data of the specified dense cuboid
 */
value_t *fetch(unsigned int d_id, size_t &size) {
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);
    return (value_t *) ptr;
}

/**
 *  Garbage collects storage used by  cuboid
 *  Does not fully delete the cuboid !!
 *  Should only be used on temporary cuboids created while querying
 *  @param id ID of the cuboid
 */
void cuboid_GC(unsigned int id) {
    size_t size;
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(id, ptr, size, keySize);
    free(ptr);
}

/**
 * Aggregates a sparse cuboid to another sparse cuboid according to a specific mask
 * Uses sorting on tempRec typed records -- avoid this function if possible
 * @param s_id Id of the cuboid being aggregated
 * @param maskpos The indexes of the mask where bit is 1
 * @param masksum Total number of 1s in the mask
 * @return Id of the new cuboid
 */
unsigned int srehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum) {
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


    unsigned int toDim = masksum;


    unsigned int newKeySize = bitsToBytes(toDim);
    unsigned int newRecSize = newKeySize + sizeof(value_t);

    assert(newKeySize <= tempKeySize);

    //allocate tempRec-typed array of the same size as the source cuboid
    const unsigned int tempRecSize = sizeof(tempRec);
    tempRec *tempstore = (tempRec *) calloc(rows, tempRecSize);

    size_t tempMB = rows * tempRecSize / (1000 * 1000);
    if (tempMB > 1000) fprintf(stderr, "\nsrehash temp calloc : %lu MB\n", tempMB);

    //project each record into tempRec-typed record
    for (size_t r = 0; r < rows; r++) {
        project_Key_to_Key(masksum, maskpos, getKey(store, r, recSize), getKey((byte **) tempstore, r, tempRecSize));
        memcpy(getVal((byte **) tempstore, r, tempRecSize), getVal(store, r, recSize), sizeof(value_t));
#ifdef VERBOSE
        print_key(masklen, getKey(store, r, recSize));
        printf(" -> (");
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %lld)\n", *getVal((byte **) tempstore, r, tempRecSize));
#endif
    }

    //sort the tempRecs based on new key
    globalnumkeybytes = newKeySize; //TODO: Check if newKeySize is okay
    std::sort(tempstore, tempstore + rows, global_compare_keys);

#ifdef VERBOSE
    printf("AFter sorting\n");
    for (unsigned long r = 0; r < rows; r++) {
        print_key(masklen, getKey((byte **) tempstore, r, tempRecSize));
        printf(", %lld)\n", *getVal((byte **) tempstore, r, tempRecSize));
    }
#endif

    //aggregate rows with same (new) key
    unsigned long watermark = 0;
    for (size_t r = 0; r < rows; r++)
        if (watermark < r) {
            byte *key1 = getKey((byte **) tempstore, watermark, tempRecSize);
            byte *key2 = getKey((byte **) tempstore, r, tempRecSize);
            bool cmp = compare_keys(key1, key2, newKeySize);
#ifdef VERBOSE
            printf(" Comparing ");
            print_key(masklen, key1);
            printf(" and ");
            print_key(masklen, key2);
            printf("  ==> %d \n", cmp);
#endif
            value_t  value = *getVal((byte **) tempstore, r, tempRecSize);
            if (value == 0) continue;
            if (!cmp) { // same keys
                //printf("Merge %lu into %lu\n", r, watermark);
                *getVal((byte **) tempstore, watermark, tempRecSize) += value;
            } else {
                if(*getVal((byte **) tempstore, watermark, tempRecSize) > 0) watermark++; //overwrite 0 vals
                if (watermark < r) {
                    //printf("Copy %lu into %lu\n", r, watermark);
                    memcpy(getKey((byte **) tempstore, watermark, tempRecSize),
                           getKey((byte **) tempstore, r, tempRecSize), tempRecSize);

                }
            }
        }
    if(*getVal((byte **) tempstore, watermark, tempRecSize) == 0) watermark--;
    size_t new_rows = watermark + 1;
    //printf("End srehash (compressing from %lu to %d)\n", rows, new_rows);

    //Convert from tempRec-type record to that of correct size
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

/**
 * Aggregates a dense cuboid into a dense cuboid.
 * @param d_id Id of the source cuboid
 * @param maskpos Indexes of mask where bit is 1
 * @param masksum Number of 1s in the mask
 * @return Id of new cuboid
 */
unsigned int drehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum) {
#ifdef VERBOSE
    printf("Begin drehash(%d, %d, %d, ", n_bits, d_id, d_bits);
    for (int i = masklen - 1; i >= 0; i--) printf("%d", mask[i]);
    printf(", %d)\n", masklen);
    printf("Input:\n");
    dense_print(d_id, n_bits);
    printf("\n");
#endif

    size_t size;
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);

    value_t *store = (value_t *) ptr;

    unsigned int newsize = 1 << masksum;

    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);
    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ndrehash calloc : %lu MB\n", numMB);

    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(value_t) * newsize);


    unsigned int newKeySize = bitsToBytes(masksum);
    for (size_t r = 0; r < size; r++) {
        size_t i = project_Long_to_Long(masksum, maskpos, r);

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

/**
 * Aggregates sparse cuboid to dense. Called before fetch() or if the aggregated cuboid is likely to be dense
 * @param s_id  Id of sparse cuboid
 * @param maskpos Indexes of mask where bit is 1
 * @param masksum Number of 1s in mask
 * @return Id of the dense cuboid
 */
unsigned int s2drehash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum) {
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

    size_t newsize = 1LL << masksum;
    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);

    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehash calloc : %lu MB\n", numMB);
    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(value_t) * newsize);

    for (size_t r = 0; r < rows; r++) {
//    print_key(masklen, getKey(store, r, recSize));
        size_t i = project_Key_to_Long(masksum, maskpos, getKey(store, r, recSize));
        newstore[i] += *getVal(store, r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
    }

    unsigned int d_id = globalRegistry.r_add(newstore, newsize, bitsToBytes(masksum));

#ifdef VERBOSE
    printf("Result:\n");
    dense_print(d_id, d_bits);
    printf("End s2drehash\n");
#endif

    return d_id;
}

/**
 * Converts a dense cuboid to a sparse cuboid. Aggregation of records with same key is not yet implemented
 * @param d_id  Id of source cuboid
 * @param maskpos Indexes of mask where bit is 1
 * @param masksum Number of 1s in  mask
 * @return Id of new cuboid
 */
unsigned int d2srehash(unsigned int d_id, unsigned int *maskpos, unsigned int masksum) {
    size_t size;
    unsigned short oldKeySize;
    void *ptr;

    globalRegistry.read(d_id, ptr, size, oldKeySize);
    value_t *store = (value_t *) ptr;

    size_t newrows = size;


    unsigned int newKeySize = bitsToBytes(masksum);
    unsigned int newRecSize = newKeySize + sizeof(value_t);

    byte **newstore = (byte **) calloc(newrows, newRecSize);
    size_t numMB = newrows * newRecSize / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\nd2srehash calloc : %lu MB\n", numMB);
    fprintf(stderr, "WARNING: d2srehash -- aggregation not implemented.\n");

    for (size_t r = 0; r < size; r++) {
        project_Long_to_Key(masksum, maskpos, r, getKey(newstore, r, newKeySize));
        *getVal(newstore, r, newRecSize) += store[r];
    }

    return globalRegistry.r_add(newstore, newrows, newKeySize);
}

/**
 * Aggregates a sparse cuboid into appropriate sparse/dense cuboid
 * @param s_id Id of the source cuboid
 * @param maskpos Index of mask where bit is 1
 * @param masksum Number of 1s in mask
 * @return Id of the new cuboid. If the id is negative, then it means the new cuboid is dense.
 */
int shybridhash(unsigned int s_id, unsigned int *maskpos, unsigned int masksum) {
    unsigned short keySize;
    size_t rows;
    void *ptr;
    globalRegistry.read(s_id, ptr, rows, keySize);
    byte **store = (byte **) ptr;
    unsigned int recSize = keySize + sizeof(value_t);

    unsigned int newKeySize = bitsToBytes(masksum);
    unsigned int newRecSize = newKeySize + sizeof(value_t);
    const unsigned int tempRecSize = sizeof(tempRec);

    size_t dense_rows = (1LL << masksum);
    size_t dense_size = dense_rows * sizeof(value_t);  //overflows when to_dim  > 61
    size_t temp_size = rows * tempRecSize;

    //if dense size is too big, fall back to sorting based approach
    if (masksum >= 40 || temp_size < dense_size) {
        //do sorting based approach and return sparse cuboid.
        return srehash(s_id, maskpos, masksum);
    } else {
        //map to dense, and then remap to sparse if necessary
        value_t *dense_store = (value_t *) calloc(dense_rows, sizeof(value_t));
        assert(dense_store);

        size_t numMB = dense_size / (1000 * 1000);
        if (numMB > 1000) fprintf(stderr, "\nhybrid rehash dense calloc : %lu MB\n", numMB);

        memset(dense_store, 0, dense_size);

        size_t sparse_rows = 0;
        for (size_t r = 0; r < rows; r++) {
            byte *from_key = getKey(store, r, recSize);
            auto newval = *getVal(store, r, recSize);
            size_t i = project_Key_to_Long(masksum, maskpos, from_key);
            //Count non-zero rows
            if (dense_store[i] == 0 && newval > 0)
                sparse_rows++;
            dense_store[i] += newval;
        }
        size_t sparseSize = sparse_rows * newRecSize;
        //if the new cuboid is not dense enough then convert to sparse cuboid
        if (sparseSize < 0.5 * dense_size) {
            //switch to sparse representation
            byte **sparse_store = (byte **) calloc(sparse_rows, newRecSize);
            size_t numMB = sparse_rows * newRecSize / (1000 * 1000);
            if (numMB > 1000) fprintf(stderr, "\nhybrid rehash sparse calloc : %lu MB\n", numMB);
            size_t w = 0;

            for (size_t r = 0; r < dense_rows; r++) {
                if (dense_store[r] != 0) {
                    from_Long_to_Key(newKeySize, r, getKey(sparse_store, w, newRecSize));
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

bool addDenseCuboidToTrie(const std::vector<int> &cuboidDims, unsigned int d_id) {
    std::cout << "DenseCuboid "<< d_id << " of dimensionality " << cuboidDims.size();
    size_t len;
    value_t *values = fetch(d_id, len);
    std::vector<value_t> valueVec(values, values + len);
    momentTransform(valueVec);
    globalSetTrie.insertAll(cuboidDims, valueVec);
    std::cout << "  TrieCount = " << globalSetTrie.count << std::endl;
    return globalSetTrie.count < globalSetTrie.maxSize;
}

bool addSparseCuboidToTrie(const std::vector<int> &cuboidDims, unsigned int s_id) {
    std::cout << "SparseCuboid" << s_id << " of dimensionality " << cuboidDims.size();
    size_t rows;
    void *ptr;
    unsigned short keySize;
    globalRegistry.read(s_id, ptr, rows, keySize);
    unsigned int recSize = keySize + sizeof(value_t);
    byte **store = (byte **) ptr;

    size_t newsize = 1LL << cuboidDims.size();
    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);

    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehash2 calloc : %lu MB\n", numMB);
    memset(newstore, 0, sizeof(value_t) * newsize);

    for (size_t r = 0; r < rows; r++) {
        size_t i = from_Key_to_Long(keySize, getKey(store, r, recSize));
        newstore[i] += *getVal(store, r, recSize);
//    printf(" %lld %d\n", i, newstore[i]);
    }

    std::vector<value_t> valueVec(newstore, newstore + newsize);
    momentTransform(valueVec);

    globalSetTrie.insertAll(cuboidDims, valueVec);
    std::cout << "  TrieCount = " << globalSetTrie.count << std::endl;
    free(newstore);
    return globalSetTrie.count < globalSetTrie.maxSize;
}

byte* slice_mask_to_index(unsigned int bitposlength, bool * sm) {
    size_t sourceSize = 1 << bitposlength;
    size_t destSize = sourceSize << 1;
    auto idx = new byte[destSize];
    //idx starts from 1. the last sourceSize entries is the original array
    memcpy(idx + sourceSize, sm , sourceSize);
    for(size_t i = sourceSize - 1; i > 0; i --) {
        auto leftChild = i << 1;
        auto rightChild = leftChild + 1;
        byte left = idx[leftChild];
        byte right = idx[rightChild];
        if(left == right) {
            idx[i] = left;
        } else {
            idx[i] = 2;
        }
    }
//    fprintf(stderr, "Tree index ");
//    for(int logh = 0; logh <= bitposlength; ++logh) {
//        fprintf(stderr, "\n");
//        for(int i = 1 << logh; i < 1 << (logh + 1); i++) {
//            fprintf(stderr, "%u ", idx[i]);
//        }
//    }
    return idx;
}
value_t* s2drehashSlice(unsigned int s_id, unsigned int *bitpos, unsigned int bitposlength, bool* sliceMask) {

    auto sliceIdx = slice_mask_to_index(bitposlength, sliceMask);
    size_t rows;
    void *ptr;
    unsigned short keySize;
    globalRegistry.read(s_id, ptr, rows, keySize);
    unsigned int recSize = keySize + sizeof(value_t);

    byte **store = (byte **) ptr;

    size_t newsize = 1LL << bitposlength;
    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);

    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ns2drehashSlice calloc : %lu MB\n", numMB);

    memset(newstore, 0, sizeof(value_t) * newsize);

    for (size_t r = 0; r < rows; r++) {
//    print_key(masklen, getKey(store, r, recSize));
        size_t i = project_Key_to_Long_WithSlice(bitposlength, bitpos, getKey(store, r, recSize), sliceIdx);
        if(i < newsize) {
            newstore[i] += *getVal(store, r, recSize);
//            fprintf(stderr, " newstore[%lld] = %d\n", i, newstore[i]);
        }
    }
    return newstore;
}

value_t* drehashSlice(unsigned int d_id, unsigned int *bitpos, unsigned int bitposlength, bool* sliceMask) {

//   fprintf(stderr, "Making tree index... ");
    auto sliceIdx = slice_mask_to_index(bitposlength, sliceMask);
//    fprintf(stderr, "   Done\n");
    size_t size;
    unsigned short keySize;
    void *ptr;
    globalRegistry.read(d_id, ptr, size, keySize);

    value_t *store = (value_t *) ptr;

    unsigned int newsize = 1 << bitposlength;

    value_t *newstore = (value_t *) calloc(newsize, sizeof(value_t));
    assert(newstore);
    size_t numMB = newsize * sizeof(value_t) / (1000 * 1000);
    if (numMB > 100) fprintf(stderr, "\ndrehash calloc : %lu MB\n", numMB);

    // all intervals are initially invalid -- no constraint
    memset(newstore, 0, sizeof(value_t) * newsize);


    unsigned int newKeySize = bitsToBytes(bitposlength);
    for (size_t r = 0; r < size; r++) {
        size_t i = project_Long_to_Long_WithSlice(bitposlength, bitpos, r, sliceIdx);
        if (i < newsize) {
            newstore[i] += store[r];
//            fprintf(stderr, " newstore[%lld] = %d\n", i, newstore[i]);
        }
    }
    return newstore;
}

void initTrie(size_t maxsize) { globalSetTrie.init(maxsize); }
void saveTrie(const char *filename) { globalSetTrie.saveToFile(filename); }
void loadTrie(const char *filename) { globalSetTrie.loadFromFile(filename); }

void prepareFromTrie(const std::vector<int>& query, std::map<int, value_t>& result) { globalSetTrie.getNormalizedSubset(query, result, 0, 0, globalSetTrie.nodes);}