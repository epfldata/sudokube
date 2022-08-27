//
// Created by Sachin Basil John on 23.08.22.
//

#ifndef SUDOKUBECBACKEND_ROWSTORE_H
#define SUDOKUBECBACKEND_ROWSTORE_H

#include "common.h"
#include <vector>
#include <mutex>
#include <algorithm>


struct RowStore {
    struct SparseCuboidRow : Cuboid {
        uint16_t keySize;
        uint16_t recSize;

        static inline uint16_t bitsToBytes(uint16_t nc) { return (nc + 8) >> 3; }  //TODO: Change 8 to 7
        void realloc() {
            if (!ptr) free(ptr);
            ptr = calloc(numRows, recSize);
        }

        SparseCuboidRow() : Cuboid(), keySize(0), recSize(0) {}

        SparseCuboidRow(Cuboid &&that) : SparseCuboidRow(that.ptr, that.numRows, that.numCols) {}

        SparseCuboidRow(Cuboid &that) : SparseCuboidRow(that.ptr, that.numRows, that.numCols) {}

        SparseCuboidRow(void *p, size_t nr, uint16_t nc) : Cuboid(p, nr, nc, false) {
            keySize = bitsToBytes(numCols);
            recSize = keySize + sizeof(Value);
        }
    };

    using Key = byte *;
    using Record = byte *;
    using BitPos = std::vector<unsigned int>;

//    static bool debug;
    std::vector<Cuboid> registry;
    /** for synchronization when multiple java threads try to update this registry simulataneously */
    std::mutex registryMutex;
    std::mutex STMutex;

    //STL requires a static type. So we use maximum possible key size. We support only 40*8 = 320 bits for those functions.
    static const unsigned int rowStoreTempKeySizeWords = 5;

    struct RowStoreTempRec {
        uint64_t key[rowStoreTempKeySizeWords];
        Value val;
    };

    /*
    * Returns whether k1 < k2 . Assumes the bytes are in little endian (LS byte first). Requires globalnumkeybytes to be set.
    * TODO: Check why :: If inlined, weird behaviour( no sorting happens) when run multiple times within the same sbt instance. No issue for the first time.
    */
    static bool temprec_compare_keys(const RowStoreTempRec &k1, const RowStoreTempRec &k2) {
        //DO NOT use unsigned !! i >= 0 always true
        for (short i = rowStoreTempKeySizeWords - 1; i >= 0; i--) {
            if (k1.key[i] < k2.key[i]) return true;
            if (k1.key[i] > k2.key[i]) return false;
        }
        return false;
    }
    /**
* Compare two dynamically typed keys of the same size
* @param numkeybytes Maximum number of bytes to be compared
* @return true if k1 < k2 , else false
*/
    inline static bool compare_keys(const Key &k1, const Key &k2, const int numkeybytes) {
        //Warning : Do not use unsigned here. i>= 0 always true
        for (int i = numkeybytes - 1; i >= 0; i--) {
            if (k1[i] < k2[i]) return true;
            if (k1[i] > k2[i]) return false;
        }
        return false;
    }
/**
 * Returns the pointer to LSB of key at some idx in a dynamic sized array of records
 * @param array Array representing sparse cuboid
 * @param idx Index of record whose key is to be fetched
 * @param recSize Size of the dynamic typed record (in bytes)
 * @returns pointer to least significant byte of key
 */
    inline static byte *getKey(void *array, size_t idx, size_t recSize) {
        //Convert to byte* because we do byte-sized pointer arithmetic
        byte *key = (byte *) array + recSize * idx;
        return key;
    }

    /**
 * Returns the pointer to value at some idx in a dynamic sized array of records
 * @param array Array representing sparse cuboid
 * @param idx Index of record whose key is to be fetched
 * @param recSize Size of the dynamic typed record (in bytes)
 * @returns pointer to value
 */
    inline static Value *getVal(void *array, size_t idx, size_t recSize) {
        //Convert to byte* because we do byte-sized pointer arithmetic
        Value *val = (Value *) ((byte *) array + (idx + 1) * recSize - sizeof(Value));
        return val;
    }

    inline static void print_key(int n_bits, const Key &key) {
        for (int pos = n_bits - 1; pos >= 0; pos--) {
            int b = (key[pos >> 3] >> (pos & 0x7)) & 0x1;
            if (b) printf("1");
            else printf("0");
            if ((pos & 0x7) == 0)
                printf(" ");
        }
    }


    //Do not call when unfrozen cuboids are present
    void clear();

    /**
    Adds a dense or sparse cuboid to the registry
    @param cuboid Cuboid to be added
    @returns unique id for the added cuboid
*/
    unsigned int registry_add(const Cuboid &cuboid);

    /**
 * Adds several dense or sparse cuboids to the registry in a batch
    @param cuboids Cuboids to be added
    @param id of the first cuboid that was added
 */
    unsigned int multi_r_add(const std::vector<Cuboid> &cuboids);

    Cuboid read(unsigned int id);

    //Must not be called when multiple threads are accessing
    Cuboid &unsafeRead(unsigned int id);


    size_t numRowsInCuboid(unsigned int id) {
        Cuboid c = read(id);
        return c.numRows;
    }

    /**
 * Returns number of bytes of storage used by cuboid with specified id.
 * Valid for only sparse cuboids
 */
    size_t numBytesInSparseCuboid(unsigned int s_id) {
        SparseCuboidRow c(read(s_id));
        return c.recSize * c.numRows;

    };

    /**
 * Initialize and pre-allocate new cuboid with a given size. The data is added using add_i method to add specific rows.
 * No need to freeze. Supports multi-threaded insertion of records to different positions using add_i.
 * @see add_i
 * @param numCols Number of binary dimensions in the cuboid (bits in key)
 * @param numRows Number of rows in the cuboid
 * @return Unique id for the new cuboid
 */
    unsigned int mkAll(unsigned int numCols, size_t numRows);

    /**
 * creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
   Does not support multi-threading.
   Supports cuboids with maximum of 320 key bits
*/
    unsigned int mk(unsigned int numCols);

    /**
 * appends one record to an appendable (not yet frozen) sparse representation initialized by mk().
   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
   Does not support multi-threading
*/
    void addRowToBaseCuboid(unsigned int s_id, unsigned int n_bits, const Key &key, Value v);


/**
 * Adds record at specified position to a cuboid initialized by mkAll()
 * Thread-safe, as long as there are no conflicts on the parameter i
 * @param i The index at which new record is to be added
 * @param s_id The id of the cuboid (returned by mkAll) to which record is to be added
 * @param key Record key
 * @param v Record value
 */
    void addRowAtPosition(size_t i, unsigned int s_id, const Key &key, Value v);

    /**
 * Finalizes cuboid construction intiatied by mk(). Changes internal representation of cuboid that is compatible with rest of the system
 * Also sets the cuboid size. Only single thread may call it.
 * @param s_id Id of the cuboid (retured by mk()) to be finalized.
 */
    void freezePartial(unsigned int s_id, unsigned int n_bits);

    void freeze(unsigned int s_id) {
        freezePartial(s_id, 0);
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
                         unsigned int id_array[], unsigned int numCuboids);


    /**
 * Writes a batch of cuboids to file
 * @param filename Name of file
 * @param isSparse_array Boolean value indicating whether the cuboid is sparse or dense, one per cuboid, in an array
 * @param ids  Id of cuboid to be written
 * @param numCuboids  Total number of cuboids in the batch, and the size of the arrays.
 */
    void writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[], unsigned int numCuboids);

    /**
 * Retrieves the contents of a dense cuboid as an array.
 * If called on sparse cuboid, converts the sparse cuboid to dense
 * @param id Id of the cuboid
 * @param size Output parameter storing the number of rows in the cuboid
 * @return Pointer to array containing the data of the specified dense cuboid
 */
    Value *fetch(unsigned int id, size_t &numRows);

    /**
 *  Unloads cuboid from RAM
 *  Does not free up the ID !
 *  @param id ID of the cuboid
 */
    void unloadCuboid(unsigned int id) {
        Cuboid c = read(id);
        free(c.ptr);
    }

    /**
 * Aggregates a sparse cuboid to another dense or sparse cuboid according to \p mode.
 * @param s_id Id of the cuboid being aggregated
 * @param bitpos The indexes we want to project to
 * @param mode Indicates type of destination cuboid: 1 for Dense, 2 for Sparse, 3 for either one chosen by minimum size
 * @return Id of the new cuboid. If mode == 3, if the id is negative, then it means the new cuboid is dense.
 */
    signed int srehash(unsigned int s_id, const BitPos &bitpos, short mode);

    /**
     * Aggregates sparse cuboid to another sparse cuboid through sorting
     * Invoked by srehash when first projecting to dense is infeasible
     * @param s_id  id of cuboid being aggregated
     * @param bitpos indexes we want to project to
     * @return Id of the new cuboid. Always sparse
     */
    unsigned int srehash_sorting(const SparseCuboidRow& sourceCuboid, const BitPos &bitpos);

    static std::pair<std::vector<uint64_t>, std::vector<uint8_t>> generateMasks(short keySizeWords, const BitPos &bitpos);

    static inline void projectKeyToKey(uint64_t *srcKey, uint64_t *dstKey,std::vector<uint64_t> & masks,
                                                const std::vector<uint8_t> &bitCountInWord, short keySizeWords)  {
        short sumBits = 0;
        short nextSumBits = 0;
        uint64_t *origDstKey  = dstKey;
        for (int k = 0, k7 = 0; k < keySizeWords; k++, k7 += 7) {
            size_t keyWord = srcKey[k]; //may read extra stuff for the last word, but when ANDed with mask, they should be 0 again
            uint64_t x = keyWord & masks[k7];
            uint64_t mv0 = masks[k7 + 1];
            uint64_t mv1 = masks[k7 + 2];
            uint64_t mv2 = masks[k7 + 3];
            uint64_t mv3 = masks[k7 + 4];
            uint64_t mv4 = masks[k7 + 5];
            uint64_t mv5 = masks[k7 + 6];
            uint64_t t;
            t = x & mv0;
            x = x ^ t | (t >> 1);
            t = x & mv1;
            x = x ^ t | (t >> 2);
            t = x & mv2;
            x = x ^ t | (t >> 4);
            t = x & mv3;
            x = x ^ t | (t >> 8);
            t = x & mv4;
            x = x ^ t | (t >> 16);
            t = x & mv5;
            x = x ^ t | (t >> 32);

            nextSumBits += bitCountInWord[k];
            *dstKey |= (x << sumBits);
            if (nextSumBits >= 64) {
                dstKey++;
                if(sumBits > 0) *dstKey |= (x >> (64 - sumBits));  // >>64 is same as >>0 and we want >>64 to be equal to 0 no matter x
                nextSumBits -= 64;
            }
            sumBits = nextSumBits;
        }


    }

    /**
 * Aggregates a dense cuboid into another dense cuboid.
 * @param d_id Id of the source cuboid
 * @param bitpos Indexes of we want to project to
 * @return Id of new cuboid
 */
    unsigned int drehash(unsigned int d_id, const BitPos &bitpos);


};


#endif //SUDOKUBECBACKEND_ROWSTORE_H
