//
// Created by Sachin Basil John on 27.08.22.
//

#ifndef SUDOKUBECBACKEND_COLUMNSTORE_H
#define SUDOKUBECBACKEND_COLUMNSTORE_H

#include "common.h"
#include <vector>
#include <mutex>

struct ColumnStore {
    using KeyWord = uint64_t;
    using BitPos = std::vector<unsigned int>;

    struct SparseCuboidCol : Cuboid {
        size_t numRowsInWords;
        uint64_t *keyPtr;

        void realloc() {
            if (!ptr) free(ptr);
            /*
             * assumes sizeof(Value) == sizeof(uint64_t);
             * for every 64 rows, there are 64 words for values and one word per column
             */
            ptr = calloc(numRowsInWords * (64 + numCols), sizeof(uint64_t));
            keyPtr = (uint64_t *) ptr + (numRowsInWords << 6);
        }

        SparseCuboidCol() : Cuboid(), numRowsInWords(0) {}

        SparseCuboidCol(void *p, size_t nr, unsigned int nc) : Cuboid(p, nr, nc, false) {
            numRowsInWords = (nr + 63) >> 6;
            keyPtr = (uint64_t *) p + (numRowsInWords << 6);
        }

        SparseCuboidCol(Cuboid &&that) : SparseCuboidCol(that.ptr, that.numRows, that.numCols) {}

        SparseCuboidCol(Cuboid &that) : SparseCuboidCol(that.ptr, that.numRows, that.numCols) {}


        inline uint64_t *getKey(unsigned int c, size_t w) {
            return keyPtr + c * numRowsInWords + w;
        }

        inline uint64_t *getVal(size_t i) {
            return (uint64_t *) ptr + i;
        }
    };

    std::vector<Cuboid> registry;
    /** for synchronization when multiple java threads try to update this registry simulataneously */
    std::mutex registryMutex;

//Do not call when unfrozen cuboids are present
    inline void clear() {
        std::lock_guard<std::mutex> lock(registryMutex);
        for (auto cub: registry)
            free(cub.ptr);
        registry.clear();
    }

    /**
    Adds a dense or sparse cuboid to the registry
    @param cuboid Cuboid to be added
    @returns unique id for the added cuboid
*/
    inline unsigned int registry_add(const Cuboid &cuboid) {
        std::lock_guard<std::mutex> lock(registryMutex);
        unsigned int id = registry.size();
        registry.push_back(cuboid);
        return id;
    }

    /**
 * Adds several dense or sparse cuboids to the registry in a batch
    @param cuboids Cuboids to be added
    @param id of the first cuboid that was added
 */
    unsigned int multi_r_add(const std::vector<Cuboid> &cuboids) {
        std::lock_guard<std::mutex> lock(registryMutex);
        unsigned int firstId = registry.size();
        registry.insert(registry.end(), cuboids.cbegin(), cuboids.cend());
        return firstId;
    }

    inline Cuboid read(unsigned int id) {
        std::lock_guard<std::mutex> lock(registryMutex);
        return registry[id];
    }

    //Must not be called when multiple threads are accessing
    inline Cuboid &unsafeRead(unsigned int id) {
        return registry[id];
    }

    size_t numRowsInCuboid(unsigned int id) {
        Cuboid c = read(id);
        return c.numRows;
    }

    /**
 * Returns number of bytes of storage used by cuboid with specified id.
 * Valid for only sparse cuboids
 */

    size_t numBytesInSparseCuboid(unsigned int s_id) {
        SparseCuboidCol c(read(s_id));
        return c.numRowsInWords * (c.numCols + 64) * sizeof(uint64_t);
    };


    inline void transpose64(uint64_t A[64]) {
        int j, k;
        uint64_t m, t;
        m = 0x00000000FFFFFFFFuLL;
        for (j = 32; j != 0; j = j >> 1, m = m ^ (m << j)) {
            for (k = 0; k < 64; k = (k + j + 1) & ~j) {
                t = (A[k] ^ (A[k + j] >> j)) & m;
                A[k] = A[k] ^ t;
                A[k + j] = A[k + j] ^ (t << j);
            }
        }
        //this transpose produces reverse of the order we need //TODO: modify the transpose instead of swapping at the end.
        //Therefore: Do mapping i -> (64 - i) while reading/writing
    }

    //------------- WE USE ROWSTORE FROM MK UNTIL FREEZE ---------------------

    /**
* Initialize and pre-allocate new cuboid with a given size. The data is added using add_i method to add specific rows.
* No need to freeze. Supports multi-threaded insertion of records to different positions using add_i.
* @see add_i
* @param numCols Number of binary dimensions in the cuboid (bits in key)
* @param numRows Number of rows in the cuboid
* @return Unique id for the new cuboid
*/
    unsigned int mkAll(unsigned int numCols, size_t numRows) {
        SparseCuboidRow cuboid(nullptr, numRows, numCols);
        cuboid.realloc();
        return registry_add(cuboid);
    }

    /**
 * creates an appendable sparse representation. add to it using add()
   and end adding using freeze()
   as long as this is appendable/a vector, must not call rehash on it.
   Does not support multi-threading.
   Supports cuboids with maximum of 320 key bits
*/
    unsigned int mk(unsigned int numCols) {
        SparseCuboidRow cuboid(nullptr, 0, numCols);
        if (numCols >= (RowStoreTempRec::rowStoreTempKeySizeWords - 1) * 64)
            throw std::runtime_error("Maximum supported bits for mk exceeded");
        cuboid.ptr = new std::vector<RowStoreTempRec>();
        return registry_add(cuboid);
    }

    /**
 * appends several records to an appendable (not yet frozen) sparse representation initialized by mk().
   inconsistent storage type with the rest: a cuboid that we can write to
   is to be a vector that we can append to. It is replaced by the standard
   sparse representation when we freeze it.
   Does not support multi-threading
*/
    void addRowsToCuboid(unsigned int s_id, SparseCuboidRow &rowsToAdd) {
        //SBJ: Currently no other thread should be running. So no locks
        //Adding to std::vector requires static type. So using tempRec
        Cuboid c = unsafeRead(s_id);
        std::vector<RowStoreTempRec> *p = (std::vector<RowStoreTempRec> *) c.ptr;

        for (int i = 0; i < rowsToAdd.numRows; i++) {
            RowStoreTempRec &myrec = p->emplace_back();
            memset(&myrec, 0, sizeof(RowStoreTempRec));
            memcpy(&myrec.key[0], rowsToAdd.getKey(i), rowsToAdd.keySize);
            myrec.val = *rowsToAdd.getVal(i);
            c.numRows++;
        }
    }


/**
 * Adds record at specified position to a cuboid initialized by mkAll()
 * Thread-safe, as long as there are no conflicts on the parameter i
 * @param i The index at which new record is to be added
 * @param s_id The id of the cuboid (returned by mkAll) to which record is to be added
 * @param key Record key
 * @param v Record value
 */
    void addRowsAtPosition(size_t startIdx, unsigned int s_id, SparseCuboidRow &rowsToAdd) {
        //can be multithreaded, but there should be no conflicts
        SparseCuboidRow cuboid(unsafeRead(s_id));
        memcpy(cuboid.getKey(startIdx), rowsToAdd.getKey(0), cuboid.recSize * rowsToAdd.numRows);
    }

    /**
 * Finalizes cuboid construction intiatied by mk(). Changes internal representation of cuboid that is compatible with rest of the system
 * Also sets the cuboid size. Only single thread may call it.
 * @param s_id Id of the cuboid (retured by mk()) to be finalized.
 */
    void freezePartial(unsigned int s_id, unsigned int n_bits);
    void freezeMkAll(unsigned int s_id); //convert row to col store
    void freeze(unsigned int s_id) {
        freezePartial(s_id, 0);
    }

    //-------------------------------- END  ROW STORE----------------------------:

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
    unsigned int srehash_sorting(SparseCuboidCol &sourceCuboid, const BitPos &bitpos);


    /**
 * Aggregates a dense cuboid into another dense cuboid.
 * @param d_id Id of the source cuboid
 * @param bitpos Indexes of we want to project to
 * @return Id of new cuboid
 */
    unsigned int drehash(unsigned int d_id, const BitPos &bitpos);

};


#endif //SUDOKUBECBACKEND_COLUMNSTORE_H
