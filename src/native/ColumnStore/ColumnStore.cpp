//
// Created by Sachin Basil John on 27.08.22.
//

#include "ColumnStore.h"
#include <algorithm>

void ColumnStore::freezePartial(unsigned int s_id, unsigned int n_bits) {
    //SBJ: No other threads. No locks
    Cuboid &c = unsafeRead(s_id);
    std::vector<RowStoreTempRec> *store = (std::vector<RowStoreTempRec> *) c.ptr;
    SparseCuboidCol baseCuboid(nullptr, store->size(), std::max<uint16_t>(n_bits, c.numCols));
    baseCuboid.realloc();

    uint64_t array[64];
    for (size_t r = 0; r < baseCuboid.numRows; r += 64) {
        for (size_t r0 = 0; r0 < 64 && r + r0 < baseCuboid.numRows; r0++) {
            *baseCuboid.getVal(r + r0) = (*store)[r + r0].val;
//            printf("Freeze Value[%zu] = %zu -> &zu \n", r + r0, (*store)[r + r0].val, *baseCuboid.getVal(r + r0));
        }
        for (unsigned int c = 0; c < baseCuboid.numCols; c += 64) {
            memset(array, 0, sizeof(array));
            //Array access is mirrored see transpose64
            for (size_t r0 = 0; r0 < 64 && r + r0 < baseCuboid.numRows; r0++) {
                // accessing bits c to c + 64 of key of row r + r0
                // safe to access columns from numCols to next multiple of 64; they just contain garbage
//                printf("Freeze RowKey[%zu][%zu] = %llx", r + r0, c>>6, (*store)[r + r0].key[c >> 6]);
                array[63 - r0] = (*store)[r + r0].key[c >> 6];
            }
            transpose64(array);
            for (unsigned int c0 = 0; c0 < 64 && c + c0 < baseCuboid.numCols; c0++) {
                *baseCuboid.getKey(c + c0, r >> 6) = array[63 - c0];
//                printf("Freeze ColKey[%zu][%zu] = %llx", c + c0, r >> 6, *baseCuboid.getKey(c + c0, r >> 6));
            }
        }
    }

    delete store;
    c.ptr = baseCuboid.ptr;
    c.numRows = baseCuboid.numRows;
    c.numCols = baseCuboid.numCols;
    c.isDense = false;
}

void ColumnStore::freezeMkAll(unsigned int s_id) {
    //SBJ: No other threads. No locks
    Cuboid &c = unsafeRead(s_id);
    SparseCuboidRow rowBaseCuboid(c);
    SparseCuboidCol baseCuboid(nullptr, rowBaseCuboid.numRows, rowBaseCuboid.numCols);
    baseCuboid.realloc();

    uint64_t array[64];
    for (size_t r = 0; r < baseCuboid.numRows; r += 64) {
        for (size_t r0 = 0; r0 < 64 && r + r0 < baseCuboid.numRows; r0++) {
            *baseCuboid.getVal(r + r0) = *rowBaseCuboid.getVal(r + r0);
//            printf("FreezeAll Value[%zu] = %zu -> %zu \n", r + r0, *rowBaseCuboid.getVal(r + r0),
//                   *baseCuboid.getVal(r + r0));
        }
        for (unsigned int c = 0; c < baseCuboid.numCols; c += 64) {
            memset(array, 0, sizeof(array));
            //Array access is mirrored see transpose64
            for (size_t r0 = 0; r0 < 64 && r + r0 < baseCuboid.numRows; r0++) {
                // accessing bits c to c + 64 of key of row r + r0
                // safe to access columns from numCols to next multiple of 64; they just contain garbage
//                printf("FreezeAll RowKey[%zu][%zu] = %llx\n", r + r0, c >> 6,
//                       *((uint64_t *) rowBaseCuboid.getKey(r + r0) + (c >> 6)));
                array[63 - r0] = *((uint64_t *) rowBaseCuboid.getKey(r + r0) + (c >> 6));
            }
            transpose64(array);
            for (unsigned int c0 = 0; c0 < 64 && c + c0 < baseCuboid.numCols; c0++) {
                *baseCuboid.getKey(c + c0, r >> 6) = array[63 - c0];
//                printf("FreezeAll ColKey[%zu][%zu] = %llx\n", c + c0, r >> 6, *baseCuboid.getKey(c + c0, r >> 6));
            }
        }
    }

    free(rowBaseCuboid.ptr);
    c.ptr = baseCuboid.ptr;
    c.numRows = baseCuboid.numRows;
    c.numCols = baseCuboid.numCols;
    c.isDense = false;
}

void
ColumnStore::readMultiCuboid(const char *filename, int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                             unsigned int id_array[], unsigned int numCuboids) {
    //printf("readMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "rb");
    assert(fp != NULL);
    std::vector<Cuboid> allCuboids;
    for (unsigned int i = 0; i < numCuboids; i++) {
        bool sparse = isSparse_array[i];

        //Note: Conversion from Int to unsigned int
        unsigned int n_bits = n_bits_array[i];
        //Note: Conversion from Int to size_t
        size_t size = size_array[i];


        if (sparse) {
            SparseCuboidCol sparseCuboid(nullptr, size, n_bits);
            sparseCuboid.realloc();
            size_t readElems = fread(sparseCuboid.ptr, sizeof(uint64_t) * (sparseCuboid.numCols + 64),
                                     sparseCuboid.numRowsInWords, fp);
            if (readElems != sparseCuboid.numRowsInWords) {
                fprintf(stderr, "File %s  ::  %lu*64 rows read instead of %lu*64 because %s \n", filename, readElems,
                        sparseCuboid.numRowsInWords, strerror(errno));
                throw std::runtime_error(std::string("Error while reading file") + filename);
            }
            allCuboids.push_back(sparseCuboid);
        } else {
            DenseCuboid denseCuboid(nullptr, n_bits);
            denseCuboid.realloc();
            size_t readElems = fread(denseCuboid.ptr, sizeof(Value), denseCuboid.numRows, fp);
            if (readElems != denseCuboid.numRows) {
                fprintf(stderr, "File %s  :: %lu rows read instead of %lu because %s \n", filename, readElems,
                        denseCuboid.numRows,
                        strerror(errno));
                throw std::runtime_error(std::string("Error while reading file") + filename);
            }
            allCuboids.push_back(denseCuboid);
        }
    }
    fclose(fp);
    unsigned int firstId = multi_r_add(allCuboids);
    for (int i = 0; i < numCuboids; i++)
        id_array[i] = firstId + i;
}

void
ColumnStore::writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[],
                              unsigned int numCuboids) {

    printf("writeMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);
    for (unsigned int i = 0; i < numCuboids; i++) {
        //Note: Conversion from Int to unsigned int
        unsigned int id = ids[i];
        bool sparse = isSparse_array[i];
        Cuboid c = read(id);
        if (sparse) {
            SparseCuboidCol sparse(c);
            size_t sw = fwrite(sparse.ptr, sizeof(uint64_t) * (sparse.numCols + 64), sparse.numRowsInWords, fp);
            if (sw != sparse.numRowsInWords) {
                printf("Write Sparse Error: %s. Written %lu*64/%lu*64 rows \n", strerror(errno), sw,
                       sparse.numRowsInWords);
                exit(1);
            }
        } else {
            size_t sw = fwrite(c.ptr, sizeof(Value), c.numRows, fp);
            if (sw != c.numRows) {
                printf("Write Dense Error: %s. Written %lu/%lu rows \n", strerror(errno), sw, c.numRows);
                exit(1);
            }
        }

    }
    fclose(fp);
}


Value *ColumnStore::fetch(unsigned int id, size_t &numRows) {
    Cuboid c = read(id);
    if (c.isDense) {
        numRows = c.numRows;
        return (Value *) c.ptr;
    } else {
        throw std::runtime_error("Fetch was called on sparse cuboid\n");
    }
}


unsigned int ColumnStore::srehash_sorting(SparseCuboidCol &sourceCuboid, const BitPos &bitpos) {



    //------------ PROJECT TO TEMP STORE -----------------
    SparseCuboidTempRecord tempCuboid(nullptr, sourceCuboid.numRows, bitpos.size());
    tempCuboid.realloc();

    uint64_t array[64];
    for (size_t r = 0; r < sourceCuboid.numRows; r += 64) {
        unsigned int c64 = 0;
        for (size_t r0 = 0; r0 < 64 && r + r0 < sourceCuboid.numRows; r0++) {
            *tempCuboid.getVal(r + r0) = *sourceCuboid.getVal(r + r0);
        }
        for (auto c_it = bitpos.cbegin(); c_it < bitpos.cend(); c64 += 1) {  //Group by 64 bits of projected dimensions
            memset(array, 0, sizeof(array));
            //Array access is mirrored, see transpose64
            for (unsigned int c0 = 0; c0 < 64 && c_it < bitpos.cend(); c0++, c_it++) {
                array[63 - c0] = *sourceCuboid.getKey(*c_it, r >> 6);
//                printf("Sorting ColKey[%zu][%zu] = %llx\n", c0, r >> 6, array[63 - c0]);
            }
            transpose64(array);
            for (size_t r0 = 0; r0 < 64 && r + r0 < sourceCuboid.numRows; r0++) {
                *((uint64_t *) tempCuboid.getKey(r + r0) + c64) = array[63 - r0];
//                printf("Sorting RowKey[%zu][%zu] = %llx %llx\n", r >> 6, r0, array[63 - r0], *((uint64_t *) tempCuboid.getKey(r + r0) + c64));
            }
        }
    }

    const int destinationKeySizeWords = (tempCuboid.numCols + 63) >> 6;
    //----------------------- DUPLICATE ELIMINATION BY SORTING ------------------
    std::sort(tempCuboid.begin(), tempCuboid.end(), RowStoreTempRec::temprec_compare_keys);
    size_t w = 0;

    for (size_t r = 0; r < sourceCuboid.numRows; r++) {
        if (w < r) {
            byte *kw = tempCuboid.getKey(w);
            byte *kr = tempCuboid.getKey(r);
            Value valueR = *tempCuboid.getVal(r);
            Value &valueW = *tempCuboid.getVal(w);
//            printf("Merge W<R r= %d w=%d ValueR = %zu ValueW = %zu KeyR= %llx, KewW=%llx\n", r, w, valueR, valueW, *(uint64_t*)kr,
//                   *(uint64_t*)kw);
            if (valueR == 0) continue;
            bool cmp = RowStoreTempRec::compare_keys((uint64_t *) kw, (uint64_t *) kr, destinationKeySizeWords);
            if (!cmp) {
                valueW += valueR;
            } else {
                if (valueW > 0) {
                    w++;
                    kw = (byte *) kw + sizeof(RowStoreTempRec);
                }
                if (w < r) {
                    memcpy(kw, kr, sizeof(RowStoreTempRec)); //copy key and value
                }
            }
        } else {
            byte *kw = tempCuboid.getKey(w);
            byte *kr = tempCuboid.getKey(r);
            Value valueR = *tempCuboid.getVal(r);
            Value &valueW = *tempCuboid.getVal(w);
//            printf("Merge R=W r= %d w=%d ValueR = %zu ValueW = %zu KeyR= %llx, KewW=%llx\n", r, w, valueR, valueW, *(uint64_t*) kr,
//                   *(uint64_t*) kw);
        }
    }
    if (*tempCuboid.getVal(w) == 0) w--;
    w++;
    // --------------------- CONVERT TO SPARSE CUBOID -------------------
    SparseCuboidCol destinationCuboid(nullptr, w, bitpos.size());
    destinationCuboid.realloc();
    for (size_t r = 0; r < destinationCuboid.numRows; r += 64) {
        for (size_t r0 = 0; r0 < 64 && r0 + r < destinationCuboid.numRows; r0++) {
            *destinationCuboid.getVal(r + r0) = *tempCuboid.getVal(r + r0);
        }
        for (unsigned int c = 0; c < tempCuboid.numCols; c += 64) {
            memset(array, 0, sizeof(array));
            for (size_t r0 = 0; r0 < 64 && r + r0 <
                                           destinationCuboid.numRows; r0++) { //number of unique rows in tempCuboid is stored in destinationCuboid.numRows
                memcpy(array + (63 - r0), ((uint64_t *) tempCuboid.getKey(r + r0)) + (c >> 6), sizeof(uint64_t));
//                printf("Sorting toCol RowKey[%zu] = %llx\n", r + r0, array[63-r0]);
            }
            transpose64(array);
            for (unsigned int c0 = 0; c0 < 64 && c + c0 < destinationCuboid.numCols; c0++) {
//                printf("Sorting toCol ColKey[%zu][%zu] = %llx\n", c0, r >> 6, array[63-c0]);
                memcpy(destinationCuboid.getKey(c + c0, r >> 6), array + (63 - c0), sizeof(uint64_t));
            }
        }
    }
    free(tempCuboid.ptr);
    return registry_add(destinationCuboid);
}

signed int ColumnStore::srehash(unsigned int s_id, const BitPos &bitpos, short mode) {

    SparseCuboidCol sourceCuboid(read(s_id));
    DenseCuboid destinationDenseCuboid(nullptr, bitpos.size());
    size_t tempSize = sourceCuboid.numRows * sizeof(RowStoreTempRec);
    size_t denseSize = destinationDenseCuboid.numRows * sizeof(Value); //WARNING : denseSize may overflow

    bool debug = false;

    //If projection is not fixed to dense cuboid, we use sorting based rehashing for large sparse cuboids. We don't want to allocate gigantic dense cuboids
    if (mode > 1 && (tempSize < denseSize || destinationDenseCuboid.numCols >= 40)) {
        if (debug) printf("Sorting based srehash \n");
        return srehash_sorting(sourceCuboid, bitpos);
    }


    if (debug) {
        printf("Dense based srehash bitpos = ");
        for (auto c: bitpos) { printf(" %d ", c); }
    }
    if (bitpos.size() > 32) {
        throw std::runtime_error("Projection to dense supported for only upto 32 bits");
    }
    uint64_t array[64];
    //------------------------------ ACTUAL PROJECTION -----------------------------
    //first project to dense and then convert to sparse if necessary
    destinationDenseCuboid.realloc();
    Value *values = (Value *) destinationDenseCuboid.ptr;
    size_t uniqueCount = 0;
    for (size_t rw = 0; rw < sourceCuboid.numRowsInWords; rw++) {
        memset(array, 0, sizeof(array));
        unsigned int c0 = 0;
        for (auto c: bitpos) { //assuming bitpos.size < 64
            memcpy(array + (63 - c0), sourceCuboid.getKey(c, rw), sizeof(uint64_t));
            if (debug)
                printf("s_id = %d s2d OrigColkey[%u][%zu] =  %llx \n", s_id, c, rw, *sourceCuboid.getKey(c, rw));
            c0++;
        }
        transpose64(array);
        //accessing rw*64 to (rw+1)*64 bits. Safe to access, but contains garbage
        for (size_t r0 = 0; r0 < 64; r0++) {
            uint64_t result = array[63 - r0]; //safe because bitpos size < 32 and result will fit in 32 bits
            if (debug) printf("s_id = %d s2d ProjectedRowKey[%zu][%zu] =  %llx \n", s_id, rw, r0, result);
            Value value = *sourceCuboid.getVal((rw << 6) + r0);
            if (values[result] == 0 && value > 0) uniqueCount++;
            values[result] += value;
        }
    }
    //------------ PROJECTION OVER --------------- //
    SparseCuboidCol destinationSparseCuboid(nullptr, uniqueCount, bitpos.size());
    bool denseIsCheaper =
            (destinationSparseCuboid.numRowsInWords * sizeof(uint64_t) * (destinationSparseCuboid.numCols + 64)) >=
            (destinationDenseCuboid.numRows * sizeof(Value)) * 0.5;  //FIXME
    if (mode == 1) { //SparseToDense
        return registry_add(destinationDenseCuboid);
    } else if (mode == 3 && denseIsCheaper) {
        return -registry_add(destinationDenseCuboid); //negative id to indicate hybrid rehash resulted in dense
    } else {
        //------------ Conversion to sparse ----------------- //

        destinationSparseCuboid.realloc();
        size_t r0 = 0, r = 0;
        memset(array, 0, sizeof(array));
        for (size_t i = 0; i < destinationDenseCuboid.numRows; i++) {
            if (values[i] != 0) {
                memcpy(destinationSparseCuboid.getVal(r), values + i, sizeof(Value));
                array[63 - r0] = i;
                if (debug) printf("s_id = %d d2s ProjRowKey[%zu] = %zx \n", s_id, r, i);
                r0++;
                if (r0 == 64 || r == destinationSparseCuboid.numRows - 1) {
                    transpose64(array);
                    r0 = 0;
                    for (unsigned int c0 = 0; c0 < destinationSparseCuboid.numCols; c0++) { // numCols < 64
                        memcpy(destinationSparseCuboid.getKey(c0, r >> 6), array + (63 - c0), sizeof(uint64_t));
                        if (debug)
                            printf("s_id = %d d2s ProjColKey[%u][%zu] = %llx \n", s_id, c0, r >> 6,
                                   *destinationSparseCuboid.getKey(c0, r >> 6));
                    }
                }
                r++;
            }
        }
        free(destinationDenseCuboid.ptr);
        return registry_add(destinationSparseCuboid);
    }
}


unsigned int ColumnStore::drehash(unsigned int d_id, const BitPos &bitpos) {

    DenseCuboid sourceCuboid(read(d_id));
    DenseCuboid destinationCuboid(nullptr, bitpos.size());
    destinationCuboid.realloc();
    Value *sourceArray = (Value *) sourceCuboid.ptr;
    Value *destArray = (Value *) destinationCuboid.ptr;
    //-------------PREPARE MASKS----------
    uint64_t m = 0, mv0, mv1, mv2, mv3, mv4, mv5;
    for (auto const b: bitpos) {
        auto w = b >> 6;
        auto o = b & 63;
        m |= 1uLL << o; //set the 0th mask for word w
    }

    uint64_t mk, mp, morig = m; //m gets changed here but we want the original m later
    mk = ~m << 1;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv0 = mp & m;
    m = m ^ mv0 | (mv0 >> 1);
    mk = mk & ~mp;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv1 = mp & m;
    m = m ^ mv1 | (mv1 >> 2);
    mk = mk & ~mp;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv2 = mp & m;
    m = m ^ mv2 | (mv2 >> 4);
    mk = mk & ~mp;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv3 = mp & m;
    m = m ^ mv3 | (mv3 >> 8);
    mk = mk & ~mp;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv4 = mp & m;
    m = m ^ mv4 | (mv4 >> 16);
    mk = mk & ~mp;

    mp = mk ^ (mk << 1);
    mp = mp ^ (mp << 2);
    mp = mp ^ (mp << 4);
    mp = mp ^ (mp << 8);
    mp = mp ^ (mp << 16);
    mp = mp ^ (mp << 32);
    mv5 = mp & m;
    m = m ^ mv5 | (mv5 >> 32);
    mk = mk & ~mp;

    //-----PROJECTION BEGINS HERE-----
    for (size_t i = 0; i < sourceCuboid.numRows; i++) {
        uint64_t x = i & morig;
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
        destArray[x] += sourceArray[i];
    }

    return registry_add(destinationCuboid);
}