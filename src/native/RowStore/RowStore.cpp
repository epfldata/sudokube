//
// Created by Sachin Basil John on 23.08.22.
//

#include "RowStore.h"
#include <exception>
#include <thread>
#include <chrono>
#include <cassert>

void RowStore::freezePartial(unsigned int s_id, unsigned int n_bits) {
    //SBJ: No other threads. No locks
    Cuboid &c = unsafeRead(s_id);
    std::vector<RowStoreTempRec> *store = (std::vector<RowStoreTempRec> *) c.ptr;
    SparseCuboidRow sparseCuboidRow(nullptr, store->size(), std::max<uint16_t>(n_bits, c.numCols));
    sparseCuboidRow.realloc();

    for (size_t r = 0; r < sparseCuboidRow.numRows; r++) {
        memcpy(sparseCuboidRow.getKey(r), &(*store)[r].key[0], sparseCuboidRow.keySize);
        memcpy(sparseCuboidRow.getVal(r), &((*store)[r].val), sizeof(Value));
    }
    delete store;
    c.ptr = sparseCuboidRow.ptr;
    c.numRows = sparseCuboidRow.numRows;
    c.numCols = sparseCuboidRow.numCols;
    c.isDense = false;
}

void
RowStore::readMultiCuboid(const char *filename, int n_bits_array[], int size_array[], unsigned char isSparse_array[],
                          unsigned int id_array[], unsigned int numCuboids) {
    //printf("readMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "r");
    assert(fp != NULL);
    std::vector<Cuboid> allCuboids;
    for (unsigned int i = 0; i < numCuboids; i++) {
        bool sparse = isSparse_array[i];

        //Note: Conversion from Int to unsigned int
        unsigned int n_bits = n_bits_array[i];
        //Note: Conversion from Int to size_t
        size_t size = size_array[i];


        if (sparse) {
            SparseCuboidRow sparseCuboid(nullptr, size, n_bits);
            sparseCuboid.realloc();
            size_t readElems = fread(sparseCuboid.ptr, sparseCuboid.recSize, sparseCuboid.numRows, fp);
            if (readElems != sparseCuboid.numRows) {
                fprintf(stderr, "File %s  ::  %lu rows read instead of %lu because %s \n", filename, readElems,
                        sparseCuboid.numRows, strerror(errno));
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
    unsigned int firstId = multi_r_add(allCuboids);
    for (int i = 0; i < numCuboids; i++)
        id_array[i] = firstId + i;
}

void
RowStore::writeMultiCuboid(const char *filename, unsigned char isSparse_array[], int ids[], unsigned int numCuboids) {

    printf("writeMultiCuboid(\"%s\", %d)\n", filename, numCuboids);
    FILE *fp = fopen(filename, "wb");
    assert(fp != NULL);
    for (unsigned int i = 0; i < numCuboids; i++) {
        //Note: Conversion from Int to unsigned int
        unsigned int id = ids[i];
        bool sparse = isSparse_array[i];
        Cuboid c = read(id);
        size_t elementSize;
        if (sparse) {
            elementSize = SparseCuboidRow::bitsToBytes(c.numCols) + sizeof(Value);
        } else {
            elementSize = sizeof(Value);
        }
        size_t sw = fwrite(c.ptr, elementSize, c.numRows, fp);
        if (sw != c.numRows) {
            printf("Write Error: %s. Written %lu/%lu rows \n", strerror(errno), sw, c.numRows);
            exit(1);
        }
    }
    fclose(fp);
}


Value *RowStore::fetch(unsigned int id, size_t &numRows) {
    Cuboid c = read(id);
    if (c.isDense) {
        numRows = c.numRows;
        return (Value *) c.ptr;
    } else {
        fprintf(stderr, "WARNING -- Fetch was called on sparse cuboid\n");
        if (c.numCols > 30) throw std::runtime_error("Cannot fetch cuboids of dimensionality greater than 30 as dense");
        SparseCuboidRow sparseCuboid(c);
        DenseCuboid denseCuboid(nullptr, c.numCols);
        denseCuboid.realloc();
        numRows = denseCuboid.numRows;
        Value *array = (Value *) denseCuboid.ptr;
        for (size_t i = 0; i < sparseCuboid.numRows; i++) {
            size_t idx = 0;
            memcpy(&idx, sparseCuboid.getKey(i), sparseCuboid.keySize);
            array[idx] = *sparseCuboid.getVal(i);
        }
        return array;
    }
}

std::pair<std::vector<uint64_t>, std::vector<uint8_t>>
RowStore::generateMasks(short keySizeWords, const BitPos &bitpos) {
    std::vector<uint64_t> masks(keySizeWords * 7, 0);
    std::vector<uint8_t> bitCountInWord(keySizeWords, 0);

    for (auto const b: bitpos) {
        auto w = b >> 6;
        auto o = b & 63;
        bitCountInWord[w] += 1;
        masks[w * 7] |= 1uLL << o; //set the 0th mask for word w
    }

    int w7 = 0;
    for (int w = 0; w < keySizeWords; w++, w7 += 7) {
        uint64_t mk, mp, m;
        m = masks[w7];
        mk = ~m << 1;
        uint8_t oneShiftIminusOne = 1;
        for (int i = 1; i < 7; i++, oneShiftIminusOne <<= 1) {
            mp = mk ^ (mk << 1);
            mp = mp ^ (mp << 2);
            mp = mp ^ (mp << 4);
            mp = mp ^ (mp << 8);
            mp = mp ^ (mp << 16);
            mp = mp ^ (mp << 32);
            masks[w7 + i] = mp & m;
            m = m ^ masks[w7 + i] | (masks[w7 + i] >> oneShiftIminusOne);
            mk = mk & ~mp;
        }
    }
    return std::make_pair(masks, bitCountInWord);
}

unsigned int RowStore::srehash_sorting(SparseCuboidRow &sourceCuboid, const BitPos &bitpos) {

    SparseCuboidRow destinationCuboid(nullptr, 0, bitpos.size());

    short keySizeWords = (sourceCuboid.keySize + 7) >> 3;
    auto [masks, bitCountInWords] = generateMasks(keySizeWords, bitpos);


    //------------ PROJECT TO TEMP STORE -----------------
    SparseCuboidTempRecord tempCuboid(nullptr, sourceCuboid.numRows, sourceCuboid.numCols);
    tempCuboid.realloc();
    for (size_t r = 0; r < sourceCuboid.numRows; r++) {
        auto keyInWords = (uint64_t *) sourceCuboid.getKey(r);
        auto value = *sourceCuboid.getVal(r);
        auto destinationKey = (uint64_t *) tempCuboid.getKey(r);
        projectKeyToKey(keyInWords, destinationKey, masks, bitCountInWords, keySizeWords);
        *tempCuboid.getVal(r) = value;
    }

    const int destKeySizeWords = (destinationCuboid.numCols + 63) >> 6;
    //----------------------- DUPLICATE ELIMINATION BY SORTING ------------------
    std::sort(tempCuboid.begin(), tempCuboid.end(), RowStoreTempRec::temprec_compare_keys);
    size_t w = 0;
    for (size_t r = 0; r < sourceCuboid.numRows; r++) {
        if (w < r) {
            Key kw = tempCuboid.getKey(w);
            Key kr = tempCuboid.getKey(r);
            Value valueR = *tempCuboid.getVal(r);
            Value &valueW = *tempCuboid.getVal(w);
            if (valueR == 0) continue;
            bool cmp = RowStoreTempRec::compare_keys((uint64_t *)kw, (uint64_t *)kr, destKeySizeWords);
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
        }
    }
    if (*tempCuboid.getVal(w) == 0) w--;
    destinationCuboid.numRows = w + 1;
    // --------------------- CONVERT TO SPARSE CUBOID -------------------
    destinationCuboid.realloc();
    for (size_t r = 0; r < destinationCuboid.numRows; r++) {
        memcpy(destinationCuboid.getKey(r),
               tempCuboid.getKey(r), destinationCuboid.keySize);
        memcpy(destinationCuboid.getVal(r),
               tempCuboid.getVal(r), sizeof(Value));
    }
    free(tempCuboid.ptr);
    return registry_add(destinationCuboid);
}

signed int RowStore::srehash(unsigned int s_id, const BitPos &bitpos, short mode) {
    SparseCuboidRow sourceCuboid(read(s_id));
    DenseCuboid destinationDenseCuboid(nullptr, bitpos.size());
    size_t tempSize = sourceCuboid.numRows * sizeof(RowStoreTempRec);
    size_t denseSize = destinationDenseCuboid.numRows * sizeof(Value); //WARNING : denseSize may overflow
    //If projection is not fixed to dense cuboid, we use sorting based rehashing for large sparse cuboids. We don't want to allocate gigantic dense cuboids
    if (mode > 1 && (tempSize < denseSize || destinationDenseCuboid.numCols >= 40)) {
        return srehash_sorting(sourceCuboid, bitpos);
    }

    if (bitpos.size() > 32) {
        throw std::runtime_error(
                "Projection to dense supported for only upto 32 bits");
    }

    short keySizeWords = (sourceCuboid.keySize + 7) >> 3;
    auto [masks, bitCountInWords] = generateMasks(keySizeWords, bitpos);
    //------------------------------ ACTUAL PROJECTION -----------------------------
    //first project to dense and then convert to sparse if necessary
    destinationDenseCuboid.realloc();
    Value *values = (Value *) destinationDenseCuboid.ptr;
    SparseCuboidRow destinationSparseCuboid(nullptr, 0, bitpos.size());
    for (size_t r = 0; r < sourceCuboid.numRows; r++) {
        uint64_t result = 0; //safe because bitpos size < 32 and result will fit in 32 bits
        auto keyInWords = (uint64_t *) sourceCuboid.getKey(r);
        auto value = *sourceCuboid.getVal(r);
        projectKeyToKey(keyInWords, &result, masks, bitCountInWords, keySizeWords);
        if (values[result] == 0 && value > 0) destinationSparseCuboid.numRows++;
        values[result] += value;
    }
    //------------ PROJECTION OVER --------------- //
    bool denseIsCheaper = (destinationSparseCuboid.numRows * destinationSparseCuboid.recSize) >=
                          (destinationDenseCuboid.numRows * sizeof(Value)) * 0.5;  //FIXME
    if (mode == 1) { //SparseToDense
        return registry_add(destinationDenseCuboid);
    } else if (mode == 3 && denseIsCheaper) {
        return -registry_add(destinationDenseCuboid); //negative id to indicate hybrid rehash resulted in dense
    } else {
        //------------ Conversion to sparse ----------------- //
        destinationSparseCuboid.realloc();
        size_t w = 0;

        for (size_t r = 0; r < destinationDenseCuboid.numRows; r++) {
            if (values[r] != 0) {
                memcpy(destinationSparseCuboid.getKey(w), &r,
                       destinationSparseCuboid.keySize);
                memcpy(destinationSparseCuboid.getVal(w), values + r,
                       sizeof(Value));
                w++;
            }
        }
        if (w != destinationSparseCuboid.numRows)
            throw std::runtime_error("Non-zero entries count does not match in dense to sparse conversion");
        free(values);
        return registry_add(destinationSparseCuboid);
    }
}


unsigned int RowStore::drehash(unsigned int d_id, const BitPos &bitpos) {

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



