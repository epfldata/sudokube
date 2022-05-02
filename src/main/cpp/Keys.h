
//#define KEY_BYTES 25
//#define KEY_BYTES 100
//typedef unsigned char key_type[KEY_BYTES];
#include <cstdint>
#include <inttypes.h>
#include <cstring>
typedef unsigned char byte;
typedef int64_t value_t;  //we have Java Long as value

/**
 * Returns the pointer to key at some idx in a dynamic sized array of records
 * @param array Array representing sparse cuboid
 * @param idx Index of record whose key is to be fetched
 * @param recSize Size of the dynamic typed record (in bytes)
 * @returns pointer to least significant byte of key
 */

inline byte *getKey(byte **array, size_t idx, size_t recSize) {
    byte *key = (byte *) array + recSize * idx;
//    printf("array = %d idx = %d recSize = %d key = %d\n", array, idx, recSize, key);
    return key;
}

/**
 * Returns the pointer to value at some idx in a dynamic sized array of records
 * @param array Array representing sparse cuboid
 * @param idx Index of record whose key is to be fetched
 * @param recSize Size of the dynamic typed record (in bytes)
 * @returns pointer to value
 */
inline value_t *getVal(byte **array, size_t idx, size_t recSize) {
    value_t *val = (value_t *) ((byte *) array + (idx + 1) * recSize - sizeof(value_t));
//    printf("array = %d idx = %d recSize = %d val = %d\n", array, idx, recSize, val);
    return val;
}

/**
 * Converts number of bits to number of bytes
 */
inline unsigned int bitsToBytes(unsigned int bits) {
    return (bits + 7) >> 3;
}

inline void print_key(int n_bits, byte *key) {
    for(int pos = n_bits - 1; pos >= 0; pos--) {
        int b = (key[pos / 8] >> (pos % 8)) % 2;
        if(b) printf("1");
        else  printf("0");
        if(pos % 8 == 0)
            printf(" ");
    }
}

/**
 * Compare two dynamically typed keys of the same size
 * @param numkeybytes Maximum number of bytes to be compared
 * @return true if k1 < k2 , else false
 */
inline  bool compare_keys(const byte *k1, const byte *k2, int numkeybytes) {
    //Warning : Do not use unsigned here. i>= 0 always true
    for(int i = numkeybytes - 1; i >= 0; i--) {
        if(k1[i] < k2[i]) return true;
        if(k1[i] > k2[i]) return false;
    }
    return false;
}

/**
 * Projects a dynamically sized key  using the mask into a long value that represents index in a dense representation of cuboid.
 * @param masksum Number of 1s in mask
 * @param maskpos Indexes of mask where bit is 1
 * @param from_key pointer to the source key which is to be projected
 * @return the projected key, as a Long value
 */
inline size_t project_Key_to_Long(unsigned int masksum, unsigned int *maskpos, byte *from_key) {
    size_t to = 0;
    //start from lsb of destination key
    for(int wpos = 0; wpos < masksum; wpos++) {
        //get index of source key from mask
        int rpos = maskpos[wpos];

        //we read (rpos % 8)^th bit of (rpos / 8)^th byte. If this bit is 1, then bit at  wpos is set to 1 in destination key
        unsigned int bit = 1 << (rpos & 0x7);
        if (from_key[rpos >> 3] & bit)
            to |= 1 << wpos;

    }
    return to;
}

/**
 * Projects a long value repsenting index in dense cuboid to a sparse key
 * @param masksum Number of 1s in mask
 * @param maskpos Indexes of mask where bit is 1
 * @param from_key Long value storing source key
 * @param to Pointer to destination key
 */
inline void project_Long_to_Key(unsigned int masksum, unsigned int *maskpos,  size_t from_key, byte *to) {
    unsigned int numkeybytes = bitsToBytes(masksum);
    //set all bits to 0
    memset(to, 0, numkeybytes);
    //Start at lsb of destination key
    for(int wpos = 0; wpos < masksum; wpos++) {
        //get index of source key from mask
        int rpos = maskpos[wpos];
        size_t bit = (1L << rpos);
        //if bit at rpos is set, then set the (wpos % 8)^th bit of the (wpos / 8)^th byte of destination
        if(from_key & bit)
            to[wpos >> 3] |= 1 << (wpos & 0x7);
    }
}

/**
 * Converts 64-bit dense key to dynamic sized sparse key
 * Equivalent to project_Long_to_Key with a mask of all 1s
 * @param numkeybytes Number of bytes of destination key
 * @param from Source key
 * @param to Pointer to destination key
 */
inline void from_Long_to_Key(unsigned int numkeybytes, size_t from, byte* to) {
    //set all bits to 0
    memset(to, 0, numkeybytes);
    unsigned int pos = 0;
    //copy byte by byte from source to destination
    while(from > 0) {
        to[pos] = from & 0xff;
        from >>= 8;
        pos++;
    }
    assert(pos <= numkeybytes);
}

/**
 * Projects a dynamically sized sparse key to another sparse key using a mask
 * @param masksum Number of 1s in the mask
 * @param maskpos Indexes of mask where bit is 1
 * @param from Pointer to source key
 * @param to Pointer to destination key
 */
inline void project_Key_to_Key(unsigned int masksum, unsigned int *maskpos,  byte *from, byte *to) {
    unsigned int numkeybytes = bitsToBytes(masksum);
    //set all bits to zero
    memset(to, 0, numkeybytes);
    //start at lsb of destination
    for(int wpos = 0; wpos < masksum; wpos++) {
        //get index of source from mask
        int rpos = maskpos[wpos];
        //read (rpos%8)^th bit of the (rpos/8)^th byte and if it is 1, set the (wpos % 8)^th bit of the (wpos / 8)^th byte
        unsigned int bit = 1 << (rpos & 0x7);
        if(from[rpos >> 3] & bit)
            to[wpos >> 3] |= 1 << (wpos & 0x7);
    }
}

/**
 * Projects a 64-bit dense key to another 64-bit dense key using a  mask
 * @param masksum Number of 1s in the mask
 * @param maskpos Indexes of mask where bit is 1
 * @param from Source key
 * @return Destination key
 */
inline size_t project_Long_to_Long(unsigned int masksum, unsigned int *maskpos, size_t from) {
    size_t to = 0;
    //start at lsb of destination
    for(int wpos = 0; wpos < masksum; wpos++) {
        //get index of source from mask
        int rpos = maskpos[wpos];
        size_t bit = 1L << rpos;
        //read rpos^th bit from source and if 1, set the wpos^th bit of destination
        if (from & bit)
            to |= 1 << wpos;

    }
    return to;
}



