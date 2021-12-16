
//#define KEY_BYTES 25
//#define KEY_BYTES 100
//typedef unsigned char key_type[KEY_BYTES];
#include <cstdint>
#include <inttypes.h>
#include <cstring>
typedef unsigned char byte;
typedef int64_t value_t;

/*
// sufficient for 80 dimensions
unsigned KEY_BYTES = 10
typedef unsigned char *key_type;
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


inline void print_key(int n_bits, byte *key) {
    for(int pos = n_bits - 1; pos >= 0; pos--) {
        int b = (key[pos / 8] >> (pos % 8)) % 2;
        if(b) printf("1");
        else  printf("0");
        if(pos % 8 == 0)
            printf(" ");
    }
}

inline  bool compare_keys(const byte *k1, const byte *k2, int numkeybytes) {
    //Warning : Do not use unsigned here. i>= 0 always true
    for(int i = numkeybytes - 1; i >= 0; i--) {
        if(k1[i] < k2[i]) return true;
        if(k1[i] > k2[i]) return false;
    }
    return false;
}

inline size_t project_Key_to_Long(unsigned int masksum, unsigned int *maskpos, byte *from_key) {
    size_t to = 0;
    for(int wpos = 0; wpos < masksum; wpos++) {
        int rpos = maskpos[wpos];
        unsigned int bit = 1 << (rpos & 0x7);
        if (from_key[rpos >> 3] & bit)
            to |= 1 << wpos;

    }
    return to;
}

inline void project_Long_to_Key(unsigned int masksum, unsigned int *maskpos,  size_t from_key, byte *to) {
    unsigned int numkeybytes = bitsToBytes(masksum);
    memset(to, 0, numkeybytes);
    for(int wpos = 0; wpos < masksum; wpos++) {
        int rpos = maskpos[wpos];
        size_t bit = (1L << rpos);
        if(from_key & bit)
            to[wpos >> 3] |= 1 << (wpos & 0x7);
    }
}

//with mask all 1
inline void from_Long_to_Key(unsigned int numkeybytes, size_t from, byte* to) {
    memset(to, 0, numkeybytes);
    unsigned int pos = 0;
    while(from > 0) {
        to[pos] = from & 0xff;
        from >>= 8;
        pos++;
    }
    assert(pos <= numkeybytes);
}
inline void project_Key_to_Key(unsigned int masksum, unsigned int *maskpos,  byte *from, byte *to) {
    unsigned int numkeybytes = bitsToBytes(masksum);
    memset(to, 0, numkeybytes);
    for(int wpos = 0; wpos < masksum; wpos++) {
        int rpos = maskpos[wpos];
        unsigned int bit = 1 << (rpos & 0x7);
        if(from[rpos >> 3] & bit)
            to[wpos >> 3] |= 1 << (wpos & 0x7);
    }
}


inline size_t project_Long_to_Long(unsigned int masksum, unsigned int *maskpos, size_t from) {
    size_t to = 0;
    for(int wpos = 0; wpos < masksum; wpos++) {
        int rpos = maskpos[wpos];
        size_t bit = 1L << rpos;
        if (from & bit)
            to |= 1 << wpos;

    }
    return to;
}



