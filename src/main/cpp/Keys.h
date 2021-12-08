
//#define KEY_BYTES 25
//#define KEY_BYTES 100
//typedef unsigned char key_type[KEY_BYTES];
#include <cstdint>
#include <inttypes.h>
typedef unsigned char byte;
typedef int64_t value_t;

/*
// sufficient for 80 dimensions
unsigned KEY_BYTES = 10
typedef unsigned char *key_type;
*/

extern void print_key(int n_bits, byte *key);
extern void project_key(unsigned int n_bits, unsigned int toBytes, byte *from, unsigned int *mask, byte *to);
unsigned long long project_key_toLong(unsigned int masklen, byte *from_key, unsigned int *mask);
extern void fromLong(byte *key, unsigned long long _lkey, int numkeybytes);
extern unsigned long long toLong(int n_bits, byte *key);
extern bool compare_keys(const byte *k1, const byte *k2, int numkeybytes);


