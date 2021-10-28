
//#define KEY_BYTES 25
//#define KEY_BYTES 100
//typedef unsigned char key_type[KEY_BYTES];

typedef unsigned char byte;
typedef int64_t value_t;

/*
// sufficient for 80 dimensions
unsigned KEY_BYTES = 10
typedef unsigned char *key_type;
*/

extern void print_key(int n_bits, byte *key);
extern void project_key(int n_bits, byte *from, int *mask, byte *to);
extern void fromLong(byte *key, unsigned long long _lkey, int numkeybytes);
extern unsigned long long toLong(int n_bits, byte *key);
extern bool compare_keys(const byte *k1, const byte *k2, int numkeybytes);


