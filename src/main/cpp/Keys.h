
//#define KEY_BYTES 25
#define KEY_BYTES 100
typedef unsigned char key_type[KEY_BYTES];

/*
// sufficient for 80 dimensions
unsigned KEY_BYTES = 10
typedef unsigned char *key_type;
*/

extern void print_key(int n_bits, unsigned char *key);
extern void project_key(int n_bits, key_type &from, int *mask, key_type &to);
extern void fromLong(unsigned char *key, unsigned long long _lkey);
extern unsigned long long toLong(int n_bits, unsigned char *key);
extern bool compare_keys(key_type &k1, key_type &k2);


