#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cs165_api.h"


#define PRIME 16777619
#define HASH 2166136261


// next is 1 if empty or NULL is present and the last in the chain,
// otherwise it points to the next in the chain.
typedef struct bucket {
    long long val;
    int pos;
    struct bucket* next;
} bucket;


// http://isthe.com/chongo/tech/comp/fnv
unsigned fnv1_hash(unsigned val);

int min2(int a, int b);

// from http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
// round n to the next power of 2 equal to or greater than n
unsigned next_2_power(unsigned n);

void htable_insert(bucket* htable, unsigned hash, int val, int pos);

void recursive_free(bucket* b);

// hashjoin using FNV-1 hashing and a hash table size the size of the smaller input, 
// rounded up to the nearest power of 2
// chaining is used to handled collisions
void hash_join(var* val1, var* pos1, var* val2, var* pos2, var* r1, var* r2);

void block_join(var* val1, var* pos1, var* val2, var* pos2, var* r1, var* r2);
