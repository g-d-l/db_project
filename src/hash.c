#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cs165_api.h"
#include "hash.h"

// Hashing via the formula here:
// http://isthe.com/chongo/tech/comp/fnv
unsigned fnv1_hash(unsigned val) {
    unsigned prime = PRIME;
    unsigned hash = HASH;
    for (int i = 0; i < 4; i++) {
        unsigned octet = val & ((1 << 8) - 1);
        hash *= prime;
        hash ^= octet;
        val = val >> 8;
    }
    return hash;
}


int min2(int a, int b) {
    return (a < b) ? a : b;
}

// from http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
// round n to the next power of 2 equal to or greater than n
unsigned next_2_power(unsigned n) {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    return n;
}

void htable_insert(bucket* htable, unsigned hash, int val, int pos) {
    if (htable[hash].next == (bucket*) 1) {
        htable[hash].next = NULL;
        htable[hash].val = val;
        htable[hash].pos = pos;
    }
    else {
        bucket* curr = &htable[hash];
        while (curr->next != NULL)
            curr = curr->next;
        bucket* new_bucket = malloc(sizeof(bucket));
        curr->next = new_bucket;
        new_bucket->val = val;
        new_bucket->pos = pos;
        new_bucket->next = NULL;
    }
}

// for freeing chains of buckets
void recursive_free(bucket* b) {
    if (b == NULL)
        return;
    recursive_free(b->next);
    free(b);

}


// hashjoin using FNV-1 hashing and a hash table size the size of the smaller input, 
// rounded up to the nearest power of 2
// chaining is used to handled collisions
void hash_join(var* val1, var* pos1, var* val2, var* pos2, var* r1, var* r2) {
    int out_len = min2(val1->len, val2->len);

    if (out_len < 2 * 4096) {
        block_join(val1, pos1, val2, pos2, r1, r2);
        return;
    }

    r1->vals = malloc(sizeof(int) * out_len);
    r1->len = 0;
    
    r2->vals = malloc(sizeof(int) * out_len);
    r2->len = 0;


    var* smaller_val;
    var* smaller_pos;
    var* larger_val;
    var* larger_pos;
    if (out_len == val1->len) {
        smaller_val = val1;
        smaller_pos = pos1;
        larger_val = val2;
        larger_pos = pos2;
    }
    else {
        smaller_val = val2;
        smaller_pos = pos2;
        larger_val = val1;
        larger_pos = pos1;
        var* temp = r2;
        r2 = r1;
        r1 = temp;
    }

    int htable_len = next_2_power(smaller_val->len);
    unsigned bucket_mask = htable_len - 1;

    // make and fill hash table
    bucket* htable = malloc(sizeof(bucket) * htable_len);
    for (int i = 0; i < htable_len; i++) {
        htable[i].next = (bucket*) 1;
    }
    for (int i = 0; i < smaller_val->len; i++) {
        unsigned bin = fnv1_hash(smaller_val->vals[i]) & bucket_mask;
        htable_insert(htable, bin, smaller_val->vals[i], smaller_pos->vals[i]);
    }

    // probing
    for (int i = 0; i < larger_val->len; i++) {
        unsigned slot = fnv1_hash(larger_val->vals[i]) & bucket_mask;
        if (htable[slot].next == (bucket*) 1) {
            continue;
        }
        else {
            bucket* curr = &htable[slot];
            do {
                if (curr->val == larger_val->vals[i]) {
                    if (r1->len == out_len) {
                        out_len *= 2;
                        r1->vals = realloc(r1->vals, sizeof(int) * out_len);
                        r2->vals = realloc(r2->vals, sizeof(int) * out_len);
                    }
                    r1->vals[r1->len++] = curr->pos;
                    r2->vals[r2->len++] = larger_pos->vals[i];
                }
                curr = curr->next;
            } while (curr);
        }
    }

    for (int i = 0; i < smaller_val->len; i++) {
        if (htable[i].next == (bucket*) 1)
            continue;
        recursive_free(htable[i].next);
    }
    free(htable);
}

// Block joining optimized around the page size
void block_join(var* val1, var* pos1, var* val2, var* pos2, var* r1, var* r2) {
    int out_len = min2(val1->len, val2->len);

    r1->vals = malloc(sizeof(int) * out_len);
    r1->len = 0;
    r1->use_overflow = 0;
    r1->is_float = 0;
    
    r2->vals = malloc(sizeof(int) * out_len);
    r2->len = 0;
    r2->use_overflow = 0;
    r2->is_float = 0;

    var* smaller_val;
    var* smaller_pos;
    var* larger_val;
    var* larger_pos;
    if (out_len == val1->len) {
        smaller_val = val1;
        smaller_pos = pos1;
        larger_val = val2;
        larger_pos = pos2;
    }
    else {
        smaller_val = val2;
        smaller_pos = pos2;
        larger_val = val1;
        larger_pos = pos1;
        var* temp = r2;
        r2 = r1;
        r1 = temp;
    }

    for (int i = 0; i < smaller_val->len; i += PAGE) {
        for (int k = 0; k < larger_val->len; k++) {
            int block_size = min2(PAGE, smaller_val->len - i);
            for (int j = i; j < i + block_size; j++) {
                if (smaller_val->vals[j] == larger_val->vals[k]) {
                    if (r1->len == out_len) {
                        out_len *= 2;
                        r1->vals = realloc(r1->vals, sizeof(int) * out_len);
                        r2->vals = realloc(r2->vals, sizeof(int) * out_len);
                    }
                    r1->vals[r1->len++] = smaller_pos->vals[j];
                    r2->vals[r2->len++] = larger_pos->vals[k];
                }
            }
        }
    }
}
