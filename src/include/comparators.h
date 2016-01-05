#include "cs165_api.h"

// comparing key, value pairs when bulk loading
int bl_compare(const void* a, const void* b);

// basic integer comparison
int int_compare(const void* a, const void* b);

// comparator for sorting commands according to db, table, and column
// so that all jobs on the same column are adjacent once sorted
int select_compare(const void* a, const void* b);