#include "cs165_api.h"
#include "comparators.h"

// compare a struct containing a key and value by the keys
int bl_compare(const void* a, const void* b) {
	if (((pair*) a)->key < ((pair*) b)->key)
		return -1;
	else
		return 1;
}

// integer comparison
int int_compare(const void* a, const void* b) {
    return (*(int*) a < *(int*) b) ? -1 : 1;
}

// for sorting by db, then table, then column
int select_compare(const void* a, const void* b) {
	const db_operator* a1 = *((db_operator**) a);
	const db_operator* b1 = *((db_operator**) b);
	if (a1->db1 < b1->db1)
		return -1;
	else if (a1->db1 > b1->db1)
		return 1;
	else if (a1->table1 < b1->table1)
		return -1;
	else if (a1->table1 > b1->table1)
		return 1;
	else if (a1->column1 < b1->column1)
		return -1;
	else if (a1->column1 > b1->column1)
		return 1;
	else
		return 0;
}