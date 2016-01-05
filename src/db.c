#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "db.h"
#include "cs165_api.h"
#include "global_lists.h"
#include "rwlocks.h"
#include "comparators.h"
#include "bptree.h"




db* create_db(const char* name) {
    db* db1 = malloc(sizeof(db));
    int len = strlen(name);
    char* name_cpy = malloc(len + 1);
    memcpy(name_cpy, name, len + 1);
    db1->name = name_cpy;
    db1->table_count = 0;
    db1->capacity = 10;
    db1->tables = malloc(sizeof(table*) * 10);
    insert_db(db1);
    return db1;
}


table* create_table(const char* name, db* db1, int col_count) {
    if (db1->table_count == db1->capacity) {
        db1->tables = realloc(db1->tables, 2 * db1->capacity * sizeof(table*));
        db1->capacity *= 2;
    }
    int i = db1->table_count;
    db1->table_count++;
    db1->tables[i] = malloc(sizeof(table));

    int len = strlen(name);
    char* name_cpy = malloc(len + 1);
    memcpy(name_cpy, name, len + 1);
    db1->tables[i]->name = name_cpy;
    db1->tables[i]->col_count = col_count;
    db1->tables[i]->active_cols = 0;
    db1->tables[i]->columns = malloc(sizeof(column*) * col_count);
    db1->tables[i]->length = INIT_COL_LEN;
    db1->tables[i]->nstored = 0;
    db1->tables[i]->sorted_col = -1;
    pthread_mutex_init(&db1->tables[i]->rlock, NULL);
    db1->tables[i]->reader_count = 0;
    pthread_mutex_init(&db1->tables[i]->wlock, NULL);
    db1->tables[i]->loaded = 1;
    db1->tables[i]->modified = 1;
    return db1->tables[i];
}



void create_column(const char* name, table* tbl1, int is_sorted) {
    int i = tbl1->active_cols;
    tbl1->active_cols++;
    tbl1->columns[i] = malloc(sizeof(column));

    int len = strlen(name);
    char* name_cpy = malloc(len + 1);
    memcpy(name_cpy, name, len + 1);

    tbl1->columns[i]->name = name_cpy;
    tbl1->columns[i]->data = malloc(sizeof(int) * tbl1->length);
    tbl1->columns[i]->index.type = NONE;
    tbl1->columns[i]->index.ordering = NULL;
    
    if (is_sorted)
        tbl1->sorted_col = i;
}

void create_index(table* tbl1, column* col1, IndexType type) {
    if (type == SORTED) {
        free(col1->index.ordering);
        int* data = malloc(sizeof(int) * tbl1->length);
        for (int i = 0; i < tbl1->nstored; i++) {
            data[i] = col1->data[i];
        }
        qsort(data, tbl1->nstored, sizeof(int), int_compare);
        col1->index.type = type;
        col1->index.ordering = data;
    }
    else if (type == B_PLUS_TREE) {
        destroy_tree(col1->index.ordering);
        pair* data = malloc(sizeof(pair) * tbl1->nstored);
        for (int i = 0; i < tbl1->nstored; i++) {
            data[i].key = col1->data[i];
            data[i].value = (void*) i;
        }
        col1->index.type = type;
        col1->index.ordering = (void*) bulk_load(data, tbl1->nstored, bl_compare);
    }
}
