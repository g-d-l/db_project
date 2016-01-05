#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "db.h"
#include "global_lists.h"
#include "select_buf.h"
#include "save_data.h"

#define INIT_LIST_SZ 32

extern db_group* db_list;

void insert_db(db* ptr) {
    // out of space, so double length
    if (db_list->index == db_list->capacity) {
        db_list->capacity *= 2;
        db_list->list = realloc(db_list->list, db_list->capacity * sizeof(db*));
    }
    db_list->list[db_list->index++] = ptr;
}

db* find_db(const char* db_name) {
    for (int i = 0; i < db_list->index; i++) {
        if (strcmp(db_name, db_list->list[i]->name) == 0)
            return db_list->list[i];
    }
    return NULL;
}


table* find_table(const char* table_name, db* db1) {
    for (int i = 0; i < db1->table_count; i++) {
        if (strcmp(table_name, db1->tables[i]->name) == 0) {
            if (db1->tables[i]->loaded == 0)
                load_table_data(db1, db1->tables[i]);
            return db1->tables[i];
        }
    }
    return NULL;
}


column* find_col(const char* col_name, table* tbl) {
    for (int i = 0; i < tbl->col_count; i++) {
        if (strcmp(col_name, tbl->columns[i]->name) == 0)
            return tbl->columns[i];
    }
    return NULL;
}

var_group* new_var_group(void) {
    var_group* new = malloc(sizeof(var_group));
    new->list = malloc(sizeof(var*) * INIT_LIST_SZ);
    new->index = 0;
    new->capacity = INIT_LIST_SZ;
    pthread_mutex_init(&new->lock, NULL);
    return new;
}

void insert_res(var* ptr, var_group* g) {
    pthread_mutex_lock(&g->lock);
    // uninitialized list
    if (g->index == g->capacity) {
        g->capacity *= 2;
        g->list = realloc(g->list, g->capacity * sizeof(var*));
    }
    g->list[g->index++] = ptr;
    pthread_mutex_unlock(&g->lock);
}

var* find_res(const char* res_name, var_group* g) {
    for (int i = 0; i < g->index; i++) {
        if (strcmp(res_name, g->list[i]->name) == 0)
            return g->list[i];
    }
    // not found, so flush
    flush_selects();
    return find_res(res_name, g);
}
