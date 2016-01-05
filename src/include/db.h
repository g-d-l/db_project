#ifndef DB_H__
#define DB_H__

#include "cs165_api.h"

#define INIT_COL_LEN 4096

db* create_db(const char* name);

table* create_table(const char* name, db* db1, int col_count);

void create_column(const char* name, table* tbl1, int is_sorted);

void create_index(table* tbl1, column* col1, IndexType type);

#endif // DB_H__
