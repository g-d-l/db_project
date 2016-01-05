#include <stdlib.h>
#include "cs165_api.h"



void insert_db(db* ptr);
db* find_db(const char* db_name);

table* find_table(const char* table_name, db* db1);
column* find_col(const char* col_name, table* tbl);

var_group* new_var_group(void);
void insert_res(var* ptr, var_group* g);
var* find_res(const char* res_name, var_group* g);
