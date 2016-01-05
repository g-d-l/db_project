#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cs165_api.h"

extern db_group* db_list;


// return number of files in a directory, used to count number of columns in a table
int num_files(const char* path);

// load database metadata
void load_dbs(void);

// load the given table's metadata and actual data into db1
void load_table_data(db* db1, table* tbl1);

// save modified tables to disk
void flush_data(void);
