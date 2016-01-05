/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <pthread.h>

#include "dsl.h"


#define INIT_COL_LEN 4096
#define PAGE 4096

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/
/**
typedef enum DataType {
     INT,
     LONG,
     // Others??
} DataType;
**/


/**
 * IndexType
 * Flag to identify index type. Defines an enum for the different possible column indices.
 * Additonal types are encouraged as extra.
 **/
typedef enum IndexType {
    NONE,
    SORTED,
    B_PLUS_TREE,
} IndexType;

/**
 * column_index
 * Defines a general column_index structure, which can be used as a sorted
 * index or a b+-tree index.
 * - type, the column index type (see enum index_type)
 * - index, a pointer to the index structure. For SORTED, this points to the
 *       start of the sorted array. For B+Tree, this points to the root node.
 *       You will need to cast this from void* to the appropriate type when
 *       working with the index.
 **/
typedef struct column_index {
    IndexType type;
    void* ordering;
} column_index;

/**
 * column
 * Defines a column structure, which is the building block of our column-store.
 * Operations can be performed on one or more columns.
 * - name, the string associated with the column. Column names must be unique
 *       within a table, but columns from different tables can have the same
 *       name.
 * - data, this is the raw data for the column. Operations on the data should
 *       be persistent.
 * - index, this is an [opt] index built on top of the column's data.
 *
 * NOTE: We do not track the column length in the column struct since all
 * columns in a table should share the same length. Instead, this is
 * tracked in the table (length).
 **/
typedef struct column {
    char* name;
    int* data;
    column_index index;
} column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * in clude a size_t table_size).
 * name, the name associated with the table. Table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col, this is the pointer to an array of columns contained in the table.
 * - length, the size of the columns in the table.
 * - nstored, how many values are currently stored
 **/
typedef struct table {
    char* name;
    int col_count;
    int active_cols;
    column** columns;
    int length;
    int nstored;
    int sorted_col;
    pthread_mutex_t rlock;
    int reader_count;
    pthread_mutex_t wlock;
    int loaded;
    int modified;
} table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - table_count: the number of tables in the database.
 * - tables: the pointer to the array of tables contained in the db.
 **/
typedef struct db {
    char* name;
    int table_count;
    int capacity;
    table** tables;
} db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call.
  */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct status {
    StatusCode code;
    char* error_message;
} status;


typedef struct result {
    size_t num_tuples;
    int *payload;
} result;



// variable that the user uses to save returned results
typedef struct var {
    char* name;
    int* vals;
    int len;
    long long* overflow;
    int use_overflow;
    int is_float;
    double decimal;
} var;

// key, value pairs for bulk loading into a B+ tree
typedef struct pair {
    int key;
    void* value;
} pair;

typedef struct db_group {
    db** list;
    int index;
    int capacity;
} db_group;

typedef struct var_group {
    var** list;
    int index;
    int capacity;
    pthread_mutex_t lock;
} var_group;

/**
 * db_operator
 * The db_operator defines a database operation.  Generally, an operation will
 * be applied on column(s) of a table (SELECT, PROJECT, AGGREGATE) while other
 * operations may involve multiple tables (JOINS). The DSLGroup defines
 * the kind of operation.
 *
 * In UNARY operators that only require a single table, only the variables
 * related to table1/column1 will be used.
 * In BINARY operators, the variables related to table2/column2 will be used.
 *
 * If you are operating on more than two tables, you should rely on separating
 * it into multiple operations (e.g., if you have to project on more than 2
 * tables, you should select in one operation, and then create separate
 * operators for each projection).
 *
 * Example query:
 * SELECT a FROM A WHERE A.a < 100;
 * db_operator op1;
 * op1.table1 = A;
 * op1.column1 = b;
 *
 * filter f;
 * f.value = 100;
 * f.type = LESS_THAN;
 * f.mode = NONE;
 *
 * op1.comparator = f;
 **/
typedef struct db_operator {
    // Flag to choose operator
    DSLGroup type;

    // Used for every operator
    db* db1;
    db* db2;
    table* table1;
    column* column1;
    table* table2;
    column* column2;

    // recording insertions
    int *value1;
    
    // for selects
    int min;
    int max;

    // for fetches and hashjoins
    var* var1;
    var* var2;
    var* var3;
    var* var4;
    // for n-tuples
    var** tuples;
    int tuple_count;
    // for loads
    char* contents;
    int contents_len;

    // for tracking var_group for buffered selects
    var_group* group;

    // for updates
    int new_val;

    // variables the user uses to store results
    char* output_name1;
    char* output_name2;

    // for creating B-trees vs. regular sorted indexes
    IndexType index_type;

} db_operator;


typedef struct select_job {
    db* db1;
    table* table1;
    column* column1;
    int* mins;
    int* maxs;
    char** outputs;
    var** positions;
    var_group** groups;
    int num;
    struct select_job* next;
} select_job;



typedef struct select_buf {
    db_operator** buf;
    int index;
    int capacity;
    pthread_mutex_t buf_lock;
} select_buf;



#endif /* CS165_H */

