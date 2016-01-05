// dsl.h
//
// CS165, Fall 2015
//
// This file defines some of the regular expressions to match the incoming DSL.
// For now, we implement regular expressions to match the create functions for
// you, and you are free to update this for all the other DSL commands.

#ifndef DSL_H__
#define DSL_H__

#include <stdlib.h>

#define NUM_DSL_COMMANDS (21)

// This helps group similar DSL commands together.
// For example, some queries can be parsed together:
//
// create(col, <col_name>, <tbl_name>, unsorted)
// create(col, <col_name>, <tbl_name>, sorted)
//
// and
//
// p = select(col1, 5, 5) (point query)
// p = select(col1, 5, 10) (range query)
typedef enum DSLGroup {
    CREATE_DB,
    CREATE_TABLE,
    CREATE_COLUMN,
    CREATE_INDEX,
    LOAD,
    REL_INSERT,
    SELECT,
    SELECT_POS,
    FETCH,
    DELETE,
    HASH_JOIN,
    MIN_VAL,
    MIN_POS_VAL,
    MAX_VAL,
    MAX_POS_VAL,
    AVG_VAL,
    ADD,
    SUB,
    UPDATE,
    TUPLE,
    SHUTDOWN,
    DONE,
    SELECT_FLUSH
} DSLGroup;


// A dsl is defined as the DSL listed on the project website.
// We use this to track the relevant string to parse, and its group.
typedef struct dsl {
    const char* c;
    DSLGroup g;
} dsl;

// This returns an array of all the DSL commands that you can match with.
dsl** dsl_commands_init(void);

// We define these in the dsl.c file.
extern const char* create_db_command;
extern const char* create_table_command;
extern const char* create_col_command;
extern const char* create_btree_command;
extern const char* load_command;
extern const char* rel_insert_command;
extern const char* select_command;
extern const char* select_pos_command;
extern const char* fetch_command;
extern const char* rel_delete_command;
extern const char* hashjoin_command;
extern const char* min_val_command;
extern const char* min_pos_val_command;
extern const char* max_val_command;
extern const char* max_pos_val_command;
extern const char* avg_val_command;
extern const char* add_command;
extern const char* sub_command;
extern const char* update_command;
extern const char* tuple_command;

#endif // DSL_H__
