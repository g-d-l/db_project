#include "dsl.h"

// Create Commands
// Matches: create(db, <db_name>);
const char* create_db_command = 
    "^create\\(db\\,\\\"[a-zA-Z0-9_]+\\\"\\)";

// Matches: create(tbl, <table_name>, <db_name>, <col_count>);
const char* create_table_command = 
    "^create\\(tbl\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_\\.]+\\,[0-9]+\\)";

// Matches: create(col, <col_name>, <tbl_var>, sorted);
const char* create_col_command = 
    "^create\\(col\\,\\\"[a-zA-Z0-9_\\.]+\\\"\\,[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,(sorted|unsorted)\\)";

// Matches: create(idx, <col_name>, [btree]);
const char* create_index_command = 
    "^create\\(idx\\,[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,(btree|sorted)\\)";
    
// Matches: load(<filename>);
const char* load_command = 
    "^load\\(\\\"[a-zA-Z0-9_\\./]+\\\"\\)";

// Matches: relational_insert(<tbl_var>,[INT1],[INT2],...);
const char* rel_insert_command =
    "^relational_insert\\([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+(\\,-?([0-9]+))+\\)";

// Matches: <vec_pos>=select(<col_var>,<low>,<high>);
const char* select_command = 
    "^[a-zA-Z0-9_]+=select\\([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,(-?([0-9]+)|null)\\,(-?([0-9]+)|null)\\)";

// Matches: <vec_pos>=select(<posn_vec>,<col_var>,<low>,<high>);
const char* select_pos_command = 
    "^[a-zA-Z0-9_]+=select\\([a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,(-?([0-9]+)|null)\\,(-?([0-9]+)|null)\\)";

// Matches: <vec_val>=fetch(<col_var>,<vec_pos>);
const char* fetch_command = 
    "^[a-zA-Z0-9_]+=fetch\\([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+\\)";

// Matches: relational_delete(<tbl_var>,<vec_pos>);
const char* rel_delete_command = 
    "^relational_delete\\([a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+\\)";

// Matches: <vec_pos1_out>,<vec_pos2_out>=hashjoin(<vec_val1>,<vec_pos1>,<vec_val2>,<vec_pos2>);
const char* hashjoin_command = 
    "^[a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+=hashjoin\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: <min_val>=min(<vec_val>);
const char* min_val_command = 
    "^[a-zA-Z0-9_]+=min\\([a-zA-Z0-9_\\.]+\\)";

// Matches: <min_pos>,<min_val>=min(<vec_pos>,<vec_val>);
const char* min_pos_val_command = 
    "^[a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+=min\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: <max_val>=max(<vec_val>);
const char* max_val_command = 
    "^[a-zA-Z0-9_]+=max\\([a-zA-Z0-9_\\.]+\\)";

// Matches: <max_pos>,<max_val>=min(<vec_pos>,<vec_val>);
const char* max_pos_val_command = 
    "^[a-zA-Z0-9_]+\\,[a-zA-Z0-9_]+=max\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";
    
// Matches: <scl_val>=avg(<vec_val>);
const char* avg_val_command = 
    "^[a-zA-Z0-9_]+=avg\\([a-zA-Z0-9_\\.]+\\)";
    
// Matches: <vec_val>=add(<vec_val1>,<vec_val2>);
const char* add_command = 
    "^[a-zA-Z0-9_]+=add\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";

// Matches: <vec_val>=sub(<vec_val1>,<vec_val2>);
const char* sub_command = 
    "^[a-zA-Z0-9_]+=sub\\([a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\)";
  
// Matches: update(<col_var>,<vec_pos>,[INT]);
const char* update_command = 
    "^update\\([a-zA-Z0-9_\\.]+\\.[a-zA-Z0-9_\\.]+\\.[a-zA-Z0-9_\\.]+\\,[a-zA-Z0-9_\\.]+\\,(-?[0-9]+)\\)";
 
// Matches: tuple(<vec_val1>,...);
const char* tuple_command = 
    "^tuple\\([a-zA-Z0-9_]+(\\,[a-zA-Z0-9_]+)*\\)";

// Matches: shutdown;
const char* shutdown_command = 
    "^shutdown";
    
// TODO(USER): You will need to update the commands here for every single command you add.
dsl** dsl_commands_init(void)
{
    dsl** commands = calloc(NUM_DSL_COMMANDS, sizeof(dsl*));

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        commands[i] = malloc(sizeof(dsl));
    }

    // Assign the create commands
    commands[0]->c = create_db_command;
    commands[0]->g = CREATE_DB;

    commands[1]->c = create_table_command;
    commands[1]->g = CREATE_TABLE;

    commands[2]->c = create_col_command;
    commands[2]->g = CREATE_COLUMN;
    
    commands[3]->c = create_index_command;
    commands[3]->g = CREATE_INDEX;
    
    commands[4]->c = load_command;
    commands[4]->g = LOAD;
    
    commands[5]->c = rel_insert_command;
    commands[5]->g = REL_INSERT;
    
    commands[6]->c = select_command;
    commands[6]->g = SELECT;
    
    commands[7]->c = select_pos_command;
    commands[7]->g = SELECT_POS;
    
    commands[8]->c = fetch_command;
    commands[8]->g = FETCH;
    
    commands[9]->c = rel_delete_command;
    commands[9]->g = DELETE;
    
    commands[10]->c = hashjoin_command;
    commands[10]->g = HASH_JOIN;
    
    commands[11]->c = min_val_command;
    commands[11]->g = MIN_VAL;
    
    commands[12]->c = min_pos_val_command;
    commands[12]->g = MIN_POS_VAL;
    
    commands[13]->c = max_val_command;
    commands[13]->g = MAX_VAL;
    
    commands[14]->c = max_pos_val_command;
    commands[14]->g = MAX_POS_VAL;
    
    commands[15]->c = avg_val_command;
    commands[15]->g = AVG_VAL;
    
    commands[16]->c = add_command;
    commands[16]->g = ADD;
    
    commands[17]->c = sub_command;
    commands[17]->g = SUB;
    
    commands[18]->c = update_command;
    commands[18]->g = UPDATE;
    
    commands[19]->c = tuple_command;
    commands[19]->g = TUPLE;

    commands[20]->c = shutdown_command;
    commands[20]->g = SHUTDOWN;

    return commands;
}
