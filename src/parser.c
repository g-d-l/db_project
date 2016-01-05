#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#include "cs165_api.h"
#include "db.h"
#include "select_buf.h"
#include "message.h"


#define BUF 4096

// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op, var_group* g, int client_socket);

// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
status parse_command_string(char* str, dsl** commands, db_operator* op, var_group* g, int client_socket)
{
    log_info("Parsing: %s", str);

    // Create a regular expression to parse the string
    regex_t regex;
    int ret;

    // Track the number of matches; a string must match all
    int n_matches = 1;
    regmatch_t m;

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
        dsl* d = commands[i];
        if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
            log_err("Could not compile regex\n");
        }

        // Bind regular expression associated with the string
        ret = regexec(&regex, str, n_matches, &m, 0);

        // If we have a match, then figure out which one it is!
        if (ret == 0) {
            log_info("Found Command: %d\n", i);
            // Here, we actually strip the command as appropriately
            // based on the DSL to get the variable names.
            return parse_dsl(str, d, op, g, client_socket);
        }
    }

    // Nothing was found!
    status s;
    s.code = ERROR;
    return s;
}

// return a copy of a name to be saved
char* name_copy(const char* name) {
    char* output_copy = malloc(strlen(name) + 1);
    memcpy(output_copy, name, strlen(name) + 1);
    return output_copy;
}

status parse_dsl(char* str, dsl* d, db_operator* op, var_group* g, int client_socket)
{
    printf("    %s\n", str);
    // Use the commas to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char comma[2] = ",";
    char quotes[2] = "\"";
    char period[2] = ".";
    char equals[2] = "=";

    char* str_cpy = malloc(strlen(str) + 1);
    strncpy(str_cpy, str, strlen(str) + 1);
    // create(db, <db_name>);
    if (d->g == CREATE_DB) {
        strtok(str_cpy, open_paren);
        strtok(NULL, comma);
        char* db_name = strtok(NULL, quotes);

        log_info(str);

        create_db(db_name);
        free(str_cpy);

        op->type = DONE;
        status ret;
        ret.code = OK;
        
        return ret;

    // create(tbl, <table_name>, <db_name>, <col_count>);
    } else if (d->g == CREATE_TABLE) {
        strtok(str_cpy, open_paren);
        strtok(NULL, comma);

        char* tbl_name = strtok(NULL, quotes);

        char* db_name = strtok(NULL, comma);
        
        char* count_str = strtok(NULL, close_paren);
        int count = atoi(count_str);

        log_info(str);

  
        db* db1 = find_db(db_name);

        
        create_table(tbl_name, db1, count);

        free(str_cpy);

        op->type = DONE;
        status ret;
        ret.code = OK;
        return ret;

    // create(col, <col_name>, <tbl_var>, sorted);
    } else if (d->g == CREATE_COLUMN) {
        strtok(str_cpy, open_paren);
        strtok(NULL, comma);

        char* col_name = strtok(NULL, quotes);
        char* db_name = &strtok(NULL, period)[1];
        char* tbl_name = strtok(NULL, comma);

        // This gives us the "unsorted/sorted"
        char* sorting_str = strtok(NULL, close_paren);

        log_info(str);

        db* db1 = find_db(db_name);

        table* tbl1 = find_table(tbl_name, db1);

        int is_sorted = (strcmp(sorting_str, "sorted") == 0) ? 1 : 0;    
        create_column(col_name, tbl1, is_sorted);
        tbl1->modified = 1;
        free(str_cpy);
        
        
        // No db_operator required, since no query plan
        op->type = DONE;
        status ret;
        ret.code = OK;
        return ret;
    }
    // create(idx, <col_name>, btree | sorted);
    else if (d->g == CREATE_INDEX) {

        strtok(str_cpy, open_paren);

        strtok(NULL, comma);

        char* db_name = strtok(NULL, period);

        char* tbl_name = strtok(NULL, period);
        
        char* col_name = strtok(NULL, comma);

        char* type = strtok(NULL, close_paren);

        log_info(str);

        db* db1 = find_db(db_name);
        
        table* tbl1 = find_table(tbl_name, db1);

        column* col1 = find_col(col_name, tbl1);

        op->db1 = db1;
        op->table1 = tbl1;
        op->column1 = col1;
        op->type = CREATE_INDEX;
        op->index_type = (strcmp(type, "btree") == 0) ? B_PLUS_TREE : SORTED; 
        // Free the str_cpy
        free(str_cpy);
        
        // No db_operator required, since no query plan
        status ret;
        ret.code = OK;
        return ret;
    }
    // relational_insert(<tbl_var>,[INT1],[INT2],...);
    else if (d->g == REL_INSERT) {

        strtok(str_cpy, open_paren);

        char* db_name = strtok(NULL, period);
        
        char* tbl_name = strtok(NULL, comma);
        
        db* db1 = find_db(db_name);
        
        table* tbl1 = find_table(tbl_name, db1);

        int nvals = tbl1->col_count;
        op->type = REL_INSERT;
        op->value1 = malloc(sizeof(int) * nvals);
        for (int i = 0; i < nvals - 1; i++) {
            char* next = strtok(NULL, ",");
            op->value1[i] = atoi(next);
        }
        char* next = strtok(NULL, "\n");
        op->value1[nvals - 1] = atoi(next);        
        
        op->db1 = db1;
        op->table1 = tbl1;

        log_info(str);

        // Free the str_cpy
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // relational_delete(<tbl_var>,<vec_pos>);
    else if (d->g == DELETE) {

        strtok(str_cpy, open_paren);

        char* db_name = strtok(NULL, period);
        char* tbl_name = strtok(NULL, comma);
        char* vec_name = strtok(NULL, close_paren);

        db* db1 = find_db(db_name);
        
        table* tbl1 = find_table(tbl_name, db1);

        var* vec = find_res(vec_name, g);

        op->type = DELETE;
        op->db1 = db1;
        op->table1 = tbl1;
        op->var1 = vec;

        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // tuple(<vec_val1>,...);
    else if (d->g == TUPLE) {
        
        int count = 0;
        int i = 0;
        while (str_cpy[i] != ')') {
            if (str_cpy[i] == ',')
                count++;
            i++;
        }
        count++;
        op->tuples = malloc(sizeof(var*) * count);
        op->tuple_count = count;
        strtok(str_cpy, open_paren);
        for (i = 0; i < count - 1; i++) {
            char* var_name = strtok(NULL, comma);
            var* v = find_res(var_name, g);
            op->tuples[i] = v;
        }
        char* var_name = strtok(NULL, close_paren);
        var* v = find_res(var_name, g);
        op->tuples[i] = v;

        op->type = TUPLE;

        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    else if (d->g == LOAD) {

        strtok(str_cpy, open_paren);
        
        char* file_name = strtok(NULL, quotes);

        message send_message;
        message recv_message;

        // tell client to send over file
        send_message.status = PROVIDE_FILE;
        send_message.length = strlen(file_name) + 1;
        send_message.payload = NULL;
        send(client_socket, &(send_message), sizeof(message), 0);
        recv(client_socket, &(recv_message), sizeof(message), 0);
        send(client_socket, file_name, strlen(file_name) + 1, 0);
        int size;
        recv(client_socket, &size, sizeof(int), 0);
        char* contents = malloc(size);
        int index = 0;
        send_message.status = OK_WAIT_FOR_RESPONSE;
        send(client_socket, &(send_message), sizeof(message), 0);
        recv(client_socket, &(recv_message), sizeof(message), 0);

        while (recv_message.status == SENDING_FILE) {
            int chunk_size = recv_message.length;
            send(client_socket, &(send_message), sizeof(message), 0);
            recv(client_socket, &contents[index], chunk_size, 0);
            index += chunk_size;
            send(client_socket, &(send_message), sizeof(message), 0);
            recv(client_socket, &(recv_message), sizeof(message), 0);
        }
        send_message.status = OK_DONE;

        

        op->type = LOAD;
        op->contents = contents;
        op->contents_len = size;

        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_pos>=select(<col_var>,<low>,<high>);
    else if (d->g == SELECT) {
        char* output_name = strtok(str_cpy, equals);
        op->output_name1 = name_copy(output_name);
        strtok(NULL, open_paren);

        char* db_name = strtok(NULL, period);
        char* table_name = strtok(NULL, period);
        char* col_name = strtok(NULL, comma);
        
        db* db1 = find_db(db_name);
        table* tbl1 = find_table(table_name, db1);
        column* col1 = find_col(col_name, tbl1);
        op->type = SELECT;        
        
        char* vals = strtok(NULL, close_paren);
        long long i;
        if (strncmp(vals, "null", 4) == 0)
            op->min = INT_MIN;
        else {
            sscanf(vals, "%lld", &i);
            op->min = (i > (long long) INT_MIN) ? (int) i : INT_MIN;
        }
        vals = strtok(vals, comma);
        vals = strtok(NULL, close_paren);
        if (strncmp(vals, "null", 4) == 0)
            op->max = INT_MAX;
        else {
            sscanf(vals, "%lld", &i);
            op->max = (i < (long long) INT_MAX) ? (int) i : INT_MAX;
        }
        op->db1 = db1;
        op->table1 = tbl1;
        op->column1 = col1;
        op->group = g;
        op->var1 = NULL;

        insert_select(op);

        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_pos>=select(<posn_vec>,<col_var>,<low>,<high>);
    else if (d->g == SELECT_POS) {

        char* output_name = strtok(str_cpy, equals);
        op->output_name1 = name_copy(output_name);

        strtok(NULL, open_paren);

        char* vec_name = strtok(NULL, comma);
        char* db_name = strtok(NULL, period);
        char* table_name = strtok(NULL, period);
        
        char* col_name = strtok(NULL, comma);
        
        db* db1 = find_db(db_name);
        table* tbl1 = find_table(table_name, db1);
        column* col1 = find_col(col_name, tbl1);
        var* vec = find_res(vec_name, g);
        op->type = SELECT;        
        
        char* vals = strtok(NULL, close_paren);
        long long i;
        if (strncmp(vals, "null", 4) == 0)
            op->min = INT_MIN;
        else {
            sscanf(vals, "%lld", &i);
            op->min = (i > (long long) INT_MIN) ? (int) i : INT_MIN;
        }
        vals = strtok(vals, comma);
        vals = strtok(NULL, close_paren);
        if (strncmp(vals, "null", 4) == 0)
            op->max = INT_MAX;
        else {
            sscanf(vals, "%lld", &i);
            op->max = (i < (long long) INT_MAX) ? (int) i : INT_MAX;
        }
        op->db1 = db1;
        op->table1 = tbl1;
        op->column1 = col1;
        op->var1 = vec;
        op->group = g;

        insert_select(op);

        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_val>=fetch(<col_var>,<vec_pos>);
    else if (d->g == FETCH) {
        
        op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);
        char* db_name = strtok(NULL, period);
        char* table_name = strtok(NULL, period);
        char* col_name = strtok(NULL, comma);
        
        char* pos_var = strtok(NULL, close_paren);
        
        db* db1 = find_db(db_name);
        table* tbl1 = find_table(table_name, db1);
        column* col1 = find_col(col_name, tbl1);
        var* var1 = find_res(pos_var, g);
        
        op->type = FETCH;        
        op->db1 = db1;
        op->table1 = tbl1;
        op->column1 = col1;
        op->var1 = var1;
            
        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_pos1_out>,<vec_pos2_out>=hashjoin(<vec_val1>,<vec_pos1>,<vec_val2>,<vec_pos2>);
    else if (d->g == HASH_JOIN) {
        
        op->output_name1 = name_copy(strtok(str_cpy, comma));
        op->output_name2 = name_copy(strtok(NULL, equals));
        strtok(NULL, open_paren);

        char* vec_val1 = strtok(NULL, comma);
        char* vec_pos1 = strtok(NULL, comma);
        char* vec_val2 = strtok(NULL, comma);
        char* vec_pos2 = strtok(NULL, close_paren);

        op->type = HASH_JOIN;        
        op->var1 = find_res(vec_val1, g);
        op->var2 = find_res(vec_pos1, g);
        op->var3 = find_res(vec_val2, g);
        op->var4 = find_res(vec_pos2, g);

        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    else if (d->g == MIN_VAL) {
                op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);
        char* pos_var = strtok(NULL, close_paren);
        
        
        op->type = MIN_VAL;        
        op->var1 = find_res(pos_var, g);
            
        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <min_pos>,<min_val>=min(<vec_pos>,<vec_val>);
    else if (d->g == MIN_POS_VAL) {
        op->output_name1 = name_copy(strtok(str_cpy, comma));
        op->output_name2 = name_copy(strtok(NULL, equals));
        strtok(NULL, open_paren);
        char* vec_pos = strtok(NULL, comma);
        char* vec_val = strtok(NULL, close_paren);

        
        op->type = MIN_POS_VAL;        
        op->var1 = find_res(vec_pos, g);
        op->var2 = find_res(vec_val, g);
            
        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <max_val>=max(<vec_val>);
    else if (d->g == MAX_VAL) {
        op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);
        char* pos_var = strtok(NULL, close_paren);
        
        
        op->type = MAX_VAL;        
        op->var1 = find_res(pos_var, g);
            
        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <max_pos>,<max_val>=min(<vec_pos>,<vec_val>);
    else if (d->g == MAX_POS_VAL) {
        op->output_name1 = name_copy(strtok(str_cpy, comma));
        op->output_name2 = name_copy(strtok(NULL, equals));
        strtok(NULL, open_paren);
        char* vec_pos = strtok(NULL, comma);
        char* vec_val = strtok(NULL, close_paren);

        
        op->type = MAX_POS_VAL;        
        op->var1 = find_res(vec_pos, g);
        op->var2 = find_res(vec_val, g);
            
        log_info(str);

        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <scl_val>=avg(<vec_val>);
    else if (d->g == AVG_VAL) {
        op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);

        char* vals = strtok(NULL, close_paren);
        int is_col = 0;
        unsigned i = 0;
        while (i < strlen(vals)) {
            if (vals[i] == '.') {
                is_col = 1;
                break;
            }
            i++;
        }

        if (is_col) {
            char* db_name = strtok(vals, period);
            char* table_name = strtok(NULL, period);
            char* col_name = strtok(NULL, comma);
            
            db* db1 = find_db(db_name);
            op->table1 = find_table(table_name, db1);
            op->column1 = find_col(col_name, op->table1);
            op->var1 = NULL;

        }
        else
            op->var1 = find_res(vals, g);

        op->type = AVG_VAL;  

        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_val>=add(<vec_val1>,<vec_val2>);
    else if (d->g == ADD) {
        op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);

        char* vals1 = strtok(NULL, comma);
        char* vals2 = strtok(NULL, close_paren);

        
        op->type = ADD;        
        op->var1 = find_res(vals1, g);
        op->var2 = find_res(vals2, g);
            
        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // <vec_val>=sub(<vec_val1>,<vec_val2>);
    else if (d->g == SUB) {
        op->output_name1 = name_copy(strtok(str_cpy, equals));
        strtok(NULL, open_paren);

        char* vals1 = strtok(NULL, comma);
        char* vals2 = strtok(NULL, close_paren);

        
        op->type = SUB;        
        op->var1 = find_res(vals1, g);
        op->var2 = find_res(vals2, g);
            
        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // update(<col_var>,<vec_pos>,[INT]);
    else if (d->g == UPDATE) {
        strtok(str_cpy, open_paren);

        char* db_name = strtok(NULL, period);
        char* table_name = strtok(NULL, period);
        char* col_name = strtok(NULL, comma);
        char* vec_name = strtok(NULL, comma);
        
        char* new_val_str = strtok(NULL, close_paren);
        
        
        db* db1 = find_db(db_name);
        table* tbl1 = find_table(table_name, db1);
        column* col1 = find_col(col_name, tbl1);
        var* vec = find_res(vec_name, g);
        
        op->type = UPDATE;        
        op->db1 = db1;
        op->table1 = tbl1;
        op->column1 = col1;
        op->var1 = vec;
        op->new_val = atoi(new_val_str);

        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    else if (d->g == SHUTDOWN) {
        op->type = SHUTDOWN;        
        
        
        log_info(str);
        free(str_cpy);
        
        status ret;
        ret.code = OK;
        return ret;
    }
    // Should have been caught earlier...
    free(str_cpy);
    status fail;
    fail.code = ERROR;
    return fail;
}
