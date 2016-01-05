/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
**/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <float.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "parser.h"
#include "utils.h"
#include "bin_search.h"
#include "bptree.h"
#include "rwlocks.h"
#include "comparators.h"
#include "threads.h"
#include "db.h"
#include "server.h"
#include "hash.h"
#include "save_data.h"
#include "threads.h"







// Here, we allow for a global of DSL COMMANDS to be shared in the program
dsl** dsl_commands;

// for when a client signal the server to shutdown
pthread_mutex_t shutdown_lock;
int shutdown_flag = 0;

// for keeping track of thread ids for WORKER_THREADS worker threads
// and n number of client threads
pthread_mutex_t thread_id_lock;
int nactive_clients = 0;
int nthread_slots = 0;
pthread_t* thread_ids = NULL;

// global instance of our worker hub
worker_hub hub;

// locks and condition variables for the hub
pthread_mutex_t curr_idle_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  curr_idle_cond  = PTHREAD_COND_INITIALIZER;
int curr_idle_count = 0;

pthread_mutex_t worker_ready_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  worker_ready_cond  = PTHREAD_COND_INITIALIZER;
int worker_ready = 0;

pthread_cond_t  currently_working_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t currently_working_mutex= PTHREAD_MUTEX_INITIALIZER;
int currently_working_count = 0;

pthread_mutex_t can_finish_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  can_finish_cond  = PTHREAD_COND_INITIALIZER;
int can_finish_count = 0;


// global list of databases;
db_group* db_list;

// global buffer of SELECT and SELECT_POS commands
select_buf* sbuf;




// create WORKER_THREADS many worker threads, and set the rest of the 
// initial slots to -1 to indicate that they're free to use
void thread_init(void) {
    nactive_clients = 0;
    nthread_slots = WORKER_THREADS + INIT_CLIENT_SLOTS;
    thread_ids = malloc(sizeof(pthread_t) * nthread_slots);
    int i = 0;
    for (; i < WORKER_THREADS; i++) {
        pthread_create(&thread_ids[i], NULL, worker_thread, (void*) i);
    }
    for (; i < nthread_slots; i++)
        thread_ids[i] = 0;
}

void save_client_id(pthread_t id) {
    pthread_mutex_lock(&thread_id_lock);
    if (nthread_slots == nactive_clients + WORKER_THREADS) {
        nthread_slots *= 2;
        thread_ids = realloc(thread_ids, sizeof(pthread_t) * nthread_slots);
    }
    int i = 0;
    while (thread_ids[i] != 0)
        i++;
    thread_ids[i] = id;
    nactive_clients++;
    pthread_mutex_unlock(&thread_id_lock);
}

void remove_client_id(pthread_t id) {
    pthread_mutex_lock(&thread_id_lock);
    int i = 0;
    while (thread_ids[i] != id)
        i++;
    thread_ids[i] = 0;
    nactive_clients--;
    pthread_mutex_unlock(&thread_id_lock);
}


void free_query(db_operator* query) {
    free(query->value1);
    free(query->tuples);
    free(query->contents);
    free(query);
}

void free_db(db* db1) {
    free(db1->name);
    for (int t = 0; t < db1->table_count; t++) {
        free(db1->tables[t]->name);
        for (int c = 0; c < db1->tables[t]->active_cols; c++) {
            free(db1->tables[t]->columns[c]->name);
            free(db1->tables[t]->columns[c]->data);
            free(db1->tables[t]->columns[c]);
        }
        free(db1->tables[t]->columns);
        free(db1->tables[t]);
    }
    free(db1->tables);
    free(db1);
}

void free_var_group(var_group* g) {
    for (int i = 0; i < g->index; i++) {
        free(g->list[i]->name);
        if (g->list[i]->use_overflow)
            free(g->list[i]->overflow);
        else if (!g->list[i]->is_float)
            free(g->list[i]->vals);
        free(g->list[i]);
    }
    free(g->list);
    free(g);
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator* parse_command(message* recv_message, message* send_message, var_group* g, int client_socket) {
    send_message->status = OK_WAIT_FOR_RESPONSE;
    db_operator *dbo = calloc(1, sizeof(db_operator));
    // Here you parse the message and fill in the proper db_operator fields for
    // now we just log the payload
    cs165_log(stdout, recv_message->payload);

    // Here, we give you a default parser, you are welcome to replace it with anything you want
    status parse_status = parse_command_string(recv_message->payload, dsl_commands, dbo, g, client_socket);
    if (parse_status.code != OK) {
        // Something went wrong
    }

    return dbo;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char* execute_db_operator(db_operator* query, var_group* g) {

    if (query->type == REL_INSERT) {
        table* tbl1 = query->table1;
        pthread_mutex_lock(&tbl1->wlock);
        tbl1->modified = 1;
        // lengthen columns or their secondary sorted array indices if needed
        if (tbl1->nstored == tbl1->length) {
            for (int i = 0; i < tbl1->col_count; i++) {
                tbl1->columns[i]->data = realloc(tbl1->columns[i]->data, 2 * sizeof(int) * tbl1->length);
                if (tbl1->columns[i]->index.type != SORTED)
                    tbl1->columns[i]->index.ordering = realloc(tbl1->columns[i]->index.ordering, 2 * sizeof(int) * tbl1->length);
            }
        }
        
        int insert_pos;
        // order must be propagated to whole table
        if (tbl1->sorted_col != -1) {
            insert_pos = bin_search_ins(tbl1->columns[tbl1->sorted_col]->data, tbl1->nstored, query->value1[tbl1->sorted_col]);
            for (int i = 0; i < tbl1->col_count; i++) {
                memmove(&tbl1->columns[i]->data[insert_pos + 1], &tbl1->columns[i]->data[insert_pos], (tbl1->nstored - insert_pos) * sizeof(int));
                tbl1->columns[i]->data[insert_pos] = query->value1[i];
            }
        }
        // otherwise put at end
        else {
            insert_pos = tbl1->nstored;
            for (int i = 0; i < tbl1->col_count; i++)
                tbl1->columns[i]->data[insert_pos] = query->value1[i];
        }
        // update any secondary indices as necessary
        for (int i = 0; i < tbl1->col_count; i++) {
            if (tbl1->columns[i]->index.type == SORTED) {
                int* ordering = (int*) tbl1->columns[i]->index.ordering;
                insert_pos = bin_search_ins(ordering, tbl1->nstored, query->value1[i]);
                memmove(&ordering[insert_pos + 1], &ordering[insert_pos], (tbl1->nstored - insert_pos) * sizeof(int));
                ordering[insert_pos] = query->value1[i];
            }
            else if (tbl1->columns[i]->index.type == B_PLUS_TREE) {
                tbl1->columns[i]->index.ordering = insert(tbl1->columns[i]->index.ordering, query->value1[i], insert_pos);
            }
        }

        tbl1->nstored++;
        pthread_mutex_unlock(&tbl1->wlock);
    }
    else if (query->type == CREATE_INDEX) {
        writer_lock(query->table1);
        create_index(query->table1, query->column1, query->index_type);
        writer_unlock(query->table1);
    }
    else if (query->type == HASH_JOIN) {
        var* val1 = query->var1;
        var* pos1 = query->var2;
        var* val2 = query->var3;
        var* pos2 = query->var4;

        var* r1 = malloc(sizeof(var));
        r1->name = query->output_name1;
        insert_res(r1, g);

        var* r2 = malloc(sizeof(var));
        r2->use_overflow = 0;
        r2->name = query->output_name2;
        insert_res(r2, g);

        hash_join(val1, pos1, val2, pos2, r1, r2);


    }
    else if (query->type == DELETE) {
        table* tbl1 = query->table1;
        writer_lock(tbl1);
        tbl1->modified = 1;
        var* to_del = query->var1;
        int original_len = tbl1->nstored;
        tbl1->nstored -= to_del->len;
        for (int i = 0; i < tbl1->col_count; i++) {
            int* vals_to_del = NULL;
            if (tbl1->columns[i]->index.type == SORTED)
                vals_to_del = malloc(sizeof(int) * to_del->len);
            for (int j = 0; j < to_del->len; j++) {
                int pos = to_del->vals[j];
                if (vals_to_del)
                    vals_to_del[j] = tbl1->columns[i]->data[pos];
                int next_pos = (j == to_del->len - 1) ? original_len : to_del->vals[j + 1];
                memmove(&tbl1->columns[i]->data[pos - j], &tbl1->columns[i]->data[pos + 1], sizeof(int) * (next_pos - pos - 1));
            }

            if (vals_to_del) {
                qsort(vals_to_del, to_del->len, sizeof(int), int_compare);
                int* positions = malloc(sizeof(int) * to_del->len);
                for (int j = 0; j < to_del->len; j++)
                    positions[j] = bin_search_del(tbl1->columns[i]->index.ordering, original_len, vals_to_del[j]);
                for (int j = 0; j < to_del->len; j++) {
                    int pos = positions[j];
                    int next_pos = (j == to_del->len - 1) ? original_len : to_del->vals[j + 1];
                    int* ordering = (int*) tbl1->columns[i]->index.ordering;
                    memmove(&ordering[pos - j], &ordering[pos + 1], 
                        sizeof(int) * (next_pos - pos - 1));
                }
            }
            if (tbl1->columns[i]->index.type == B_PLUS_TREE) {
                create_index(tbl1, tbl1->columns[i], B_PLUS_TREE);
            }
        }
        writer_unlock(tbl1);
    }
    else if (query->type == TUPLE) {
        int len = query->tuples[0]->len;

        if (query->tuples[0]->is_float) {
            int size = (DBL_MANT_DIG + 1) * query->tuple_count * len + 2;
            char* result = malloc(size);
            int index = 0;
            for (int i = 0; i < query->tuple_count; i++) {
                index += sprintf(&result[index], "%lf\n", query->tuples[i]->decimal);
            }
            result[index] = '\0';
            return result;
        }
        else {
            int size = (INT_MAX_WIDTH + 1) * query->tuple_count * len + 1;
            char* result = malloc(size);
            int index = 0;
            for (int i = 0; i < len; i++) {
                for (int j = 0; j < query->tuple_count - 1; j++) {
                    if (query->tuples[j]->use_overflow == 0)
                        index += sprintf(&result[index], "%d,", query->tuples[j]->vals[i]);
                    else 
                        index += sprintf(&result[index], "%lld,", query->tuples[j]->overflow[i]);
                }
                if (query->tuples[query->tuple_count - 1]->use_overflow == 0) 
                    index += sprintf(&result[index], "%d\n", query->tuples[query->tuple_count - 1]->vals[i]);
                else
                    index += sprintf(&result[index], "%lld\n", query->tuples[query->tuple_count - 1]->overflow[i]);
            }
            result[index] = '\0';
            return result;
        }
    }
    else if (query->type == SELECT || query->type == SELECT_POS)
        ; // handled via sbuf
    else if (query->type == FETCH) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 0;
        res->is_float = 0;
        res->name = query->output_name1;
        insert_res(res, g);

        var* var1 = query->var1;
        res->len = var1->len;
        table* tbl1 = query->table1;
        reader_lock(tbl1);
        column* col1 = query->column1;
        res->vals = malloc(sizeof(int) * var1->len);


        // no other client using hub
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {

            hub.task = FETCH;
            int division_size = var1->len / WORKER_THREADS;
            for (int i = 0; i < WORKER_THREADS; i++) {
                hub.arrays1[i] = col1->data;
                hub.arrays2[i] = &var1->vals[i * division_size];
                hub.arrays3[i] = &res->vals[i * division_size];
                hub.lens[i] = division_size;
            }
            hub.lens[WORKER_THREADS - 1] = var1->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();
            pthread_mutex_unlock(&hub.hub_lock);    
        }
        else {
            fetch(col1->data, var1->vals, var1->len, res->vals);
        }
        reader_unlock(tbl1);

    }
    else if (query->type == LOAD) {
        char* contents = query->contents;
        char* db_name = strtok(contents, ".");
        char* tbl_name = strtok(NULL, ".");
        db* db1 = find_db(db_name);
        table* tbl1 = find_table(tbl_name, db1);

        writer_lock(tbl1);
        int dim = tbl1->col_count;
        int rows = 0;
        int index = 0;
        while (index < query->contents_len) {
            if (contents[index] == '\n')
                rows++;
            index++;
        }
        rows--;

        tbl1->length = (rows + tbl1->nstored) * 2;
        int original_len = tbl1->nstored;
        tbl1->nstored = rows + tbl1->nstored;
        // import contents as integers
        strtok(NULL, "\n");
        int** matrix = malloc(sizeof(int*) * dim);
        for (int i = 0; i < dim; i++)
            matrix[i] = malloc(sizeof(int) * tbl1->length);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < dim - 1; j++) {
                char* next = strtok(NULL, ",");
                matrix[j][i] = atoi(next);
            }
            char* next = strtok(NULL, "\n");
            matrix[dim - 1][i] = atoi(next);
        }
        for (int i = 0; i < dim; i++) {
            for (int j = 0; j < original_len; j++)
                matrix[i][rows + j] = tbl1->columns[i]->data[j];
        }
        // need to account for sorted column
        if (tbl1->sorted_col != -1) {
            pair* pairings = malloc(sizeof(pair) * (tbl1->nstored));
            for (int i = 0; i < (tbl1->nstored); i++) {
                pairings[i].key = matrix[tbl1->sorted_col][i];
                pairings[i].value = (void*) i;
            }
            qsort(pairings, tbl1->nstored, sizeof(pair), bl_compare);
            for (int i = 0; i < dim; i++) {
                tbl1->columns[i]->data = malloc(sizeof(int) * tbl1->nstored);
                for (int j = 0; j < (tbl1->nstored); j++) {
                    tbl1->columns[i]->data[j] = matrix[i][(int) pairings[j].value];
                }
                if (tbl1->columns[i]->index.type != NONE)
                    create_index(tbl1, tbl1->columns[i], tbl1->columns[i]->index.type);
            }
            for (int i = 0; i < dim; i++)
                free(matrix[i]);
            free(matrix);
        }
        else {
            for (int i = 0; i < dim; i++) {
                free(tbl1->columns[i]->data);
                tbl1->columns[i]->data = matrix[i];
                if (tbl1->columns[i]->index.type != NONE)
                     create_index(tbl1, tbl1->columns[i], tbl1->columns[i]->index.type);

            }
        }

        writer_unlock(tbl1);

    }
    else if (query->type == MIN_VAL) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 1;
        res->is_float = 0;
        res->name = query->output_name1;
        res->overflow = malloc(sizeof(long long));
        res->len = 1;
        insert_res(res, g);

        var* var1 = query->var1;

        // no other client using hub
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            hub.task = MIN_VAL;
            int division_size = var1->len / WORKER_THREADS;
            if (var1->use_overflow) {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = (int*) (&var1->overflow[i * division_size]);
                    hub.lens[i] = division_size;
                    hub.overflow = 1;
                }
            }
            else {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &var1->vals[i * division_size];
                    hub.lens[i] = division_size;
                    hub.overflow = 0;
                }
            }
            hub.lens[WORKER_THREADS - 1] = var1->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            long long result = hub.results[0];
            for (int i = 1; i < WORKER_THREADS; i++)
                result = (hub.results[i] < result) ? hub.results[i] : result;
            *res->overflow = result;

            pthread_mutex_unlock(&hub.hub_lock);
            
        }
        else {
            long long result;
            if (var1->use_overflow)
                find_min_l(var1->overflow, var1->len, &result);
            else 
                find_min(var1->vals, var1->len, &result);
            *res->overflow = result;
        }

    }
    else if (query->type == MIN_POS_VAL) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 1;
        res->is_float = 0;
        res->name = query->output_name1;
        res->overflow = malloc(sizeof(long long));
        res->len = 1;
        insert_res(res, g);

        var* positions = query->var1;
        var* values = query->var2;
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            
            hub.task = MIN_POS_VAL;
            int division_size = positions->len / WORKER_THREADS;
            if (values->use_overflow) {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &positions->vals[i * division_size];
                    hub.arrays2[i] = (int*) (&values->overflow[i * division_size]);
                    hub.lens[i] = division_size;
                    hub.overflow = 1;
                }
            }
            else {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &positions->vals[i * division_size];
                    hub.arrays2[i] = &values->vals[i * division_size];
                    hub.lens[i] = division_size;
                    hub.overflow = 0;
                }
            }
            hub.lens[WORKER_THREADS - 1] = positions->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            long long result = hub.results[0];
            for (int i = 1; i < WORKER_THREADS; i++)
                result = (hub.results[i] < result) ? hub.results[i] : result;
            *res->overflow = result;

            pthread_mutex_unlock(&hub.hub_lock);

            
        }
        else {
            long long result;
            if (values->use_overflow)
                find_min_pos_l(positions->vals, values->overflow, positions->len, &result);
            else
                find_min_pos(positions->vals, values->vals, positions->len, &result);
            *res->overflow = result;
        }

    }
    else if (query->type == MAX_VAL) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 1;
        res->is_float = 0;
        res->name = query->output_name1;
        res->overflow = malloc(sizeof(long long));
        res->len = 1;
        insert_res(res, g);

        var* var1 = query->var1;
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            hub.task = MAX_VAL;
            int division_size = var1->len / WORKER_THREADS;
            if (var1->use_overflow) {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = (int*) (&var1->overflow[i * division_size]);
                    hub.lens[i] = division_size;
                    hub.overflow = 1;
                }
            }
            else {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &var1->vals[i * division_size];
                    hub.lens[i] = division_size;
                    hub.overflow = 0;
                }
            }
            hub.lens[WORKER_THREADS - 1] = var1->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            long long result = hub.results[0];
            for (int i = 1; i < WORKER_THREADS; i++)
                result = (hub.results[i] > result) ? hub.results[i] : result;
            
            *res->overflow = result;

            pthread_mutex_unlock(&hub.hub_lock);

            
        }
        else {
            long long result;
            if (var1->use_overflow)
                find_max_l(var1->overflow, var1->len, &result);
            else
                find_max(var1->vals, var1->len, &result);
            *res->overflow = result;
        }

    }
    else if (query->type == MAX_POS_VAL) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 1;
        res->is_float = 0;
        res->name = query->output_name1;
        res->overflow = malloc(sizeof(long long));
        res->len = 1;
        insert_res(res, g);

        var* positions = query->var1;
        var* values = query->var2;
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            hub.task = MAX_POS_VAL;
            int division_size = values->len / WORKER_THREADS;
            if (values->use_overflow) {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &positions->vals[i * division_size];
                    hub.arrays2[i] = (int*) (&values->overflow[i * division_size]);
                    hub.lens[i] = division_size;
                }
                hub.overflow = 1;
            }
            else {
                for (int i = 0; i < WORKER_THREADS; i++) {
                    hub.arrays1[i] = &positions->vals[i * division_size];
                    hub.arrays2[i] = &values->vals[i * division_size];
                    hub.lens[i] = division_size;
                }
                hub.overflow = 0;
            }
            hub.lens[WORKER_THREADS - 1] = positions->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            long long result = hub.results[0];
            for (int i = 1; i < WORKER_THREADS; i++)
                result = (hub.results[i] > result) ? hub.results[i] : result;
            *res->overflow = result;

            pthread_mutex_unlock(&hub.hub_lock);

            
        }
        else {
            long long result;
            if (values->use_overflow)
                find_max_pos_l(positions->vals, values->overflow, positions->len, &result);
            else
                find_max_pos(positions->vals, values->vals, positions->len, &result);
            *res->overflow = result;
        }

    }
    else if (query->type == AVG_VAL) {
        var* res = malloc(sizeof(var));
        res->use_overflow = 0;
        res->is_float = 1;
        res->name = query->output_name1;
        res->len = 1;
        insert_res(res, g);

        int* vals = (query->var1 == NULL) ? query->column1->data : query->var1->vals;
        int len = (query->var1 == NULL) ? query->table1->nstored : query->var1->len;

        if (pthread_mutex_trylock(&hub.hub_lock) != 0) {
            hub.task = AVG_VAL;
            int division_size = len / WORKER_THREADS;
            for (int i = 0; i < WORKER_THREADS; i++) {
                hub.arrays1[i] = &vals[i * division_size];
                hub.lens[i] = division_size;
            }
            hub.lens[WORKER_THREADS - 1] = len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            long long result = 0;
            for (int i = 0; i < WORKER_THREADS; i++)
                result += hub.results[i];
            res->decimal = ((double) result / (float) len);
            
            pthread_mutex_unlock(&hub.hub_lock);

            
        }
        else {
            long long result;
            sum(vals, len, &result);
            res->decimal = ((double) result / (float) len);
        }

    }
    else if (query->type == ADD) {

        var* var1 = query->var1;
        var* var2 = query->var2;

        var* res = malloc(sizeof(var));
        res->use_overflow = 0;
        res->name = query->output_name1;
        res->is_float = 0;
        res->len = var1->len;
        insert_res(res, g);

        res->overflow = malloc(sizeof(long long) * var1->len);
        res->use_overflow = 1;
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            hub.task = ADD;
            int division_size = var1->len / WORKER_THREADS;
            for (int i = 0; i < WORKER_THREADS; i++) {
                hub.arrays1[i] = &var1->vals[i * division_size];
                hub.arrays2[i] = &var2->vals[i * division_size];
                hub.arrays3[i] = (int*) &res->overflow[i * division_size];
                hub.lens[i] = division_size;
            }
            hub.lens[WORKER_THREADS - 1] = var1->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            pthread_mutex_unlock(&hub.hub_lock);            
        }
        else {
            add(var1->vals, var2->vals, var1->len, res->overflow);
        }

    }
    else if (query->type == SUB) {

        var* var1 = query->var1;
        var* var2 = query->var2;

        var* res = malloc(sizeof(var));
        res->use_overflow = 0;
        res->is_float = 0;
        res->name = query->output_name1;
        res->len = var1->len;
        insert_res(res, g);

        res->overflow = malloc(sizeof(long long) * var1->len);
        res->use_overflow = 1;
        if (pthread_mutex_trylock(&hub.hub_lock) == 0) {
            hub.task = SUB;
            int division_size = var1->len / WORKER_THREADS;
            for (int i = 0; i < WORKER_THREADS; i++) {
                hub.arrays1[i] = &var1->vals[i * division_size];
                hub.arrays2[i] = &var2->vals[i * division_size];
                hub.arrays3[i] = (int*) &res->overflow[i * division_size];
                hub.lens[i] = division_size;
            }
            hub.lens[WORKER_THREADS - 1] = var1->len - (WORKER_THREADS - 1) * division_size;
            
            use_threads();

            pthread_mutex_unlock(&hub.hub_lock);            
        }
        else {
            sub(var1->vals, var2->vals, var1->len, res->overflow);
        }

    }
    else if (query->type == UPDATE) {
        table* tbl1 = query->table1;
        column* col1 = query->column1;
        var* positions = query->var1;
        db_operator** inserts = malloc(sizeof(db_operator*) * positions->len);
        reader_lock(tbl1);
        tbl1->modified = 1;
        int col_index = 0;
        while (tbl1->columns[col_index] != col1)
            col_index++;

        for (int i = 0; i < positions->len; i++) {
            inserts[i] = malloc(sizeof(db_operator));
            inserts[i]->value1 = malloc(sizeof(int) * tbl1->active_cols);
            for (int j = 0; j < tbl1->active_cols; j++)
                inserts[i]->value1[j] = tbl1->columns[j]->data[positions->vals[i]];
            inserts[i]->value1[col_index] = query->new_val;
            inserts[i]->table1 = tbl1;
            inserts[i]->type = REL_INSERT;
        }
        reader_unlock(tbl1);
        query->type = DELETE;
        execute_db_operator(query, g);
        for (int i = 0; i < positions->len; i++) {
            execute_db_operator(inserts[i], g);
            free(inserts[i]->value1);
        }
        free(inserts);


    }
    else if (query->type == SHUTDOWN) {
        pthread_mutex_lock(&shutdown_lock);
        shutdown_flag = 1;
        pthread_mutex_unlock(&shutdown_lock);
    }
    return "";
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void* handle_client(void* arg) {
    int client_socket = (int) arg;
    int done = 0;
    int length = 0;
    var_group* g = new_var_group();

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            // 1. Parse command
            db_operator* query = parse_command(&recv_message, &send_message, g, client_socket);

            // 2. Handle request
            char* result = execute_db_operator(query, g);
            if (!(query->type == SELECT || query->type == SELECT_POS))
                free_query(query);
            send_message.length = strlen(result);
            if (send_message.length == 0)
                send_message.status = EMPTY;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            recv(client_socket, &(recv_message), sizeof(message), 0);
            int len = send_message.length;
            int nsent = 0;
            while (len > nsent) {
                send_message.status = SENDING_FILE;
                send_message.length = min2(PAGE, len - nsent);
                send_message.payload = NULL;
                send(client_socket, &(send_message), sizeof(message), 0);
                recv(client_socket, &(recv_message), sizeof(message), 0);
                send(client_socket, &result[nsent], send_message.length, 0);
                recv(client_socket, &(recv_message), sizeof(message), 0);
                nsent += send_message.length;
            }
            send_message.length = 0;
            send(client_socket, &(send_message), sizeof(message), 0);
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    remove_client_id(pthread_self());
    free_var_group(g);
    return NULL;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}









int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    pthread_mutex_init(&hub.queue_lock, NULL);
    pthread_mutex_init(&hub.hub_lock, NULL);
    thread_init();

    // Populate the global dsl commands
    dsl_commands = dsl_commands_init();


    load_dbs();

    sbuf = malloc(sizeof(select_buf));
    sbuf->buf = malloc(sizeof(db_operator*) * INIT_COL_LEN);
    sbuf->index = 0;
    sbuf->capacity = INIT_COL_LEN;

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;
    fcntl(server_socket, F_SETFL, fcntl(server_socket, F_GETFL, 0) | O_NONBLOCK);


    // spawn a new thread for each client
    while (1) {

        client_socket = accept(server_socket, (struct sockaddr *)&remote, &t);
        if (client_socket >= 0) {
            pthread_t thread_id;
            pthread_create(&thread_id, NULL, handle_client, (void*) client_socket);
            save_client_id(thread_id);
        }


        // check for a shutdown call
        pthread_mutex_lock(&shutdown_lock);
        if (shutdown_flag) {
            flush_data();
            pthread_mutex_unlock(&shutdown_lock);
            break;
        }
        pthread_mutex_unlock(&shutdown_lock);
    }
    flush_data();

    // freeing memory
    for (int i = 0; i < db_list->index; i++)
        free_db(db_list->list[i]);
    free(db_list->list);
    free(db_list);

    free(sbuf->buf);
    free(sbuf);
    free(thread_ids);

    for (int i = 0; i < NUM_DSL_COMMANDS; ++i)
        free(dsl_commands[i]);
    free(dsl_commands);
    
    return 0;
}

