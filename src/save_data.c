#define _BSD_SOURCE
#define _XOPEN_SOURCE 700

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <error.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>


#include "cs165_api.h"
#include "global_lists.h"
#include "db.h"
#include "rwlocks.h"

extern db_group* db_list;

// return number of files in a directory, used to count number of columns in a table
int num_files(const char* path) {
    int count = 0;
    struct dirent* dent;
    DIR* srcdir = opendir(path);

    while((dent = readdir(srcdir)) != NULL) {
        if(strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
            continue;
        count++;
    }
    closedir(srcdir);
    return count;
}

// load database metadata
void load_dbs(void) {
    char* path = "saved_dbs";
    db_list = malloc(sizeof(db_group));
    db_list->list = malloc(sizeof(db*) * INIT_COL_LEN);
    db_list->index = 0;
    db_list->capacity = INIT_COL_LEN;


    struct dirent* db_f;
    DIR* srcdir = opendir(path);

    while ((db_f = readdir(srcdir)) != NULL) {
        struct stat st;

        if (strcmp(db_f->d_name, ".") == 0 || strcmp(db_f->d_name, "..") == 0)
            continue;

        fstatat(dirfd(srcdir), db_f->d_name, &st, 0);

        if (S_ISDIR(st.st_mode)) {
            db* db1 = create_db(db_f->d_name);

            // make table metadata
            char* tbl_dir = malloc(strlen(path) + strlen(db_f->d_name) + 2);
            memcpy(tbl_dir, path, strlen(path));
            tbl_dir[strlen(path)] = '/';
            memcpy(&tbl_dir[strlen(path) + 1], db_f->d_name, strlen(db_f->d_name) + 1);
            
            

            struct dirent* tbl_f;
            DIR* subdir = opendir(tbl_dir);
            while ((tbl_f = readdir(subdir)) != NULL) {

                if (strcmp(tbl_f->d_name, ".") == 0 || strcmp(tbl_f->d_name, "..") == 0)
                    continue;
                
                char* col_path = malloc(strlen(tbl_dir) + 1 + strlen(tbl_f->d_name));
                memcpy(col_path, tbl_dir, strlen(tbl_dir));
                col_path[strlen(tbl_dir)] = '/';
                memcpy(&col_path[strlen(tbl_dir) + 1], tbl_f->d_name, strlen(tbl_f->d_name) + 1);
                int col_count = num_files(col_path);
                free(col_path);
                
                table* tbl1 = create_table(tbl_f->d_name, db1, col_count);
                tbl1->loaded = 0;
                tbl1->modified = 0;
                tbl1->active_cols = col_count;
            }
            closedir(subdir);
            free(tbl_dir);
        }
    }
    closedir(srcdir);
}
    

// load the given table's metadata and actual data into db1
void load_table_data(db* db1, table* tbl1) {
    pthread_mutex_lock(&tbl1->wlock);
    if (tbl1->loaded)
        return;

    char* path = "saved_dbs";
    char* tbl_dir = malloc(strlen(path) + strlen(db1->name) + strlen(tbl1->name) + 3);
    memcpy(tbl_dir, path, strlen(path));
    tbl_dir[strlen(path)] = '/';
    memcpy(&tbl_dir[strlen(path) + 1], db1->name, strlen(db1->name));
    tbl_dir[strlen(path) + strlen(db1->name) + 1] = '/';
    memcpy(&tbl_dir[strlen(path) + strlen(db1->name) + 2], tbl1->name, strlen(tbl1->name) + 1);

    struct dirent* tbl_f;
    DIR* subdir = opendir(tbl_dir);

    while ((tbl_f = readdir(subdir)) != NULL) {

        if( strcmp(tbl_f->d_name, ".") == 0 || strcmp(tbl_f->d_name, "..") == 0)
            continue;

        if (tbl1->length == INIT_COL_LEN) {
            struct stat st;
            fstatat(dirfd(subdir), tbl_f->d_name, &st, 0);
            tbl1->length = st.st_size / 2;
            tbl1->nstored = st.st_size / 4;
        }

        IndexType i = NONE;
        int col_index = atoi(&tbl_f->d_name[1]);
        if (tbl_f->d_name[0] == 'b')
            i = B_PLUS_TREE;
        else if (tbl_f->d_name[0] == 's')
            i = SORTED;
        else if (tbl_f->d_name[0] == 'c')
            tbl1->sorted_col = col_index;
        else if (tbl_f->d_name[0] == 'z') {
            tbl1->sorted_col = col_index;
            i = B_PLUS_TREE;
        }

        int name_start = 1;
        while (tbl_f->d_name[name_start] >= '0' && tbl_f->d_name[name_start] <= '9')
            name_start++;

        
        tbl1->columns[col_index] = malloc(sizeof(column));
        
        int len = strlen(&tbl_f->d_name[name_start]);
        char* name_cpy = malloc(len + 1);
        memcpy(name_cpy, &tbl_f->d_name[name_start], len + 1);

        tbl1->columns[col_index]->name = name_cpy;
        tbl1->columns[col_index]->data = malloc(sizeof(int) * tbl1->length);
        tbl1->columns[col_index]->index.type = i;
        tbl1->columns[col_index]->index.ordering = NULL;

        char* file_path = malloc(strlen(tbl_dir) + strlen(tbl_f->d_name) + 2);
        memcpy(file_path, tbl_dir, strlen(tbl_dir));
        file_path[strlen(tbl_dir)] = '/';
        memcpy(&file_path[strlen(tbl_dir) + 1], tbl_f->d_name, strlen(tbl_f->d_name) + 1);
        FILE* file = fopen(file_path, "rb");
        fread(tbl1->columns[col_index]->data, sizeof(int), tbl1->nstored, file);
        fclose(file);
        free(file_path);
        if (i == B_PLUS_TREE || i == SORTED)
            create_index(tbl1, tbl1->columns[col_index], i);
        
    }
    closedir(subdir);
    free(tbl_dir);
    tbl1->loaded = 1;
    pthread_mutex_unlock(&tbl1->wlock);
}

void flush_data(void) {
    char* path = "saved_dbs";
    for (int i = 0; i < db_list->index; i++) {
        db* db1 = db_list->list[i];
        char* db_dir = malloc(strlen(path) + strlen(db1->name) + 2);
        memcpy(db_dir, path, strlen(path));
        db_dir[strlen(path)] = '/';
        memcpy(&db_dir[strlen(path) + 1], db1->name, strlen(db1->name) + 1);

        struct stat st;
        if (stat(db_dir, &st) == -1) {
            mkdir(db_dir, 0777);
        }
        for (int j = 0; j < db1->table_count; j++) {
            table* tbl1 = db1->tables[j];
            reader_lock(tbl1);
            if (tbl1->modified) {
                char* tbl_dir = malloc(strlen(db_dir) + strlen(tbl1->name) + 2);
                memcpy(tbl_dir, db_dir, strlen(db_dir));
                tbl_dir[strlen(db_dir)] = '/';
                memcpy(&tbl_dir[strlen(db_dir) + 1], tbl1->name, strlen(tbl1->name) + 1);

                if (stat(tbl_dir, &st) == -1) {
                    mkdir(tbl_dir, 0777);
                }
                for (int c = 0; c < tbl1->col_count; c++) {
                    column* col1 = tbl1->columns[c];
                    char* file_name = malloc(strlen(tbl_dir) + strlen(col1->name) + 6);
                    int start = strlen(tbl_dir);
                    memcpy(file_name, tbl_dir, strlen(tbl_dir));
                    file_name[start++] = '/';
                    if (c == tbl1->sorted_col && col1->index.type == B_PLUS_TREE)
                        file_name[start++] = 'z';
                    else if (col1->index.type == B_PLUS_TREE)
                        file_name[start++] = 'b';
                    else if (col1->index.type == SORTED)
                        file_name[start++] = 's';
                    else if (c == tbl1->sorted_col)
                        file_name[start++] = 'c';
                    else
                        file_name[start++] = 'x';


                    start += sprintf(&file_name[start], "%d", c);
                    memcpy(&file_name[start], col1->name, strlen(col1->name) + 1);
                    FILE* f = fopen(file_name, "w+b");
                    fwrite(col1->data, sizeof(int), tbl1->nstored, f);
                    fclose(f);
                    free(file_name);
                }

                free(tbl_dir);
            }
            reader_unlock(tbl1);
        }
        free(db_dir);
    }
}

