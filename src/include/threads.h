#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "cs165_api.h"
#include "utils.h"




void* worker_thread(void* arg);

void batch_select(select_job* s);

void fetch(int* array, int* positions, int len, int* result);

void find_min(int* array, int len, long long* result);

void find_min_pos(int* positions, int* values, int len, long long* result);

void find_max(int* array, int len, long long* result);

void find_max_pos(int* positions, int* values, int len, long long* result);

void sum(int* array, int len, long long* result);

void add(int* array1, int* array2, int len, long long* result);

void sub(int* array1, int* array2, int len, long long* result);

void use_threads(void);




void find_min_l(long long* array, int len, long long* result);

void find_min_pos_l(int* positions, long long* values, int len, long long* result);

void find_max_l(long long* array, int len, long long* result);

void find_max_pos_l(int* positions, long long* values, int len, long long* result);