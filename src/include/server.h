#include <pthread.h>
#include "cs165_api.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define WORKER_THREADS 4
#define INIT_CLIENT_SLOTS 10
#define INT_MAX_WIDTH 11
#define INIT_LIST_SZ 16

extern pthread_mutex_t curr_idle_mutex;
extern pthread_cond_t  curr_idle_cond;
extern int curr_idle_count;

extern pthread_mutex_t worker_ready_mutex;
extern pthread_cond_t  worker_ready_cond;
extern int worker_ready;

extern pthread_cond_t  currently_working_cond;
extern pthread_mutex_t currently_working_mutex;
extern int currently_working_count;

extern pthread_mutex_t can_finish_mutex;
extern pthread_cond_t  can_finish_cond;
extern int can_finish_count;

// global from which the worker threads pull information their tasks
typedef struct worker_hub {
    // where worker threads read their input and write their results
    int* arrays1[WORKER_THREADS];
    int* arrays2[WORKER_THREADS];
    int* arrays3[WORKER_THREADS];
    int lens[WORKER_THREADS];
    long long results[WORKER_THREADS];

    int overflow;

    pthread_mutex_t queue_lock;
    select_job* queue;

    DSLGroup task;

    pthread_mutex_t hub_lock;

} worker_hub;


void free_query(db_operator* query);
