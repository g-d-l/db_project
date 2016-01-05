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
#include "message.h"
#include "parser.h"
#include "utils.h"
#include "bin_search.h"
#include "server.h"
#include "threads.h"
#include "rwlocks.h"
#include "comparators.h"
#include "bptree.h"
#include "select_buf.h"

extern worker_hub hub;
extern pthread_mutex_t shutdown_lock;
extern int shutdown_flag;


void* worker_thread(void* arg) {
	int id = (int) arg;
	while (1) {
		
		// Set self as idle and signal to master
		pthread_mutex_lock(&curr_idle_mutex);
		curr_idle_count++;
		pthread_cond_signal(&curr_idle_cond);
		pthread_mutex_unlock(&curr_idle_mutex);

		// wait for work from master
		pthread_mutex_lock(&worker_ready_mutex);
		while (!worker_ready)
			pthread_cond_wait(&worker_ready_cond , &worker_ready_mutex);
		
		pthread_mutex_unlock(&worker_ready_mutex);

		// execute work

		if (hub.task == FETCH) {
			fetch(hub.arrays1[id], hub.arrays2[id], hub.lens[id], hub.arrays3[id]);
		}
		else if (hub.task == MIN_VAL) {
			if (hub.overflow)
				find_min_l((long long*) hub.arrays1[id], hub.lens[id], &hub.results[id]);
			else
				find_min(hub.arrays1[id], hub.lens[id], &hub.results[id]);
		}
		else if (hub.task == MIN_POS_VAL) {
			if (hub.overflow)
				find_min_pos_l(hub.arrays1[id], (long long*) hub.arrays2[id], hub.lens[id], &hub.results[id]);
			else
				find_min_pos(hub.arrays1[id], hub.arrays2[id], hub.lens[id], &hub.results[id]);
		}
		else if (hub.task == MAX_VAL) {
			if (hub.overflow)
				find_max_l((long long*) hub.arrays1[id], hub.lens[id], &hub.results[id]);
			else
				find_max(hub.arrays1[id], hub.lens[id], &hub.results[id]);
		}
		else if (hub.task == MAX_POS_VAL) {
			if (hub.overflow)
				find_max_pos_l(hub.arrays1[id], (long long*) hub.arrays2[id], hub.lens[id], &hub.results[id]);
			else
				find_max_pos(hub.arrays1[id], hub.arrays2[id], hub.lens[id], &hub.results[id]);
		}
		else if (hub.task == AVG_VAL) {
			sum(hub.arrays1[id], hub.lens[id], &hub.results[id]);
		}
		else if (hub.task == ADD) {
			add(hub.arrays1[id], hub.arrays2[id], hub.lens[id], (long long*) hub.arrays3[id]);
		}
		else if (hub.task == SUB) {
			sub(hub.arrays1[id], hub.arrays2[id], hub.lens[id], (long long*) hub.arrays3[id]);
		}
		else if (hub.task == SELECT_FLUSH) {
			while (1) {
				pthread_mutex_lock(&hub.queue_lock);
				if (!hub.queue) {
					pthread_mutex_unlock(&hub.queue_lock);
					break;
				}
				else {
					select_job* job = hub.queue;
					hub.queue = hub.queue->next;
					pthread_mutex_unlock(&hub.queue_lock);
					batch_select(job);
					free_select(job);
				}
			}
		}
		else if (hub.task == SHUTDOWN) {
			pthread_mutex_lock(&shutdown_lock);
			shutdown_flag = 1;
			pthread_mutex_unlock(&shutdown_lock);
		}

		// mark self as finished and signal to master
		pthread_mutex_lock(&currently_working_mutex);
		currently_working_count--;
		pthread_cond_signal(&currently_working_cond);
		pthread_mutex_unlock(&currently_working_mutex);

		// wait for permission to finish
		pthread_mutex_lock(&can_finish_mutex);
		while (!can_finish_count)
			pthread_cond_wait(&can_finish_cond , &can_finish_mutex);

		pthread_mutex_unlock(&can_finish_mutex);

	}
	return NULL;
}



void batch_select(select_job* s) {
	reader_lock(s->table1);
	var** results = malloc(sizeof(var*) * s->num);
	for (int i = 0; i < s->num; i++) {
		results[i] = malloc(sizeof(var));
		results[i]->use_overflow = 0;
		results[i]->is_float = 0;
		int name_len = strlen(s->outputs[i]);
		results[i]->name = malloc(name_len + 1);
		memcpy(results[i]->name, s->outputs[i], name_len + 1);
		results[i]->vals = malloc(sizeof(int) * s->table1->nstored);
		results[i]->len = 0;

		insert_res(results[i], s->groups[i]);
		if (s->positions[i])
			qsort(s->positions[i]->vals, s->positions[i]->len, sizeof(int), int_compare);
	}
	int* position_indices = calloc(s->num, sizeof(int));
	// check if we can use the B+ tree for those with no given position variables
	if (s->column1->index.type == B_PLUS_TREE) {
		for (int i = 0; i < s->num; i++) {
			if (!s->positions[i]) {
				results[i]->len = find_range((node*) s->column1->index.ordering, 
									s->mins[i], s->maxs[i], NULL, (void*) results[i]->vals);
			}
		}
	}
	else {
		// check if we can use the fact that the column is sorted for those with no position variables
		int* sorted = NULL;
		int i = 0;
		while (s->table1->columns[i] != s->column1)
			i++;
		if (s->table1->sorted_col == i)
			sorted = s->column1->data;
		if (sorted) {
			for (int i = 0; i < s->num; i++) {
				if (!s->positions[i]) {
					int low = bin_search_low(sorted, s->table1->nstored, s->mins[i]);
					int high = bin_search_high(sorted, s->table1->nstored, s->maxs[i]);
					if (high == s->table1->nstored)
						high--;
					else {
						while (sorted[high] > s->maxs[i])
							high--;
					}
					results[i]->len = high - low + 1;
					for (int j = low; j < results[i]->len + low; j++)
						results[i]->vals[j - low] = j;
				}
			}
			// those with explicit positions need to be checked as such
			for (int i = 0; i < s->num; i++) {
				if (s->positions[i]) {
					for (int j = 0; j < s->positions[i]->len; j++) {
						int pos = s->positions[i]->vals[j];
						if (sorted[pos] >= s->mins[i] && sorted[pos] <= s->maxs[i])
							results[i]->vals[results[i]->len++] = pos;
					}
				}
			}

		}
		else {
			// no indexing available to use
			for (int i = 0; i < s->table1->nstored; i++) {
				for (int j = 0; j < s->num; j++) {
					if (s->positions[j] && position_indices[j] < s->positions[j]->len && i == s->positions[j]->vals[position_indices[j]]) {
						if (s->column1->data[i] >= s->mins[j] && s->column1->data[i] <= s->maxs[j])
							results[j]->vals[results[j]->len++] = i;
						position_indices[j]++;
					}
					else if (!s->positions[j] && s->column1->data[i] >= s->mins[j] && s->column1->data[i] <= s->maxs[j])
						results[j]->vals[results[j]->len++] = i;
				}
			}
		}
	}
	free(position_indices);
	free(results);
	reader_unlock(s->table1);
}



void fetch(int* array, int* positions, int len, int* result) {
	for (int i = 0; i < len; i++)
		result[i] = array[positions[i]];
}

void find_min(int* array, int len, long long* result) {
	if (len == 0)
		return;
	int temp = array[0];
	for (int i = 1; i < len; i++) {
		if (array[i] < temp)
			temp = array[i];
	}
	*result = temp;
}

void find_min_pos(int* positions, int* values, int len, long long* result) {
	if (len == 0)
		return;
	int temp = values[positions[0]];
	for (int i = 1; i < len; i++) {
		if (values[positions[i]] < temp)
			temp = values[positions[i]];
	}
	*result = temp;
}

void find_max(int* array, int len, long long* result) {
	if (len == 0)
		return;
	int temp = array[0];
	for (int i = 1; i < len; i++) {
		if (array[i] > temp)
			temp = array[i];
	}
	*result = temp;
}

void find_max_pos(int* positions, int* values, int len, long long* result) {
	if (len == 0)
		return;
	int temp = values[positions[0]];
	for (int i = 1; i < len; i++) {
		if (values[positions[i]] > temp)
			temp = values[positions[i]];
	}
	*result = temp;
}

void sum(int* array, int len, long long* result) {
	long long temp = 0;
	for (int i = 0; i < len; i++)
		temp += array[i];
	*result = temp;
}

void add(int* array1, int* array2, int len, long long* result) {
	for (int i = 0; i < len; i++)
		result[i] = (long long) array1[i] + (long long) array2[i];
}

void sub(int* array1, int* array2, int len, long long* result) {
	for (int i = 0; i < len; i++)
		result[i] = (long long) array1[i] - (long long) array2[i];
}



void use_threads(void) {
	pthread_mutex_lock(&curr_idle_mutex);

	while (curr_idle_count != WORKER_THREADS) {
	    pthread_cond_wait(&curr_idle_cond, &curr_idle_mutex);
	}
	pthread_mutex_unlock(&curr_idle_mutex);

	can_finish_count = 0;
	// Reset the number of currently_working_count threads
	currently_working_count = WORKER_THREADS;

	// signal workers to start
	pthread_mutex_lock(&worker_ready_mutex);
	worker_ready = 1;
	pthread_cond_broadcast(&worker_ready_cond );
	pthread_mutex_unlock(&worker_ready_mutex);      

	// Wait for workers to finish
	pthread_mutex_lock(&currently_working_mutex);
	while (currently_working_count != 0) {
	    pthread_cond_wait(&currently_working_cond, &currently_working_mutex);
	}
	pthread_mutex_unlock(&currently_working_mutex);

	//  workers are now waiting for permission to finish
	worker_ready = 0;
	curr_idle_count = 0;

	// allow them to finish
	pthread_mutex_lock(&can_finish_mutex);
	can_finish_count = 1;
	pthread_cond_broadcast(&can_finish_cond);
	pthread_mutex_unlock(&can_finish_mutex); 
}





void find_min_l(long long* array, int len, long long* result) {
	if (len == 0)
		return;
	long long temp = array[0];
	for (int i = 1; i < len; i++) {
		if (array[i] < temp)
			temp = array[i];
	}
	*result = temp;
}

void find_min_pos_l(int* positions, long long* values, int len, long long* result) {
	if (len == 0)
		return;
	long long temp = values[positions[0]];
	for (int i = 1; i < len; i++) {
		if (values[positions[i]] < temp)
			temp = values[positions[i]];
	}
	*result = temp;
}

void find_max_l(long long* array, int len, long long* result) {
	if (len == 0)
		return;
	long long temp = array[0];
	for (int i = 1; i < len; i++) {
		if (array[i] > temp)
			temp = array[i];
	}
	*result = temp;
}

void find_max_pos_l(int* positions, long long* values, int len, long long* result) {
	if (len == 0)
		return;
	long long temp = values[positions[0]];
	for (int i = 1; i < len; i++) {
		if (values[positions[i]] > temp)
			temp = values[positions[i]];
	}
	*result = temp;
}