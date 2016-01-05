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
#include "select_buf.h"
#include "server.h"
#include "threads.h"
#include "comparators.h"

extern worker_hub hub;


// insert a SELECT or SELECT_POS to the buffer
void insert_select(db_operator* s) {
	pthread_mutex_lock(&sbuf->buf_lock);
	if (sbuf->index == sbuf->capacity) {
		sbuf->capacity *= 2;
		sbuf->buf = realloc(sbuf->buf, sbuf->capacity * sizeof(db_operator*));
	}
	sbuf->buf[sbuf->index++] = s;
	pthread_mutex_unlock(&sbuf->buf_lock);
}


// returns a linked list of select_jobs
select_job* collect_selects(db_operator** list, int len) {
	qsort(list, len, sizeof(db_operator*), select_compare);
	int index = 0;
	select_job* head = NULL;
	while (index < len) {
		int i = index + 1;
		while (i < len && list[i]->db1 == list[index]->db1 && list[i]->table1 == list[index]->table1 &&
			list[i]->column1 == list[index]->column1) {
			i++;
		}
		int num = i - index;
		select_job* new_job = malloc(sizeof(select_job));
		new_job->db1 = list[index]->db1;
		new_job->table1 = list[index]->table1;
		new_job->column1 = list[index]->column1;
		new_job->mins = malloc(sizeof(int) * num);
		new_job->maxs = malloc(sizeof(int) * num);
		new_job->outputs = malloc(sizeof(char*) * num);
		new_job->positions = malloc(sizeof(var*) * num);
		new_job->groups = malloc(sizeof(var_group*) * num);
		new_job->next = head;
		head = new_job;
		for (int j = 0; j < num; j++) {
			new_job->mins[j] = list[index]->min;
			new_job->maxs[j] = list[index]->max;
			new_job->outputs[j] = list[index]->output_name1;
			new_job->positions[j] = list[index]->var1;
			new_job->groups[j] = list[index]->group;
			index++;
		}
		new_job->num = num;
	}
	return head;
}

// free a select job
void free_select(select_job* job) {
	free(job->mins);
	free(job->maxs);
	free(job->outputs);
	free(job->positions);
	free(job->groups);
	free(job);
}

// order the jobs by column, wake up the worker threads to flush, and free the jobs
void flush_selects(void) {
	select_job* job_list = collect_selects(sbuf->buf, sbuf->index);

    
	hub.task = SELECT_FLUSH;
    hub.queue = job_list;

	use_threads();

	for (int i = 0; i < sbuf->index; i++)
		free_query(sbuf->buf[i]);
	sbuf->index = 0;
}


