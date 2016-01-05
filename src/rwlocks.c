#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "cs165_api.h"
#include "rwlocks.h"


// https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock

void reader_lock(table* tbl1) {
	pthread_mutex_lock(&tbl1->rlock);
	tbl1->reader_count++;
	if (tbl1->reader_count == 1)
		pthread_mutex_lock(&tbl1->wlock);
	pthread_mutex_unlock(&tbl1->rlock);
}

void reader_unlock(table* tbl1) {
	pthread_mutex_lock(&tbl1->rlock);
	tbl1->reader_count--;
	if (tbl1->reader_count == 0)
		pthread_mutex_unlock(&tbl1->wlock);
	pthread_mutex_unlock(&tbl1->rlock);
}

void writer_lock(table* tbl1) {
	pthread_mutex_lock(&tbl1->wlock);
}

void writer_unlock(table* tbl1) {
	pthread_mutex_unlock(&tbl1->wlock);
}
