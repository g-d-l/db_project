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


extern select_buf* sbuf;

// insert a SELECT or SELECT_POS to the buffer
void insert_select(db_operator* s);

// returns a linked list of select_jobs
select_job* collect_selects(db_operator** list, int len);

void free_select(select_job* job);

void flush_selects(void);
