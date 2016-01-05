#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "cs165_api.h"


// https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock

void reader_lock(table* tbl1);

void reader_unlock(table* tbl1);

void writer_lock(table* tbl1);

void writer_unlock(table* tbl1);
