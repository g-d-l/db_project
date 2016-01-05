#define _XOPEN_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <dirent.h>
#include <sys/stat.h>


#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 4096
#define BUF_SIZE 4096

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Ignore things such as new lines.
        // Otherwise, convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }
            len = recv(client_socket, &(recv_message), sizeof(message), 0);
            // Always wait for server response (even if it is just an OK message)
            if (1) {
                if (recv_message.status == PROVIDE_FILE) {
                    char buf[BUF_SIZE];
                    int file_name_len = recv_message.length;
                    send(client_socket, &(send_message), sizeof(message), 0);
                    recv(client_socket, buf, file_name_len, 0);
                    struct stat st;
                    stat(buf, &st);
                    int size = st.st_size;
                    send(client_socket, &size, sizeof(int), 0);
                    recv(client_socket, &(recv_message), sizeof(message), 0);

                    FILE* f = fopen(buf, "r");
                    int nread = fread(buf, 1, BUF_SIZE, f);
                    while (nread > 0) {
                        send_message.status = SENDING_FILE;
                        send_message.length = nread;
                        send_message.payload = NULL;
                        send(client_socket, &(send_message), sizeof(message), 0);
                        recv(client_socket, &(recv_message), sizeof(message), 0);
                        send(client_socket, buf, nread, 0);
                        recv(client_socket, &(recv_message), sizeof(message), 0);
                        nread = fread(buf, 1, BUF_SIZE, f);
                    }
                    send_message.status = FILE_SENT;
                    send_message.length = 0;
                    send_message.payload = read_buffer;
                    send(client_socket, &(send_message), sizeof(message), 0);
                    fclose(f);
                    recv(client_socket, &(recv_message), sizeof(message), 0);
                }
                if (recv_message.status == OK_WAIT_FOR_RESPONSE &&
                    (int) recv_message.length > 0) {
                    send(client_socket, &(send_message), sizeof(message), 0);
                    recv(client_socket, &(recv_message), sizeof(message), 0);
                    while (recv_message.length > 0) {
                        send(client_socket, &(send_message), sizeof(message), 0);
                        recv(client_socket, read_buffer, recv_message.length, 0);
                        read_buffer[recv_message.length] = '\0';
                        printf("%s", read_buffer);
                        send(client_socket, &(send_message), sizeof(message), 0);
                        recv(client_socket, &(recv_message), sizeof(message), 0);
                    }
                }
                else {
                    send(client_socket, &(send_message), sizeof(message), 0);
                    recv(client_socket, &(recv_message), sizeof(message), 0);
                }

            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
		            log_info("Server closed connection\n");
		        }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
