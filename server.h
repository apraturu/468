#ifndef __SERVER_H
#define __SERVER_H

#include <sys/socket.h>
#include <sys/select.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <unistd.h>

#define STDIN  0
#define STDOUT 1
#define STDERR 2

#define ASSERT(expression) {\
   if (!(expression)) {\
      perror("\n!!! ASSERT FAILED !!!\n\tError ");\
      fprintf(stderr, "\tFile : \"%s\"\n\tFunction : \"%s\"\n\t"\
            "Line : %d\n\n", __FILE__, __func__, __LINE__);\
      exit(1);\
   }\
}

class Server {
private:
   int port;            // Port to open server on
   int server_fd;       // FD assigned to server
   int max_sock_fd;     // The max sock_fd in use
   fd_set rdfds;        // Set of fds to select on
   sockaddr_in local;   // Local socket config
   struct timeval tv;   // Timeval for select

   // Clears the timeval struct used for timeout on the select call.
   void clear_timeval();

   // Clears the fd_set and populates it with all of the fds associated with
   // externally facing clients (as well as the server's fd).
   void config_fd_set();

   // Checks to see which clients are currently waiting for a response from
   // the server and acts on those requests.
   void handle_existing_clients();

   // Handles a new client connecting to the server.
   void handle_new_client();

public:
   // Sets up the server using the given port number.
   Server(int port);

   // Creates a TCP/IPv4 socket and associates it with the designated port
   // number. Returns the newly created socket number.
   bool setup_socket();

   // The server enters the state where it handles all client communications.
   void handle_client_traffic();
};

#endif
