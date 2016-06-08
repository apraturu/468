#include <sstream>
#include "main.h"
#include "server.h"
#include "FLOPPY_statements/statements.h"
#include "heap.h"
#include "TupleIterator.h"

using namespace std;

Server::Server(int server_port) {
   port = server_port;
   server_fd = -1;
}

bool Server::setup_socket() {
   int result;

   // Create the socket
   server_fd = socket(AF_INET, SOCK_STREAM, 0);
   ASSERT(server_fd >= 0);

   // Configure the local sockaddr
   local.sin_family = AF_INET;                  // IPv4
   local.sin_addr.s_addr = htonl(INADDR_ANY);   // Match any IP
   local.sin_port = htons(port);                // Set server's port

   // Assign the current max_socket fd to server_fd.
   max_sock_fd = server_fd;

   // Bind local config to socket
   socklen_t sockaddr_in_size = sizeof(sockaddr_in);
   result = bind(server_fd, (struct sockaddr *)&local, sizeof(sockaddr_in));

   if (result < 0) {
      fprintf(stderr, "Port %d already taken, exiting\n", port);
      exit(1);
   }

   // Obtain port number for server that the OS chose.
   result = getsockname(server_fd, (struct sockaddr *)&local,
                        &sockaddr_in_size);
   ASSERT(result >= 0);

   port = ntohs(local.sin_port);

   fprintf(stderr, "Server is using port: %d\n", ntohs(local.sin_port));

   // Setup the socket to listen
   result = listen(server_fd, 5);
   ASSERT(result >= 0);

   return true;
}

void Server::handle_client_traffic() {
   while (1) {
      config_fd_set();
      clear_timeval();

      // See if anyone has sent us a message.
      int num_fds_available = select(max_sock_fd + 1, &rdfds, NULL, NULL, &tv);
      ASSERT(num_fds_available >= 0);

      // If someone sent us a message
      if (num_fds_available > 0) {

         // If they sent it directly to the server's port then this is a new
         // client so setup the new client
         if (FD_ISSET(server_fd, &rdfds)) {
            FD_CLR(server_fd, &rdfds);
            handle_new_client();
         }

         // See if any other existing clients are trying to talk to us and
         // handle their messages
         handle_existing_clients();
      }
   }
}

void Server::clear_timeval() {
   tv.tv_sec = 0;
   tv.tv_usec = 0;
}

void Server::config_fd_set() {
   int i;

   // Clear initial fd_set.
   FD_ZERO(&rdfds);

   // Add each existing client to the fd_set.
   for (i = STDERR + 1; i <= max_sock_fd; ++i) {
      FD_SET(i, &rdfds);
   }
}

void Server::handle_new_client() {
   fprintf(stderr, "New client connected!\n");
   sockaddr_in remote;
   socklen_t sockaddr_in_size = sizeof(sockaddr_in);

   // Accept the client's connection and get a new socket fd for them
   int new_sock = accept(server_fd, (struct sockaddr *)&remote, &sockaddr_in_size);
   ASSERT(new_sock >= 0);

   fprintf(stderr, "Client was assigned socket fd: %d\n", new_sock);

   // Update the max socket fd so that we don't exclude anyone
   max_sock_fd = new_sock > max_sock_fd ? new_sock : max_sock_fd;
}

void Server::handle_existing_clients() {
   char buf[256] = {0};

   for (int fd = STDERR + 1; fd <= max_sock_fd; ++fd) {
      // See if the client's fd is set by select (meaning we received data from
      // the client).
      if (FD_ISSET(fd, &rdfds)) {
         FD_CLR(fd, &rdfds);

         int bytes_recv = recv(fd, buf, 64, 0);

         // If received 0 bytes then the client hung up so we are done here for
         // this simple client-server example.
         if (bytes_recv == 0) {
            fprintf(stderr, "Client hung-up, exiting.\n");
            return;
         }
         fprintf(stderr, "Received %d bytes from client\n", bytes_recv);
         fprintf(stderr, "Client sent msg: \"%s\"\n", buf);

         bool shouldDelete;
         int result = runStatement(buf, &shouldDelete);
         if (result > 0) { // output csv
            RecordDesc recordDesc;

            heapHeaderGetRecordDesc(buffer, result, &recordDesc);

            stringstream s;
            for (int i = 0; i < recordDesc.numFields; i++) {
               if (i > 0)
                  s << ", ";
               s << recordDesc.fields[i].name;
            }
            s << '\n';

            TupleIterator iter(result);
            for (Record *record = iter.next(); record; record = iter.next()) {
               for (int i = 0; i < recordDesc.numFields; i++) {
                  if (i > 0)
                     s << ", ";

                  Field field = recordDesc.fields[i];

                  if (field.type == VARCHAR)
                     s << "'" << record->fields[field.name].sVal << "'";
                  else if (field.type == INT)
                     s << record->fields[field.name].iVal;
                  else if (field.type == BOOLEAN)
                     s << (int)record->fields[field.name].bVal;
                  else
                     s << record->fields[field.name].fVal;
               }
               s << '\n';

            }

            string str = s.str();
            send(fd, str.c_str(), str.length(), 0);
         }
         if (shouldDelete)
            deleteFile(buffer, result);
      }
   }
}

void print_usage() {
   fprintf(stderr, "\n");
   fprintf(stderr, "Usage: server [port]\n");
   fprintf(stderr, "       port - a positive integer.\n");
   fprintf(stderr, "       If port is 0, the OS defines the port.\n");
}

/*
int main(int argc, char **argv) {
   int port = (argc == 2) ? (extract_port(argv[1])) : 0;
   Server server(port);

   if (server.setup_socket()) {
      server.handle_client_traffic();
   }

   return 0;
}*/
