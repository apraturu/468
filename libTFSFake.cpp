#include <fcntl.h>
#include <unistd.h>
#include <map>
#include <string>
#include "libTinyFS.h"
#include "tinyFS.h"

using namespace std;

static map<int, string> fdToName;
static map<string, int> nameToFd;

int tfs_mkfs(char *filename, int nBytes) {
   return 0;
}

int tfs_mount(char *filename) {
   return 0;
}

int tfs_unmount(void) {
   return 0;
}

fileDescriptor tfs_openFile(char *name) {
   int fd = open(name, O_RDWR | O_CREAT, 0666);
   //printf("tfs_openFile, opening %d, %s\n", fd, name);
   if (fd < 0)
      perror("tfs_openFile");
   fdToName[fd] = name;
   nameToFd[name] = fd;

   return fd;
}

int tfs_closeFile(fileDescriptor FD) {
   nameToFd.erase(fdToName[FD]);
   fdToName.erase(FD);
   return close(FD);
}

int tfs_writePage(fileDescriptor FD, unsigned int page, unsigned char *data) {
   lseek(FD, page * BLOCKSIZE, SEEK_SET);
   write(FD, data, BLOCKSIZE);
   return 0;
}

int tfs_readPage(fileDescriptor FD, unsigned int page, unsigned char *data) {
   lseek(FD, page * BLOCKSIZE, SEEK_SET);
   read(FD, data, BLOCKSIZE);
   return 0;
}

int tfs_numPages(fileDescriptor FD) {
   struct stat st;
   fstat(FD, &st);
   return st.st_size / BLOCKSIZE;
}

int tfs_deleteFile(fileDescriptor FD) {
   string name = fdToName[FD];
   tfs_closeFile(FD);
   if (remove(name.c_str()) != 0)
      perror("tfs_deleteFile, remove");
   return 0;
}

int getFd(string name) {
   if (!nameToFd.count(name))
      return tfs_openFile((char *)name.c_str());

   return nameToFd[name];
}