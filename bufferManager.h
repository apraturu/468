#include "tinyFS.h"
#include "libTinyFS.h"

#define MAX_BUFFER_SIZE 5

typedef struct {
   char[BLOCKSIZE] block;
   DiskAddress address;
} Block;

typedef struct {
   fileDescriptor FD;
   int pageId;
} DiskAddress;

typedef struct {
   char *database;
   int nBlocks;
   Block[MAX_BUFFER_SIZE] pages;
   long[MAX_BUFFER_SIZE] timestamp;
   char[MAX_BUFFER_SIZE] pin;
   char[MAX_BUFFER_SIZE] dirty;
   int numOccupied;
} Buffer;

int commence(char *databsae, Buffer *buf, int nBlocks);
int squash(Buffer *buf);
int readPage(Buffer *buf, DiskAddress diskPage);
int writePage(Buffer *buf, DiskAddress diskPage);
int flushPage(Buffer *buf, DiskAddress diskPage);
int pinPage(Buffer *buf, DiskAddress diskPage);
int unPinPage(Buffer *buf, DiskAddress diskPage);
int newPage(Buffer *buf, fileDescriptor FD, DiskAddress *diskPage);
