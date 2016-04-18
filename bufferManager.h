#include "tinyFS.h"
#include "libTinyFS.h"

#define MAX_BUFFER_SIZE 5

typedef struct {
   fileDescriptor FD;
   int pageId;
} DiskAddress;

typedef struct {
   char block[BLOCKSIZE];
   DiskAddress address;
} Block;

typedef struct {
   char *database;
   int nBlocks;
   Block pages[MAX_BUFFER_SIZE];
   long timestamp[MAX_BUFFER_SIZE];
   char pin[MAX_BUFFER_SIZE];
   char dirty[MAX_BUFFER_SIZE];
   int numOccupied;
} Buffer;

int commence(char *database, Buffer *buf, int nBlocks);
int squash(Buffer *buf);
int readPage(Buffer *buf, DiskAddress diskPage);
int writePage(Buffer *buf, DiskAddress diskPage);
int flushPage(Buffer *buf, DiskAddress diskPage);
int pinPage(Buffer *buf, DiskAddress diskPage);
int unPinPage(Buffer *buf, DiskAddress diskPage);
int newPage(Buffer *buf, fileDescriptor FD, DiskAddress *diskPage);
int findPage(Buffer *buf, DiskAddress diskPage);
