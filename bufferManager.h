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
   int nBufferBlocks;
   int nCacheBlocks;
   Block * pages;
   Block * cache;
   long * buffer_timestamp;
   long * cache_timestamp;
   char * pin;
   char * dirty;
   int numBufferOccupied;
   int numCacheOccupied;
   int *volatileFDs;
   int numVolatileFiles;
   int *persistentFDs;
   int numPersistentFiles;
} Buffer;

int commence(char *database, Buffer *buf, int nBufferBlocks, int nCacheBlocks);
int squash(Buffer *buf);
int readPage(Buffer *buf, DiskAddress diskPage);
int writePage(Buffer *buf, DiskAddress diskPage);
int flushPage(Buffer *buf, DiskAddress diskPage);
int pinPage(Buffer *buf, DiskAddress diskPage);
int unPinPage(Buffer *buf, DiskAddress diskPage);
int newPage(Buffer *buf, fileDescriptor FD, DiskAddress *diskPage);
int findPage(Buffer *buf, DiskAddress diskPage);
int findPageVolatile(Buffer *buf, DiskAddress diskPage);
int allocateCachePage(Buffer *buf, DiskAddress diskpage);
int removeCachePage(Buffer *buf, DiskAddress diskPage);
int checkPersistentFiles(Buffer *buf, int FD);
int checkVolatileFiles(Buffer *buf, int FD);