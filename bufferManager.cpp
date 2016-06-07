#include <stdlib.h>
#include "bufferManager.h"
#include "bufferTest.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

/*
 * Mount FileSystem,
 * Initialize the buffer.
 */
int commence(char * database,
             Buffer * buf, 
             int nBufferBlocks, 
             int nCacheBlocks) {
   int num, exit_code = 0;

   if (access(database, F_OK) == -1) { /* fs does not exist */
      exit_code += tfs_mkfs(database, (nBufferBlocks * sizeof(Block)) + sizeof(Buffer));
   }
   /* mount the database */
   exit_code += tfs_mount(database);

   /* initialize buffer */
   buf->database = (char *)malloc(sizeof(strlen(database) * sizeof(char)));
   strcpy(buf->database, database);

   /* persistent slots */
   buf->nBufferBlocks = nBufferBlocks;
   buf->numBufferOccupied = 0;
   buf->pages = (Block *)malloc(sizeof(Block) * nBufferBlocks);
   buf->buffer_timestamp = (long *)malloc(sizeof(long) * nBufferBlocks);

   /* volatile slots */
   buf->nCacheBlocks = nCacheBlocks;
   buf->numCacheOccupied = 0;
   buf->cache = (Block *)malloc(sizeof(Block) * nCacheBlocks);
   buf->cache_timestamp = (long *)malloc(sizeof(long) * nCacheBlocks);

   /* other stuff */
   buf->pin = (char *)malloc(sizeof(char) * nBufferBlocks);
   buf->dirty = (char *)malloc(sizeof(char) * nBufferBlocks);
   
   for (num = 0; num < nBufferBlocks; num++) {
      /* set timestamp to -1, -1 means it's an empty slot */
      buf->buffer_timestamp[num] = -1;
      buf->pin[num] = 0;
      buf->dirty[num] = 0;
   }

   for (num = 0; num < nCacheBlocks; num++) {
      buf->cache_timestamp[num] = -1;
   }
   
   buf->numVolatileFiles = 0;
   buf->numPersistentFiles = 0;
   
   return exit_code;
}

int squash(Buffer * buf) {
   int num;
   for (num = 0; num < buf->nBufferBlocks; num++) {
      if (buf->pin[num] == 1) {
         unPinPage(buf, buf->pages[num].address);
      }
      if (buf->dirty[num] == 1) {
         flushPage(buf, buf->pages[num].address);
      }
   }

   free(buf->database);
   free(buf->pages);
   free(buf->cache);
   free(buf->buffer_timestamp);
   free(buf->cache_timestamp);
   free(buf->pin);
   free(buf->dirty);
   free(buf->volatileFDs);
   free(buf->persistentFDs);
   free(buf);

   return tfs_unmount();
}

int checkPersistentFiles(Buffer *buf, int FD) {
   for(int i = 0; i < buf->numPersistentFiles; i++) {
      if (buf->persistentFDs[i] == FD)
         return i;
   }
   return -1;
}

int checkVolatileFiles(Buffer *buf, int FD) {
   for(int i = 0; i < buf->numVolatileFiles; i++) {
      if (buf->volatileFDs[i] == FD)
         return i;
   }
   return -1;
}

/* returns the index of the first empty slot in buffer, or -1 if there isn't one. */
int findEmpty(Buffer *buf) {
   int i;
   for (i = 0; i < buf->nBufferBlocks; i++) {
      if (buf->buffer_timestamp[i] == -1) {
         return i;
      }
   }
   return -1;
}

/* returns the index in the buffer array */
int readPage(Buffer * buf, DiskAddress diskPage) {
   int num, available = 0, toEvict; /* available is set to 1 if there are any unpinned pages */
   long oldest = -1;
   /* check if this file has been opened before in persistent */
   if (checkPersistentFiles(buf, diskPage.FD) == -1) {
      buf->numPersistentFiles += 1;
      buf->persistentFDs = (int *)realloc(buf->persistentFDs, sizeof(int) * buf->numPersistentFiles);
      buf->persistentFDs[buf->numPersistentFiles-1] = diskPage.FD;
   }

   /* buffer check for page */
   for (num = 0; num < buf->nBufferBlocks; num++) {
      if (buf->buffer_timestamp[num] != -1 && buf->pages[num].address.FD == diskPage.FD && buf->pages[num].address.pageId == diskPage.pageId) { /* found page in buffer */
         buf->buffer_timestamp[num] = time(NULL); 
         return num;
      }
   }

   /* if this is reached, then the page is not in the buffer */
   num = findEmpty(buf);
   if (num != -1) {
      /* bring page to buffer */
      tfs_readPage(diskPage.FD, diskPage.pageId, 
                  (unsigned char *)buf->pages[num].block);

      /* sets page metadata */
      buf->pages[num].address = diskPage; 
      buf->buffer_timestamp[num] = time(NULL);         
      
      buf->numBufferOccupied++;
      return num;
   }
   
   /* 
    * Implement LRU eviction.
    * all pageslots are full, check if they're all pinned 
    */
   for (num = 0; num < buf->nBufferBlocks; num++) {
      if(buf->pin[num] == 0) { /* if the page is unpinned */
         if (oldest == -1) { /* initial timestamp */
            oldest = buf->buffer_timestamp[num];
            toEvict = num;
         }
         else if (oldest > buf->buffer_timestamp[num]) { /* found an older time stamp */
            oldest = buf->buffer_timestamp[num];
            toEvict = num;
         }
         available = 1;
      }
   }
   if (available == 0) { /* all the pages are pinned */
      return -1; /* error */
   }
   /* at this point a page needs to be evicted */
   flushPage(buf, buf->pages[toEvict].address);
   /* bring page to buffer */
   tfs_readPage(diskPage.FD, diskPage.pageId, (unsigned char *)buf->pages[toEvict].block);
   /* set other bits*/
   buf->pages[toEvict].address = diskPage;
   buf->buffer_timestamp[toEvict] = time(NULL); 
   
   return toEvict;
}

/* If the given disk page is in the buffer, returns its index in
 * the buffer's array. Otherwise, returns -1. */
int findPage(Buffer *buf, DiskAddress diskPage) {
   int i;
   for (i = 0; i < buf->nBufferBlocks; i++) {
      if (buf->buffer_timestamp[i] == -1)
         continue;
      if (buf->pages[i].address.FD == diskPage.FD
       && buf->pages[i].address.pageId == diskPage.pageId)
         break;
   }
   if (i == buf->nBufferBlocks)
      i = -1;
   return i;
}

/* If the given disk page is in the cache, returns its index in
 * the cache's array. Otherwise, returns -1. */
int findPageVolatile(Buffer *buf, DiskAddress diskPage) {
   int i;
   for (i = 0; i < buf->nCacheBlocks; i++) {
      if (buf->cache_timestamp[i] == -1)
         continue;
      if (buf->cache[i].address.FD == diskPage.FD
       && buf->cache[i].address.pageId == diskPage.pageId)
         break;
   }
   if (i == buf->nCacheBlocks)
      i = -1;
   return i;
}

// Have readPage return the index of the page in the buffer after reading it in,
// or -1 if error. This makes writePage really simple
// writePage and flushPage return 0 for good, -1 for error?
int writePage(Buffer *buf, DiskAddress diskPage) {
   int i = readPage(buf, diskPage);
   if (i < 0)
      return -1;

   buf->dirty[i] = 1;
   buf->buffer_timestamp[i] = time(NULL);

   return 0;
}

int flushPage(Buffer *buf, DiskAddress diskPage) {
   int i = findPage(buf, diskPage);
   if (i < 0)
      return -1;

   tfs_writePage(diskPage.FD, diskPage.pageId, (unsigned char *) buf->pages[i].block);
   buf->dirty[i] = 0;

   return 0;
}

// returns -1 if page not in buffer
static int setPin(Buffer *buf, DiskAddress diskPage, int val) {
   int i = findPage(buf, diskPage);
   if (i < 0)
      return -1;
   buf->pin[i] = val;
   return 0;
}

int pinPage(Buffer *buf, DiskAddress diskPage) {
   return setPin(buf, diskPage, 1);
}

int unPinPage(Buffer *buf, DiskAddress diskPage) {
   return setPin(buf, diskPage, 0);
}

int newPage(Buffer *buf, fileDescriptor FD, DiskAddress *diskPage) {
   // if everything is pinned, return -1
   int i;
   for (i = 0; i < buf->nBufferBlocks; i++) {
      if (!buf->pin[i])
         break;
   }
   if (i == buf->nBufferBlocks)
      return -1;

   diskPage->FD = FD;
   diskPage->pageId = tfs_numPages(FD);
   char *data = (char *)calloc(BLOCKSIZE, 1);
   tfs_writePage(FD, diskPage->pageId, (unsigned char *)data);
   free(data);

   return readPage(buf, *diskPage);
}

int allocateCachePage(Buffer *buf, DiskAddress diskpage){
       int i, bufIndex, oldestCache = 0, oldestBuf = -1;
       
       /* check if this file has been opened before in volatile */
   if (checkVolatileFiles(buf, diskpage.FD) == -1) {
      buf->numVolatileFiles += 1;
      buf->volatileFDs = (int *)realloc(buf->volatileFDs, sizeof(int) * buf->numVolatileFiles);
      buf->volatileFDs[buf->numVolatileFiles-1] = diskpage.FD;
   }

   /* check if this page is already in cache */
   i = findPageVolatile(buf, diskpage);
   if (i != -1)
      return i;

   /* check if this page is already in persistent buffer */
   bufIndex = findPage(buf, diskpage);
   
       
       //Check if cache is full
       if(buf->nCacheBlocks == buf->numCacheOccupied){
             
             //Find index of least recently used for eviction
             for(i = 0; i < buf->nCacheBlocks; i++){
                  if(buf->cache_timestamp[oldestCache] > buf->cache_timestamp[i]) {
                        oldestCache = i;
                  }
             }
             buf->cache_timestamp[oldestCache] = -1;
             
             //Check if buffer is full
             if(buf->nBufferBlocks == buf->numBufferOccupied){
                  //If it is full find the least recently used unpinned page and write to disk
                  for(i = 0; i < buf->nBufferBlocks; i++){
                       if(buf->pin[i] == 0 &&
                        (oldestBuf == -1 || buf->buffer_timestamp[oldestBuf] > buf->buffer_timestamp[i])) {
                              oldestBuf = i;
                       }
                  }
                  if (oldestBuf == -1) // all pages were pinned
                     return -1;
                  writePage(buf, buf->pages[oldestBuf].address);
                  flushPage(buf, buf->pages[oldestBuf].address);
             } else {
                  //If it is not full then find empty spot and insert into buffer
                  for(i = 0; i < buf->nBufferBlocks; i++){
                        if(buf->buffer_timestamp[i] == -1){
                              oldestBuf = i;
                              buf->numBufferOccupied++;
                              buf->buffer_timestamp[i] = time(NULL);
                              break;
                        }
                  }
             }
             
             //Copy the block from the cache into the buffer
             memcpy(&(buf->pages[oldestBuf]), &(buf->cache[oldestCache]), sizeof(Block));
             
             
             buf->pin[oldestBuf] = 1;
       }
       
       //Write the diskapage passed into the now open cache spot
       for(i = 0; i < buf->nCacheBlocks; i++){
             if(buf->cache_timestamp[i] == -1){
                   buf->cache[i].address.pageId = diskpage.pageId;
                   buf->cache[i].address.FD = diskpage.FD;
                   buf->numCacheOccupied++;
                   buf->cache_timestamp[i] = time(NULL); 

                   /* if page was already in persistent buffer, copy its data into this cache spot */
                   if (bufIndex != -1) {
                        memcpy(buf->cache[i].block, &buf->pages[bufIndex].block, BLOCKSIZE);

                        /* remove page from persistent buffer */
                        buf->buffer_timestamp[bufIndex] = -1;
                        buf->pin[bufIndex] = 0;
                        buf->numBufferOccupied--;
                   }

                   break;
             }
       }
       return i;
 }
 
int removeCachePage(Buffer *buf, DiskAddress diskPage) {
   int i;
   for (i = 0; i < buf->nCacheBlocks; i++) {
      Block temp = buf->cache[i];
      if (temp.address.FD == diskPage.FD && temp.address.pageId == diskPage.pageId) {
         buf->cache_timestamp[i] = -1;
         buf->numCacheOccupied--;
         return 0;
      }
   }

   for (i = 0; i < buf->nBufferBlocks; i++) {
      Block temp = buf->pages[i];
      if (temp.address.FD == diskPage.FD && temp.address.pageId == diskPage.pageId) {
         buf->buffer_timestamp[i] = -1;
         buf->pin[i] = 0;
         buf->numBufferOccupied--;
         return 0;
      }
   }

   return 1;
}

//int removeFileFromPersistentList(Buffer *buf, int fd) {
//   int i;
//
//   i = checkPersistentFiles(buf,fd);
//   if (i < 0)
//      return -1;
//
//   while(i < buf->numBufferOccupied + 1) {
//      buf->persistentFDs[i] = buf->persistentFDs[i+1];
//   }
//   buf->numBufferOccupied--;
//   realloc(buf->persistentFDs, sizeof(int) * numBufferOccupied);
//
//   return 0;
//}
//
//int removeFileFromVolatileList(Buffer *buf, int fd) {
//   int i;
//
//   i = checkVolatileFiles(buf,fd);
//   if (i < 0)
//      return -1;
//
//   while(i < buf->numCacheOccupied + 1) {
//      buf->volatileFDs[i] = buf->volatileFDs[i+1];
//   }
//   buf->numCacheOccupied--;
//   realloc(buf->persistentFDs, sizeof(int) * numBufferOccupied);
//
//   return 0;
//}

void deleteFile(Buffer *buf, int FD) {
   tfs_deleteFile(FD);
   for (int i = 0; i < buf->nBufferBlocks; i++) {
      if (buf->buffer_timestamp[i] != -1 && buf->pages[i].address.FD == FD) {
         buf->buffer_timestamp[i] = -1;
         buf->pin[i] = 0;
         buf->numBufferOccupied--;
      }
   }

   for (int i = 0; i < buf->nCacheBlocks; i++) {
      if (buf->cache_timestamp[i] != -1 && buf->pages[i].address.FD == FD) {
         buf->cache_timestamp[i] = -1;
         buf->numCacheOccupied--;
      }
   }
}