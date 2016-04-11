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
int commence(char * database, Buffer * buf, int nBlocks) {
   int num, exit_code = 0;

   if (access(database, F_OK) == -1) { /* fs does not exist */
      exit_code += tfs_mkfs(database, (nBlocks * sizeof(Block)) + sizeof(Buffer));
   }

   exit_code += tfs_mount(database);

   buf->database = malloc(sizeof(strlen(database) * sizeof(char)));
   strcpy(buf->database, database);
   buf->nBlocks = nBlocks;
   buf->numOccupied = 0; // temporary?

   for (num = 0; num < nBlocks; num++) {
      /* set timestamp to -1, -1 means it's not in buffer */
      buf->timestamp[num] = -1;
      buf->pin[num] = 0;
      buf->dirty[num] = 0;
   }
   
   return exit_code;
}

int squash(Buffer * buf) {
   int num;
   for (num = 0; num < MAX_BUFFER_SIZE; num++) {
      if (buf->pin[num] == 1) {
         unPinPage(buf, buf->pages[num].address); 
      }
      if (buf->dirty[num] == 1) {
         flushPage(buf, buf->pages[num].address);
      }
   }

   tfs_unmount();

   free(buf->database);
   free(buf);

   return 0;
}

/* returns the index in the buffer array */
int readPage(Buffer * buf, DiskAddress diskPage) {
   int num, available = 0, toEvict; /* available is set to 1 if there are any unpinned pages */
   long oldest = -1;

   /* buffer check for page */
   for (num = 0; num < MAX_BUFFER_SIZE; num++) {
      if (buf->timestamp[num] != -1 && buf->pages[num].address.FD == diskPage.FD && buf->pages[num].address.pageId == diskPage.pageId) { /* found page in buffer */
         printf("DEBUG: found page %d in buffer\n", diskPage.pageId);
         buf->timestamp[num] = time(NULL); 
         return num;
      }
   }
   /* if this is reached, then the page is not in the buffer. eviction time */
   if (buf->numOccupied < buf->nBlocks) {
      /* bring page to buffer */
      tfs_readPage(diskPage.FD, diskPage.pageId, 
                  (unsigned char *)buf->pages[buf->numOccupied].block);
                  
      /* sets page metadata */
      buf->pages[buf->numOccupied].address = diskPage; 
      buf->timestamp[buf->numOccupied] = time(NULL);         
      
      return buf->numOccupied++;
   }
   /* all pageslots are full, check if they're all pinned */
   for (int num = 0; num < MAX_BUFFER_SIZE; num++) {
      if(buf->pin[num] == 0) { /* if the page is unpinned */
         if (oldest == -1) { /* initial timestamp */
            oldest = buf->timestamp[num];
            toEvict = num;
         }
         else if (oldest > buf->timestamp[num]) { /* found an older time stamp */
            oldest = buf->timestamp[num];
            toEvict = num;
         }
         available = 1;
      }
   }
   if (available == 0) { /* all the pages are pinned */
      return -1;
   }
   /* at this point a page needs to be evicted */
   flushPage(buf, buf->pages[toEvict].address);
   /* bring page to buffer */
   tfs_readPage(diskPage.FD, diskPage.pageId, (unsigned char *)buf->pages[toEvict].block);
   /* set other bits*/
   buf->pages[toEvict].address = diskPage;
   buf->timestamp[toEvict] = time(NULL); 
   return toEvict;
}

/* If the given disk page is in the buffer, returns its index in
 * the buffer's array. Otherwise, returns -1. */
int findPage(Buffer *buf, DiskAddress diskPage) {
   int i;
   for (i = 0; i < buf->nBlocks; i++) {
      if (buf->timestamp[i] == -1)
         continue;
      if (buf->pages[i].address.FD == diskPage.FD
       && buf->pages[i].address.pageId == diskPage.pageId)
         break;
   }
   if (i == buf->nBlocks)
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

   return 0;
}

int flushPage(Buffer *buf, DiskAddress diskPage) {
   printf("DEBUG: flushing page %d\n", diskPage.pageId);
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
   printf("DEBUG: unpinning page %d\n", diskPage.pageId);
   return setPin(buf, diskPage, 0);
}

int newPage(Buffer *buf, fileDescriptor FD, DiskAddress *diskPage) {
   // if everything is pinned, return -1
   int i;
   for (i = 0; i < buf->nBlocks; i++) {
      if (!buf->pin[i])
         break;
   }
   if (i == buf->nBlocks)
      return -1;

   diskPage->FD = FD;
   diskPage->pageId = tfs_numPages(FD);
   char *data = calloc(BLOCKSIZE, 1);
   tfs_writePage(FD, diskPage->pageId, (unsigned char *)data);
   free(data);

   return readPage(buf, *diskPage);
}

int main() {return 0;}
