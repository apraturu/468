#include "bufferManager.h"



/* If the given disk page is in the buffer, returns its index in
 * the buffer's array. Otherwise, returns -1. */
static int findPage(Buffer *buf, DiskAddress diskPage) {
   int i;
   for (i = 0; i < buf->nBlocks; i++) {
      if (buf->pages[i].FD == diskPage && buf->pages[i].pageId == diskPage.pageId)
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
   int i = findPage(buf, diskPage);
   if (i < 0)
      return -1;

   tfs_writePage(diskPage->FD, diskPage->pageId, buffer->pages[i].block);
   buf->dirty[i] = 0;

   return 0;
}
