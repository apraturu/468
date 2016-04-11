#include <time.h>
#include <stdio.h>
#include <string.h>
#include "tinyFS.h"
#include "bufferManager.h"
#include "bufferTest.h"

/* prints out the metadata of blocks in the buffer 
 * FORMAT: [(fileDescriptor, pageID), timestamp, pinned, dirty flag]
 */
void checkpoint(Buffer *buf) {
   int i;
   
   for(i = 0; i < buf->nBlocks; i++) {
      pageDump(buf, i);
   }
}

int pageDump(Buffer *buf, int index) {
   char ctimeBuff[256];

   printf("%d:",index);
   if (buf->timestamp[index] < 0) {
      printf("[%s]\n", EMPTY);
   }
   else {
      strcpy(ctimeBuff, ctime(&(buf->timestamp[index])));
      ctimeBuff[strlen(ctimeBuff) -1] = '\0';
      
      printf("[(%d,%d),", buf->pages[index].address.FD, buf->pages[index].address.pageId);
      printf("%s,%d,%d]\n", ctimeBuff, buf->pin[index], buf->dirty[index]); 
   }
   
   return 0;
}

int printPage(Buffer *buf, DiskAddress diskPage) {
   int i;
   if ((i = findPage(buf, diskPage)) < 0 ) {
      printf("Page not in the buffer");
      return -1;
   }
   else {
      pageDump(buf, i);
   }
   return 0;
}

int printBlock(Buffer *buf, DiskAddress diskPage) {
   return 0;
}
