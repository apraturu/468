#include <time.h>
#include <stdio.h>
#include <string.h>
#include "tinyFS.h"
#include "bufferManager.h"
#include "bufferTest.h"

/* prints out the metadata of blocks in the buffer 
 * FORMAT: index#: [(fileDescriptor, pageID), timestamp, pinned, dirty flag]
 */
void checkpoint(Buffer *buf) {
   int i;
   char ctimeBuff[256];
   
   for(i = 0; i < buf->nBlocks; i++) {
      printf("%d:",i);
      
      if (buf->timestamp[i] < 0) {
         printf("[%s]\n", EMPTY);
      }
      else {
         strcpy(ctimeBuff, ctime(&(buf->timestamp[i])));
         ctimeBuff[strlen(ctimeBuff) - 1] = '\0';
      
         printf("[(%d,%d),", buf->pages[i].address.FD, buf->pages[i].address.pageId);
         printf("%s,%d,%d]\n", ctimeBuff, buf->pin[i], buf->dirty[i]); 
      }
   }
}

int pageDump(Buffer *buf, int index) {
   if (buf->timestamp[index] < 0) {
      printf("No valid block at index %d\n", index); 
      return -1;
   }
   else {
      printf("%s\n", buf->pages[index].block);
   }
   
   return 0;
}

int printPage(Buffer *buf, DiskAddress diskPage) {
   int i;
   
   /*Come back and figure out how to check if the diskPage is on disk*/
   /*if (tfs_numPages(diskPage.FD) < diskPage.pageId) {
      printf("Page not on disk\n");
      return -1;
   }*/
   
   if ((i = findPage(buf, diskPage)) < 0 ) {
      printf("Page not in the buffer\n");
      return -1;
   }
   else {
      pageDump(buf, i);
   }
   return 0;
}

int printBlock(Buffer *buf, DiskAddress diskPage) {
   unsigned char temp[BLOCKSIZE];
   
   tfs_readPage(diskPage.FD, (unsigned) diskPage.pageId, temp);
   
   printf("%s\n", temp);
   
   return 0;
}

int main(int argc, char **argv) {
   return 0;
}
