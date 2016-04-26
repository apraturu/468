/** 
 * Runs a test program in batch mode for the buffer manager.
 */
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
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
   
   for(i = 0; i < buf->nBufferBlocks; i++) {
      printf("%d:",i);
      
      if (buf->buffer_timestamp[i] < 0) {
         printf("[%s]\n", EMPTY);
      }
      else {
         strcpy(ctimeBuff, ctime(&(buf->buffer_timestamp[i])));
         ctimeBuff[strlen(ctimeBuff) - 1] = '\0';
      
         printf("[(%d,%d),", buf->pages[i].address.FD, buf->pages[i].address.pageId);
         printf("%s,%d,%d]\n", ctimeBuff, buf->pin[i], buf->dirty[i]); 
      }
   }
}

int pageDump(Buffer *buf, int index) {
   if (buf->buffer_timestamp[index] < 0) {
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
if (argc < 2) {
      perror("usage: ./bufferTest <filename>");
      exit(1);
   }
   char file_name[25];
   strcpy(file_name, argv[1]);

   printf("%s\n", file_name);
   FILE *fp = fopen(file_name, "r"); // open test file in read mode

   if (fp == NULL) {
      perror("error while opening file\n");
      exit(1);
   }

   Buffer *buf = malloc(sizeof(Buffer));
   char *buffer = malloc(15);
   char x[25];
   char *ptr;
   int ret;
   int ret2;
   int i;
   fileDescriptor fd;
   DiskAddress *diskPage = malloc(10 * sizeof(DiskAddress));
   DiskAddress temp;
   int da_ct = 0;

   while (fscanf(fp, "%s", buffer) != EOF) {

      /* switch statement for process */
      if (strcmp(buffer, "start") == 0) {
         fscanf(fp, "%s", buffer);
         strcpy(x, buffer);
         fscanf(fp, "%s", buffer);

         ret = (int)strtol(buffer, &ptr, 10);
         commence(x, buf, ret, ret);
      } 
      else if (strcmp(buffer, "end") == 0) {
         squash(buf);
         exit(0);
      }
      else if (strcmp(buffer,"read") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         temp.FD = fd;
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         temp.pageId = ret;
         readPage(buf, temp);
      }
      else if (strcmp(buffer,"write") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         temp.FD = fd;
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         temp.pageId = ret;
         writePage(buf, temp);
      }
      else if (strcmp(buffer,"flush") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         temp.FD = fd;
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         temp.pageId = ret;
         flushPage(buf, temp);
      }
      else if (strcmp(buffer,"pin") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         temp.FD = fd;
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         temp.pageId = ret;
         pinPage(buf, temp);
      }
      else if (strcmp(buffer,"unpin") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         temp.FD = fd;
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         temp.pageId = ret;
         unPinPage(buf, temp);
      }
      else if (strcmp(buffer, "new") == 0) {
         fscanf(fp, "%s", buffer); /*filename*/
         strcpy(x, buffer);
         fd = tfs_openFile(x);
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         fscanf(fp, "%s", buffer);
         ret2 = (int)strtol(buffer, &ptr, 10);

         for (i = ret; i <= ret2; i++) {
            tfs_writePage( fd, i, (unsigned char *)"Hello World!");
         }
         newPage(buf, fd, &diskPage[da_ct++]);
      }
      else if (strcmp(buffer, "check") == 0) {
         checkpoint(buf);
      }
      else /* default: */ {
         printf("invalid operation\n");
      }
   }
   return 0;
}
