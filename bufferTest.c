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
   char *ptr, *ptr2, *token;
   int ret;
   int ret2;
   DiskAddress diskPage;
   while (fscanf(fp, "%s", buffer) != EOF) {

      /* switch statement for process */
      if (strcmp(buffer, "start") == 0) {
         fscanf(fp, "%s", buffer);
         strcpy(x, buffer);
         fscanf(fp, "%s", buffer);

         ret = (int)strtol(buffer, &ptr, 10);
         commence(x, buf, ret);
      } 
      else if (strcmp(buffer, "end") == 0) {
         squash(buf);
         exit(0);
      }
      else if (strcmp(buffer, "read") == 0) {
         fscanf(fp, "%s", buffer);
         if (buffer != NULL) {
           ptr = buffer;

            token = strsep(&buffer, ",");
            printf("%s\n", token);
            ret = (int)strtol(token, &ptr2, 10);
            token = strsep(&buffer, ",");
            ret2 = (int)strtol(token, &ptr2, 10);

           free(ptr);
         }
         diskPage.FD = ret;
         diskPage.pageId = ret2;
         readPage(buf, diskPage);
      }
      else if (strcmp(buffer, "write") == 0) {
         fscanf(fp, "%s", buffer);
         if (buffer != NULL) {
           ptr = buffer;

            token = strsep(&buffer, ",");
            printf("%s\n", token);
            ret = (int)strtol(token, &ptr2, 10);
            token = strsep(&buffer, ",");
            ret2 = (int)strtol(token, &ptr2, 10);

           free(ptr);
         }
         diskPage.FD = ret;
         diskPage.pageId = ret2;
         writePage(buf, diskPage);
      }
      else if (strcmp(buffer, "flush") == 0) {
         fscanf(fp, "%s", buffer);
         if (buffer != NULL) {
           ptr = buffer;

            token = strsep(&buffer, ",");
            printf("%s\n", token);
            ret = (int)strtol(token, &ptr2, 10);
            token = strsep(&buffer, ",");
            ret2 = (int)strtol(token, &ptr2, 10);

           free(ptr);
         }
         diskPage.FD = ret;
         diskPage.pageId = ret2;
         flushPage(buf, diskPage);
      }
      else if (strcmp(buffer, "pin") == 0) {
         fscanf(fp, "%s", buffer);
         if (buffer != NULL) {
           ptr = buffer;

            token = strsep(&buffer, ",");
            printf("%s\n", token);
            ret = (int)strtol(token, &ptr2, 10);
            token = strsep(&buffer, ",");
            ret2 = (int)strtol(token, &ptr2, 10);

           free(ptr);
         }
         diskPage.FD = ret;
         diskPage.pageId = ret2;
         pinPage(buf, diskPage);
      }
      else if (strcmp(buffer, "unpin") == 0) {
         fscanf(fp, "%s", buffer);
         if (buffer != NULL) {
           ptr = buffer;

            token = strsep(&buffer, ",");
            printf("%s\n", token);
            ret = (int)strtol(token, &ptr2, 10);
            token = strsep(&buffer, ",");
            ret2 = (int)strtol(token, &ptr2, 10);

           free(ptr);
         }
         diskPage.FD = ret;
         diskPage.pageId = ret2;
         unPinPage(buf, diskPage);
      }
      else if (strcmp(buffer, "new") == 0) {
         fscanf(fp, "%s", buffer);
         ret = (int)strtol(buffer, &ptr, 10);
         int i;
         for (i = 1; i <= ret; ++i)
         {
            newPage(buf, ret, &diskPage);
         }
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
