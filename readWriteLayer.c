#include <stdlib.h>
#include <string.h>
#include "bufferManager.h"
#include "readWriteLayer.h"
#include "tinyFS.h"

char *getPage(Buffer *buf, DiskAddress page) {
   return read(buf,page,0,BLOCKSIZE);
}

int putPage(Buffer *buf, DiskAddress page, char *data, int dataSize) { 
   if (dataSize < BLOCKSIZE)
      return -1;
   
   return write(buf,page,0,BLOCKSIZE, data, BLOCKSIZE);
}

/* returns nBytes worth of content from page with DiskAddres page starting from startOffset
 * if the page is not found in CACHE OR BUFFER, returns null 
 */
char *read(Buffer *buf, DiskAddress page, int startOffset, int nBytes) {
   char *ret;
   
   if (checkVolatileFiles(buf,page.FD) >= 0) {
      ret = readVolatile(buf,page,startOffset,nBytes);
      if (ret != NULL)
         return ret;
   }
   else if (checkPersistentFiles(buf,page.FD) >= 0) {
      ret = readPersistent(buf,page,startOffset,nBytes);
      if (ret != NULL)
         return ret;
   }
   else 
      return NULL;
}

/* writes the first nBytes from data starting at startOffset into page at diskAddress page and returns 0 on success
 * Returns -1 if page is not located in the CACHE OR BUFFER
 * Returns -2 if startOffset+nBytes is greater than the page size 
 * if dataSize is less than nBytes, will write what it can and pad rest with 0's
 */
int write(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char *data, int dataSize) {
   int retCache, retBuff;
   
   retCache = writeVolatile(buf,page,startOffset,nBytes,data,dataSize);
   if (retCache == 0)
      return 0;
      
   retBuff = writePersistent(buf,page,startOffset,nBytes,data,dataSize);
   if (retBuff == 0)
      return 0;
      
   if (retCache == -1 && retBuff == -1)
      return -1;
   else   
      return -2;  
}

/* returns nBytes worth of content from page with DiskAddres page starting from startOffset
 * if the page is not found in THE CACHE, returns null 
 */
char *readVolatile(Buffer *buf, DiskAddress page, int startOffset, int nBytes) {
   int index = allocateCachePage(buf, page);
   char *ret;
   int size = nBytes;
   
   if (index == -1)
      return NULL;
   
   if (startOffset-1+nBytes >= BLOCKSIZE) {
      size = BLOCKSIZE - startOffset;
   }
   
   ret = calloc(size, sizeof(char));
   memcpy(ret, buf->cache[index].block + startOffset, size);
   
   return ret;
}

/* writes the first nBytes from data starting at startOffset into page at diskAddress page and returns 0 on success
 * Returns -1 if page is not located in the CACHE
 * Returns -2 if startOffset+nBytes is greater than the page size 
 * if dataSize is less than nBytes, will write what it can and pad rest with 0's
 */
int writeVolatile(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char * data, int dataSize) {
   int index = allocateCachePage(buf, page);
   int size = nBytes;
   char *temp;
   
   if (index == -1)
      return -1;
   
   if (startOffset-1+nBytes >= BLOCKSIZE) 
      return -2;
   
   if (dataSize < nBytes)
      size = dataSize;
   
   temp = calloc(1, nBytes);
   memcpy(temp, data, size);
   memcpy(buf->cache[index].block + startOffset, temp, size);
   
   free(temp);
   return 0;
}

/* returns nBytes worth of content from page with DiskAddres page starting from startOffset
 * if the page is not found in THE BUFFER, returns null 
 */
char *readPersistent(Buffer *buf, DiskAddress page, int startOffset, int nBytes) {
   int index = readPage(buf, page);
   char *ret;
   int size = nBytes;
   
   if (index == -1)
      return NULL;
   
   if (startOffset-1+nBytes >= BLOCKSIZE) {
      size = BLOCKSIZE - startOffset;
   }
   
   ret = calloc(size, sizeof(char));
   memcpy(ret, buf->pages[index].block + startOffset, size);
   
   return ret;
}

/* writes the first nBytes from data starting at startOffset into page at diskAddress page and returns 0 on success
 * Returns -1 if page is not located in the BUFFER
 * Returns -2 if startOffset+nBytes is greater than the page size 
 * if dataSize is less than nBytes, will write what it can and pad rest with 0's
 */
int writePersistent(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char * data, int dataSize) {
   int index = readPage(buf, page);
   int size = nBytes;
   char *temp;
   
   if (index == -1)
      return -1;
   
   if (startOffset-1+nBytes >= BLOCKSIZE) 
      return -2;
   
   if (dataSize < nBytes)
      size = dataSize;
   
   temp = calloc(1, nBytes);
   memcpy(temp, data, size);
   memcpy(buf->pages[index].block + startOffset, temp, size);
   writePage(buf, page);
   
   free(temp);
   return 0;
}
