#include <stdlib.h>
#include <string.h>
#include "bufferManager.h"
#include "heap.h"
#include "parser.h"
#include "readWriteLayer.h"

int createHeapFile(Buffer *buf, char *createTable) {
   tableDescription *table = parseCreate(createTable);

   HeapFileHeader header;

   strcpy(header.tableName, table->tableName);

   Attribute *att = table->attList;
   int fNdx = 0;
   header.recordSize = header.recordDesc.numFields = 0;
   while (att) {
      strcpy(header.recordDesc.fields[fNdx].name, att->attName);
      header.recordDesc.fields[fNdx].type = att->attType;
      header.recordDesc.fields[fNdx].size = att->size;
      header.recordSize += att->size;
      header.recordDesc.numFields++;
      fNdx++;
      att = att->next;
   }

   header.pageList = header.freeList = -1;

   fileDescriptor fd = tfs_openFile(table->tableName);
   DiskAddress addr;

   newPage(buf, fd, &addr);
   if (table->isVolatile) {
      allocateCachePage(buf, addr);
      writeVolatile(buf, addr, 0, sizeof(HeapFileHeader), (char *)&header, sizeof(HeapFileHeader));
      addVolatileFile(buf, fd);
   }
   else {
      readPage(buf, addr);
      writePersistent(buf, addr, 0, sizeof(HeapFileHeader), (char *)&header, sizeof(HeapFileHeader));
      addPersistentFile(buf, fd);
   }
   return 0;
}

int deleteHeapFile(Buffer *buf, char *tableName) {
   int fd = tfs_openFile(tableName);
   tfs_deleteFile(fd);
   
   int i;
   for (i = 0; i < buf->nBufferBlocks; i++) {
      if (buf->buffer_timestamp[i] == -1)
         continue;
      if (buf->pages[i].address.FD == fd) {
         buf->buffer_timestamp[i] = -1;
         buf->numBufferOccupied--;
      }
   }
   for (i = 0; i < buf->nCacheBlocks; i++) {
      if (buf->cache_timestamp[i] == -1)
         continue;
      if (buf->cache[i].address.FD == fd) {
         buf->cache_timestamp[i] = -1;
         buf->numCacheOccupied--;
      }
   }
}

HeapFileHeader *getFileHeader(Buffer *buf, fileDescriptor fd) {
   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;

   readPage(buf, addr);
   return (HeapFileHeader *)read(buf, addr, 0, sizeof(HeapFileHeader));
}

HeapPageHeader *getPageHeader(Buffer *buf, DiskAddress addr) {
   readPage(buf, addr);
   return (HeapPageHeader *)read(buf, addr, 0, sizeof(HeapPageHeader));
}

int heapHeaderGetTableName(Buffer *buf, fileDescriptor fd, char *name) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header) {
      return -1;
   }

   strcpy(name, header->tableName);
   return 0;
}

int heapHeaderGetRecordDesc(Buffer *buf, fileDescriptor fd, RecordDesc *recordDesc) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   memcpy(recordDesc, &header->recordDesc, sizeof(RecordDesc));
   return 0;
}

int heapHeaderGetNextPage(Buffer *buf, fileDescriptor fd, DiskAddress *page) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   page->FD = fd;
   page->pageId = header->pageList;
   return 0;
}

int heapHeaderGetFreeSpace(Buffer *buf, fileDescriptor fd, DiskAddress *page) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   page->FD = fd;
   page->pageId = header->freeList;
   return 0;
}

int heapHeaderGetRecordSize(Buffer *buf, fileDescriptor fd, int *recordSize) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   *recordSize = header->recordSize;
   return 0;
}

int heapHeaderSetNextPage(Buffer *buf, fileDescriptor fd, int nextPage) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   header->pageList = nextPage;
   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;
   write(buf, addr, 0, sizeof(HeapFileHeader), (char *)header, sizeof(HeapFileHeader));
   return 0;
}

int heapHeaderSetFreeSpace(Buffer *buf, fileDescriptor fd, int freePage) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   header->freeList = freePage;
   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;
   write(buf, addr, 0, sizeof(HeapFileHeader), (char *)header, sizeof(HeapFileHeader));
   return 0;
}

int getRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes) {
   int recordSize;
   if (heapHeaderGetRecordSize(buf, page.FD, &recordSize) < 0)
      return -1;

   if (checkPersistentFiles(buf, page.FD) >= 0)
      readPage(buf, page);
   else
      allocateCachePage(buf, page);

   char *record = read(buf, page,
    sizeof(HeapFileHeader) + recordId * recordSize, recordSize);
   memcpy(bytes, record, recordSize);
   return 0;
}

int putRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes) {
   int recordSize;
   if (heapHeaderGetRecordSize(buf, page.FD, &recordSize) < 0)
      return -1;

   if (checkPersistentFiles(buf, page.FD) >= 0)
      readPage(buf, page);
   else
      allocateCachePage(buf, page);

   return write(buf, page, sizeof(HeapFileHeader) + recordId * recordSize,
    recordSize, bytes, recordSize);
}

int pHGetMaxRecords(Buffer *buf, DiskAddress page, int *maxRecords) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   *maxRecords = header->maxRecords;
   return 0;
}

int pHGetNumRecords(Buffer *buf, DiskAddress page, int *numRecords) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   *numRecords = header->occupied;
   return 0;
}

int pHGetBitmap(Buffer *buf, DiskAddress page, char *bitmap) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   int bitmapSize = header->maxRecords / 8;
   memcpy(bitmap, read(buf, page, sizeof(HeapPageHeader), bitmapSize), bitmapSize);
   return 0;
}

int pHGetNextPage(Buffer *buf, DiskAddress page, DiskAddress *nextPage) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   nextPage->FD = page.FD;
   nextPage->pageId = header->nextPage;
   return 0;
}

int pHGetNextFree(Buffer *buf, DiskAddress page, DiskAddress *nextPage) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   nextPage->FD = page.FD;
   nextPage->pageId = header->nextFree;
   return 0;
}

int pHGetPrevPage(Buffer *buf, DiskAddress page, DiskAddress *prevPage) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   prevPage->FD = page.FD;
   prevPage->pageId = header->prevPage;
   return 0;
}

int pHSetPrevPage(Buffer *buf, DiskAddress page, int prevPage) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   header->prevPage = prevPage;
   return write(buf, page, 0, sizeof(HeapPageHeader), (char *)header, sizeof(HeapPageHeader));
}

int pHSetNextFree(Buffer *buf, DiskAddress page, int nextFree) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   header->nextFree = nextFree;
   return write(buf, page, 0, sizeof(HeapPageHeader), (char *)header, sizeof(HeapPageHeader));
}

int pHSetBitmapTrue(Buffer *buf, DiskAddress page, int index) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   int bitmapSize = header->maxRecords / 8;
   char *bitmap = read(buf, page, sizeof(HeapPageHeader), bitmapSize);
   bitmap[index / 8] |= 0x80 >> (index % 8);

   return write(buf, page, sizeof(HeapPageHeader), bitmapSize, bitmap, bitmapSize);
}

int pHSetBitmapFalse(Buffer *buf, DiskAddress page, int index) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   int bitmapSize = header->maxRecords / 8;
   char *bitmap = read(buf, page, sizeof(HeapPageHeader), bitmapSize);
   bitmap[index / 8] &= ~(0x80 >> (index % 8));

   return write(buf, page, sizeof(HeapPageHeader), bitmapSize, bitmap, bitmapSize);
}

int pHDecrementNumRecords(Buffer *buf, DiskAddress page) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   header->occupied--;
   return write(buf, page, 0, sizeof(HeapPageHeader), (char *)header, sizeof(HeapPageHeader));
}

int pHIncrementNumRecords(Buffer *buf, DiskAddress page) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   header->occupied++;
   return write(buf, page, 0, sizeof(HeapPageHeader), (char *)header, sizeof(HeapPageHeader));
}

int getField(char *fieldName, char *record, RecordDesc rd, char *out) {
   int i;
   for (i = 0; i < rd.numFields; i++) {
      if (strcmp(rd.fields[i].name, fieldName) == 0) {
         memcpy(out, record, rd.fields[i].size);
         return 0;
      }
      else {
         record += rd.fields[i].size;
      }
   }

   return -1;
}

int setField(char *fieldName, char *record, RecordDesc rd, char *value) {
   int i;
   for (i = 0; i < rd.numFields; i++) {
      if (strcmp(rd.fields[i].name, fieldName) == 0) {
         memcpy(record, value, rd.fields[i].size);
         return 0;
      }
      else {
         record += rd.fields[i].size;
      }
   }

   return -1;
}

int insertRecord(Buffer *buf, char *tableName, char *record, DiskAddress *location) {
   DiskAddress page;

   int fd = tfs_openFile(tableName);

   if (heapHeaderGetFreeSpace(buf, fd, &page) < 0)
      return -1;

   int recordSize;
   if (page.pageId == -1) { // make a new page
      newPage(buf, page.FD, &page);

      HeapPageHeader header;
      strcpy(header.filename, tableName);
      header.pageId = page.pageId;

      heapHeaderGetRecordSize(buf, fd, &recordSize);
      header.maxRecords = (BLOCKSIZE - PAGE_HDR_SIZE) / recordSize;

      header.occupied = 0;

      // update linked list
      
      // set new page's nextPage to old nextPage
      DiskAddress tmp;
      heapHeaderGetNextPage(buf, fd, &tmp);
      header.nextPage = tmp.pageId;

      // set file header's pointers to new page
      heapHeaderSetNextPage(buf, fd, page.pageId);
      heapHeaderSetFreeSpace(buf, fd, page.pageId);

      if (header.nextPage != -1) { // set old nextPage's prevPage to new page
         DiskAddress oldNextPage;
         oldNextPage.FD = fd;
         oldNextPage.pageId = header.nextPage;
         pHSetPrevPage(buf, oldNextPage, page.pageId);
      }

      header.prevPage = 0; // set new page's prevPage to file header
      header.nextFree = -1; // set new page's nextFree to -1 because no other free pages
   }

   int maxRecords;
   pHGetMaxRecords(buf, page, &maxRecords);
   char *bitmap = malloc(maxRecords / 8), *byte = bitmap;
   pHGetBitmap(buf, page, bitmap);

   int recordNdx = 0;
   int mask;
   while (recordNdx < maxRecords) {
      mask = 0x80 >> (recordNdx % 8);

      if (mask & *byte == 0)
         break;

      recordNdx++;
      if (recordNdx % 8 == 0)
         byte++;
   }

   if (recordNdx == maxRecords)
      return -1;

   *location = page;

   pHSetBitmapTrue(buf, page, recordNdx);
   pHIncrementNumRecords(buf, page);

   // check if page is full now
   //int i = recordNdx + 1;
   //while (i < maxRecords) {
   //   mask = 0x80 >> (i % 8);

   //   if (mask & *byte == 0)
   //      break;

   //   i++;
   //   if (i % 8 == 0)
   //      byte++;
   //}
   //if (i == maxRecords) { // page is now full
   int x, y;
   pHGetMaxRecords(buf, page, &x);
   pHGetNumRecords(buf, page, &y);
   if (x == y) { // page is now full
      DiskAddress nextFree;
      pHGetNextFree(buf, page, &nextFree);
      heapHeaderSetFreeSpace(buf, fd, nextFree.pageId);
   }

   return putRecord(buf, page, recordNdx, record);  
}

/* deleteRecord:
 * if page is full:
 *    put page at front of freelist
 * setBitmapFalse
 * decrementCount 
 */
int deleteRecord(Buffer *buf, DiskAddress page, int recordId) {
   int x, y;
   pHGetMaxRecords(buf, page, &x);
   pHGetNumRecords(buf, page, &y);
   if (x == y) { // page is now full, put at front of free list
      // set this page's nextFree to file header's nextFree
      DiskAddress z;
      heapHeaderGetFreeSpace(buf, page.FD, &z);
      pHSetNextFree(buf, page, z.pageId);
      
      // set file header's nextFree to this page
      heapHeaderSetFreeSpace(buf, page.FD, page.pageId);
   }

   pHSetBitmapFalse(buf, page, recordId);
   pHDecrementNumRecords(buf, page);

   return 0;
}

int updateRecord(Buffer *buf, DiskAddress page, int recordId, char *record) {
   return putRecord(buf, page, recordId, record);
}
