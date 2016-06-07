#include <stdlib.h>
#include <string.h>
#include <string>
#include "bufferManager.h"
#include "heap.h"
#include "readWriteLayer.h"
#include "FLOPPY_statements/statements.h"

using namespace std;

int sizeOfRecordDesc(RecordDesc recordDesc) {
   int size = 0;
   for (int i = 0; i < recordDesc.numFields; i++) {
      if (recordDesc.fields[i].type == INT || recordDesc.fields[i].type == BOOLEAN) {
         int remainder = size % 4;
         if (remainder)
            size += 4 - remainder;
      }
      else if (recordDesc.fields[i].type == FLOAT || recordDesc.fields[i].type == DATETIME) {
         int remainder = size % 8;
         if (remainder)
            size += 8 - remainder;
      }

      size += recordDesc.fields[i].size;
   }
   // always pad records to a multiple of 8 bytes for simplicity.
   int remainder = size % 8;
   if (remainder)
      size += 8 - remainder;

   return size;
}

int createHeapFile(Buffer *buf, char *filename, RecordDesc recordDesc, int isVolatile) {
   HeapFileHeader header;

   strcpy(header.tableName, filename);

   header.recordDesc = recordDesc;

   header.recordSize = sizeOfRecordDesc(recordDesc);

   header.pageList = header.freeList = -1;
   header.lastPage = 0;
   
   header.numBlocks = header.numTuples = 0;
   
   fileDescriptor fd = getFd(filename);
   DiskAddress addr;

   newPage(buf, fd, &addr);
   writePersistent(buf, addr, 0, sizeof(HeapFileHeader), (char *)&header, sizeof(HeapFileHeader));

   // TODO figure out persistent vs volatile files...
   //
   //if (table->isVolatile) {
   //   writeVolatile(buf, addr, 0, sizeof(HeapFileHeader), (char *)&header, sizeof(HeapFileHeader));
   //   addVolatileFile(buf, fd);
   //}
   //else {
   //   writePersistent(buf, addr, 0, sizeof(HeapFileHeader), (char *)&header, sizeof(HeapFileHeader));
   //   addPersistentFile(buf, fd);
   //}
   return fd;
}

int deleteHeapFile(Buffer *buf, char *tableName) {
   deleteFile(buf, getFd(tableName));
   return 0;
}

HeapFileHeader *getFileHeader(Buffer *buf, fileDescriptor fd) {
   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;

   int i = readPage(buf, addr);
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

int heapHeaderGetLastPage(Buffer *buf, fileDescriptor fd, DiskAddress *page) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   page->FD = fd;
   page->pageId = header->lastPage;
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

int heapHeaderSetLastPage(Buffer *buf, fileDescriptor fd, int lastPage) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   header->lastPage = lastPage;
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

int heapHeaderGetNumBlocks(Buffer *buf, int fd, int *numBlocks) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;
   
   *numBlocks = header->numBlocks;
   return 0;
}

int heapHeaderGetNumTuples(Buffer *buf, int fd, int *numTuples) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;
      
   *numTuples = header->numTuples;
   return 0;
}

int heapHeaderIncrementNumBlocks(Buffer *buf, int fd) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;
      
   header->numBlocks++;

   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;
   write(buf, addr, 0, sizeof(HeapFileHeader), (char *)header, sizeof(HeapFileHeader));
   return 0;
}

int heapHeaderIncrementNumTuples(Buffer *buf, int fd) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;

   header->numTuples++;

   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;
   write(buf, addr, 0, sizeof(HeapFileHeader), (char *)header, sizeof(HeapFileHeader));
   return 0;
}

int heapHeaderDecrementNumTuples(Buffer *buf, int fd) {
   HeapFileHeader *header = getFileHeader(buf, fd);
   if (!header)
      return -1;
   
   header->numTuples--;

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

   // TODO figure out volatile stuff
   //if (checkPersistentFiles(buf, page.FD) >= 0)
      readPage(buf, page);
   //else
   //   allocateCachePage(buf, page);

   char *record = read(buf, page,
    PAGE_HDR_SIZE + recordId * recordSize, recordSize);
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

   return write(buf, page, PAGE_HDR_SIZE + recordId * recordSize,
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

   int bitmapSize = header->maxRecords / 8 + 1;
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

int pHSetNextPage(Buffer *buf, DiskAddress page, int nextPage) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   header->nextPage = nextPage;
   return write(buf, page, 0, sizeof(HeapPageHeader), (char *)header, sizeof(HeapPageHeader));
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

   int bitmapSize = header->maxRecords / 8 + 1;
   char *bitmap = read(buf, page, sizeof(HeapPageHeader), bitmapSize);
   bitmap[index / 8] |= 0x80 >> (index % 8);

   return write(buf, page, sizeof(HeapPageHeader), bitmapSize, bitmap, bitmapSize);
}

int pHSetBitmapFalse(Buffer *buf, DiskAddress page, int index) {
   HeapPageHeader *header = getPageHeader(buf, page);
   if (!header)
      return -1;

   int bitmapSize = header->maxRecords / 8 + 1;
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

   int fd = getFd(tableName);

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

      DiskAddress nextPage;
      heapHeaderGetNextPage(buf, fd, &nextPage);
      if (nextPage.pageId == -1)
         heapHeaderSetNextPage(buf, fd, page.pageId);

      DiskAddress oldLastPage;
      heapHeaderGetLastPage(buf, fd, &oldLastPage);
      header.prevPage = oldLastPage.pageId;
      header.nextPage = header.nextFree = -1;
      heapHeaderSetLastPage(buf, fd, page.pageId);

      if (oldLastPage.pageId > 0)
         pHSetNextPage(buf, oldLastPage, page.pageId);

      heapHeaderSetFreeSpace(buf, fd, page.pageId);

      writePersistent(buf, page, 0, sizeof(HeapPageHeader), (char *)&header, sizeof(HeapPageHeader));

      heapHeaderIncrementNumBlocks(buf, fd);
   }

   int maxRecords;
   pHGetMaxRecords(buf, page, &maxRecords);
   char *bitmap = (char *)malloc(maxRecords / 8 + 1);
   pHGetBitmap(buf, page, bitmap);

   int recordNdx;
   for (recordNdx = 0; recordNdx < maxRecords; recordNdx++) {
      if (!bitmapIsSet(bitmap, recordNdx))
         break;
   }

   if (recordNdx == maxRecords)
      return -1;

   *location = page;

   pHSetBitmapTrue(buf, page, recordNdx);
   pHIncrementNumRecords(buf, page);

   int x, y;
   pHGetMaxRecords(buf, page, &x);
   pHGetNumRecords(buf, page, &y);
   if (x == y) { // page is now full
      DiskAddress nextFree;
      pHGetNextFree(buf, page, &nextFree);
      heapHeaderSetFreeSpace(buf, fd, nextFree.pageId);
   }

   heapHeaderIncrementNumTuples(buf, fd);

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

   heapHeaderDecrementNumTuples(buf, page.FD);

   return 0;
}

int updateRecord(Buffer *buf, DiskAddress page, int recordId, char *record) {
   return putRecord(buf, page, recordId, record);
}

// Returns nonzero if the given index in the bitmap is set
int bitmapIsSet(char *bitmap, int ndx) {
   int mask = 0x80 >> (ndx % 8);
   return bitmap[ndx / 8] & mask;
}

