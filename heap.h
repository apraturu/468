#ifndef HEAP_H
#define HEAP_H

#define NAME_LEN 30
#define MAX_FIELDS 40
#define PAGE_HDR_SIZE 256

#include "bufferManager.h"

typedef struct {
   char name[NAME_LEN];
   int type;
   int size;
} Field;

typedef struct {
   int numFields;
   Field fields[MAX_FIELDS];
} RecordDesc;

typedef struct {
   char tableName[NAME_LEN];
   int recordSize;
   int pageList; // page id of first page in file
   int lastPage;
   int freeList; // page id of first page with free space
   int numBlocks;
   int numTuples;
   RecordDesc recordDesc;
} HeapFileHeader;

typedef struct {
   char filename[NAME_LEN];
   int pageId;
   int maxRecords;
   int occupied;
   int nextPage; // page id of next page in file
   int prevPage;
   int nextFree; // page id of next page with free space
} HeapPageHeader;

int sizeOfRecordDesc(RecordDesc recordDesc);

int createHeapFile(Buffer *buf, char *filename, RecordDesc recordDesc, int isVolatile);
int deleteHeapFile(Buffer *buf, char *tableName);

int heapHeaderGetTableName(Buffer *buf, int fd, char *name);
int heapHeaderGetRecordDesc(Buffer *buf, int fd, RecordDesc *recordDesc);
int heapHeaderGetNextPage(Buffer *buf, int fd, DiskAddress *page);
int heapHeaderGetLastPage(Buffer *buf, int fd, DiskAddress *page);
int heapHeaderGetFreeSpace(Buffer *buf, int fd, DiskAddress *page);
int heapHeaderGetRecordSize(Buffer *buf, int fd, int *recordSize);
int heapHeaderGetNumBlocks(Buffer *buf, int fd, int *numBlocks);
int heapHeaderGetNumTuples(Buffer *buf, int fd, int *numTuples);
int heapHeaderIncrementNumBlocks(Buffer *buf, int fd);
int heapHeaderIncrementNumTuples(Buffer *buf, int fd);
int heapHeaderDecrementNumTuples(Buffer *buf, int fd);

int heapHeaderSetNextPage(Buffer *buf, fileDescriptor fd, int nextPage);
int heapHeaderSetLastPage(Buffer *buf, fileDescriptor fd, int lastPage);
int heapHeaderSetFreeSpace(Buffer *buf, fileDescriptor fd, int freePage);

int getRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes);
int putRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes);

int pHGetMaxRecords(Buffer *buf, DiskAddress page, int *maxRecords);
int pHGetNumRecords(Buffer *buf, DiskAddress page, int *numRecords);
int pHGetBitmap(Buffer *buf, DiskAddress page, char *bitmap);
int pHGetNextPage(Buffer *buf, DiskAddress page, DiskAddress *nextPage);
int pHGetNextFree(Buffer *buf, DiskAddress page, DiskAddress *nextPage);
int pHGetPrevPage(Buffer *buf, DiskAddress page, DiskAddress *prevPage);
int pHSetPrevPage(Buffer *buf, DiskAddress page, int prevPage);
int pHSetNextFree(Buffer *buf, DiskAddress page, int nextFree);
int pHSetBitmapTrue(Buffer *buf, DiskAddress page, int index);
int pHSetBitmapFalse(Buffer *buf, DiskAddress page, int index);
int pHDecrementNumRecords(Buffer *buf, DiskAddress page);
int pHIncrementNumRecords(Buffer *buf, DiskAddress page);

int getField(char *fieldName, char *record, RecordDesc rd, char *out);
int setField(char *fieldName, char *record, RecordDesc rd, char *value);

int insertRecord(Buffer *buf, char *tableName, char *record, DiskAddress *location);
int deleteRecord(Buffer *buf, DiskAddress page, int recordId);
int updateRecord(Buffer *buf, DiskAddress page, int recordId, char *record);

int bitmapIsSet(char *bitmap, int ndx);

#endif