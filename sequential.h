#include "heap.h"

/*
typedef struct {
   char name[NAME_LEN];
   int type;
   int size;
} Field;

typedef struct {
   int numFields;
   Field fields[MAX_FIELDS];
} RecordDesc;
*/

typedef struct {
   char indexName[NAME_LEN];
   char tableName[NAME_LEN];
   int recordSize;
   int pageList; // page id of first page in file
   int freeList; // page id of first page with free space
   RecordDesc recordDesc;
} SequentialFileHeader;

typedef struct {
   char filename[NAME_LEN];
   int pageId;
   int maxRecords;
   int occupied;
   int nextPage; // page id of next page in file
   int prevPage;
   int nextFree; // page id of next page with free space
} SequentialPageHeader;

int createSequentialFile(Buffer *buf, char *name, RecordDesc recordDesc, int volatileFlag);
int deleteSequentialFile(Buffer *buf, char *name);
int sequentialHeaderGetRecordSize(Buffer *buf, fileDescriptor fd, int *recordSize);
int seqHeaderGetRecordSize(Buffer *buf, int fd, int *recordSize);
int seqHeaderGetRecordDesc(Buffer *buf, int fd, RecordDesc *recordDesc);
int seqHeaderGetNextPage(Buffer *buf, int fd, DiskAddress *page);
int seqHeaderGetIndexName(Buffer *buf, int fd, char *name);
int seqGetRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes);
int seqPutRecord(Buffer *buf, DiskAddress page, char *bytes);
int seqDeleteRecord(Buffer *buf, DiskAddress page, int recordId);

int pHGetRecSize(Buffer *buf, DiskAddress page);
int pHSetRecSize(Buffer *buf, DiskAddress page, int size);

int seqInsertRecord(char *name, char *record, DiskAddress *location);
int seqUpdateRecord(Buffer *buf, DiskAddress page, int recordId, char *record);

int shiftRecordsTowardsEnd(Buffer *buf, DiskAddress page, int shiftIndex);
int shiftRecordsTowardsFront(Buffer *buf, DiskAddress page, int shiftIndex);





