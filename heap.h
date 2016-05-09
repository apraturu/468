#define NAME_LEN 30
#define MAX_FIELDS 20
#define PAGE_HDR_SIZE 256

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
   int freeList; // page id of first page with free space
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

int createHeapFile(Buffer *buf, char *createTable);
int deleteHeapFile(Buffer *buf, char *tableName);

int heapHeaderGetTableName(Buffer *buf, int fd, char *name);
int heapHeaderGetRecordDesc(Buffer *buf, int fd, RecordDesc *recordDesc);
int heapHeaderGetNextPage(Buffer *buf, int fd, DiskAddress *page);
int heapHeaderGetFreeSpace(Buffer *buf, int fd, DiskAddress *page);
int heapHeaderGetRecordSize(Buffer *buf, int fd, int *recordSize);
int heapHeaderSetNextPage(Buffer *buf, fileDescriptor fd, int nextPage);
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
