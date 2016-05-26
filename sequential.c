#include <stdlib.h>
#include <string.h>
#include "bufferManager.h"
#include "sequential.h"
#include "parser.h"
#include "readWriteLayer.h"


SequentialPageHeader *getSequentialPageHeader(Buffer *buf, DiskAddress addr) {
   readPage(buf, addr);
   return (SequentialPageHeader *)read(buf, addr, 0, sizeof(SequentialPageHeader));
}

SequentialFileHeader *getSequentialFileHeader(Buffer *buf, fileDescriptor fd) {
   SequentialFileHeader *header = malloc(sizeof(SequentialFileHeader)); 
   DiskAddress addr;
   addr.FD = fd;
   addr.pageId = 0;

   if(checkPersistentFiles(buf, fd) >= 0) {
       readPage(buf, addr);
   } else {
       allocateCachePage(buf, addr);
   }
   
   return (SequentialFileHeader *)read(buf, addr, 0, sizeof(SequentialFileHeader));
}

int pHIncrementRecords(Buffer *buf, DiskAddress page) {
   SequentialPageHeader *header = getSequentialPageHeader(buf, page);
   if (!header)
      return -1;

   header->occupied++;
   return 0;
}

int pHDecrementRecords(Buffer *buf, DiskAddress page) {
   SequentialPageHeader *header = getSequentialPageHeader(buf, page);
   if (!header)
      return -1;

   header->occupied--;
   return 0;
}

int createSequentialFile(Buffer *buf, char *createTable, RecordDesc recordDesc, int volatileFlag) {
   tableDescription *table = parseCreate(createTable);

   SequentialFileHeader header;

   strcpy(header.tableName, table->tableName);
   
   //Makes index on primary key
   strcpy(header.indexName, table->pKey->attName);
   
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

//Go back and remove file descriptors from persistent/volatile list of fds
//Do it in the heap file as well

//Once volatile and persistent list functions are finished, update this function
int deleteSequentialFile(Buffer *buf, char *tableName) {
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
   
   if(checkPersistentFiles(buf, fd) >= 0) {
       removeFileFromPersistentList(buf, fd);
   } else {
       removeFileFromVolatileList(buf, fd);
   }
   
   return 0;
}

int seqHeaderGetRecordDesc(Buffer *buf, int fd, RecordDesc *recordDesc) {
   SequentialFileHeader *header = getSequentialFileHeader(buf, fd);
   if (!header)
      return -1;

   memcpy(recordDesc, &header->recordDesc, sizeof(RecordDesc));
   return 0;
}


int seqHeaderGetNextPage(Buffer *buf, int fd, DiskAddress *page) {
   SequentialFileHeader *header = getSequentialFileHeader(buf, fd);
   if (!header)
      return -1;

   page->FD = fd;
   page->pageId = header->pageList;
   return 0;
}

int seqGetRecord(Buffer *buf, DiskAddress page, int recordId, char *bytes) {
   int recordSize;
   if (seqHeaderGetRecordSize(buf, page.FD, &recordSize) < 0)
      return -1;

   if (checkPersistentFiles(buf, page.FD) >= 0)
      readPage(buf, page);
   else
      allocateCachePage(buf, page);

   char *record = read(buf, page,
   sizeof(SequentialFileHeader) + recordId * recordSize, recordSize);
   memcpy(bytes, record, recordSize);
   return 0;
}

int seqHeaderGetRecordSize(Buffer *buf, int fd, int *recordSize) {
   SequentialFileHeader *header = getSequentialFileHeader(buf, fd);
   if (!header)
      return -1;

   *recordSize = header->recordSize;
   return 0;
}

//Returns 0 if record was inserted
//Returns 1 if record was inserted but block is now full
//Padding will be an issue, were ignoring for right now
int seqPutRecord(Buffer *buf, DiskAddress page, char *bytes) {
    int i, offset = 0, padding = 0, attSize, type, curInt, insertInt,
     numRecords, recordSize, maxRecords;
    float curFloat, insertFloat;
    char *indexName, *curRecord, *curVarchar, *insertVarchar;
    RecordDesc recordDesc;
    seqHeaderGetRecordDesc(buf, page.FD, &recordDesc);
    seqHeaderGetIndexName(buf, page.FD, indexName);
    
    for(i = 0; i < recordDesc.numFields; i++) {
        if(strcmp(indexName, recordDesc.fields[i].name) == 0) {
            break;
        } else {
            offset += recordDesc.fields[i].size;
        }
    }
    
    type = recordDesc.fields[i].type;
    attSize = recordDesc.fields[i].size;
    
    offset += padding;
    
    if(checkPersistentFiles(buf, page.FD) >= 0) {
        readPage(buf, page);
    } else {
        allocateCachePage(buf, page);
    }

    pHGetNumRecords(buf, page, &numRecords);
    sequentialHeaderGetRecordSize(buf, page.FD, &recordSize);
        
    if(type == 0) {
        //INT
        memcpy(&insertInt, bytes + offset, attSize);
        
        for(i = 0; i < numRecords; i++) {
            memcpy(curRecord, read(buf, page, offset + PAGE_HDR_SIZE + (i * recordSize), attSize), attSize);
            memcpy(&curInt, curRecord, attSize);
            if(curInt > insertInt) {
                break;
            }
        }        
    } else if (type == 1) {
        //FLOAT
        memcpy(&curFloat, curRecord, attSize);
        
        for(i = 0; i < numRecords; i++) {
            memcpy(curRecord, read(buf, page, offset + PAGE_HDR_SIZE + (i * recordSize), attSize), attSize);
            memcpy(&curFloat, curRecord, attSize);
            if(curInt > insertInt) {
                break;
            }
        }  
    } else if (type == 2) {
        //VARCHAR
        curVarchar = curRecord;
        
        for(i = 0; i < numRecords; i++) {
            memcpy(curRecord, read(buf, page, offset + PAGE_HDR_SIZE + (i * recordSize), attSize), attSize);
            memcpy(&curVarchar, curRecord, attSize);
            if(curInt > insertInt) {
                break;
            }
        }  
    } else if (type == 3) {
        //DATETIME
        memcpy(&curInt, curRecord, attSize);
        
        for(i = 0; i < numRecords; i++) {
            memcpy(curRecord, read(buf, page, offset + PAGE_HDR_SIZE + (i * recordSize), attSize), attSize);
            memcpy(&curInt, curRecord, attSize);
            if(curInt > insertInt) {
                break;
            }
        }
    } else {
        //BOOLEAN
        memcpy(&curInt, curRecord, attSize);
        
        for(i = 0; i < numRecords; i++) {
            memcpy(curRecord, read(buf, page, offset + PAGE_HDR_SIZE + (i * recordSize), attSize), attSize);
            memcpy(&curInt, curRecord, attSize);
            if(curInt > insertInt) {
                break;
            }
        }
    }
    
    pHGetMaxRecords(buf, page, &maxRecords);
    
    if(i + 2 < maxRecords) {
       shiftRecordsTowardsEnd(buf, page, i);
       write(buf, page, PAGE_HDR_SIZE + i * recordSize, recordSize, bytes, recordSize);
       SequentialFileHeader *header =  getSequentialFileHeader(buf, page.FD);
       pHIncrementRecords(buf, page);
       return 0;
    } else {
       //Block is now full, either create new block now or take care of it somewhere else
       return 1; 
    }
}

//I'm assuming the first recordId will be record # 1
int seqUpdateRecord(Buffer *buf, DiskAddress page, int recordId, char *record) {
    int recordSize;
    
    sequentialHeaderGetRecordSize(buf, page.FD, &recordSize);
    
    write(buf, page, PAGE_HDR_SIZE + (recordId - 1) * recordSize, recordSize, record, recordSize);
    return 0;
}

int shiftRecordsTowardsfront(Buffer *buf, DiskAddress page, int shiftIndex) {
    int recordSize, numRecords, i;
    char *curRecord, *nextRecord;
    
    sequentialHeaderGetRecordSize(buf, page.FD, &recordSize);
    pHGetNumRecords(buf, page, &numRecords);
    
    if(numRecords == 1)
        return 0;
    
    if(numRecords == 0)
        return -1;

    memcpy(nextRecord, read(buf, page, PAGE_HDR_SIZE + ((numRecords - 1) * recordSize), recordSize), recordSize);
    
    for(i = numRecords - 1; i > shiftIndex; i--) {
        memcpy(curRecord, nextRecord, recordSize);
        memcpy(nextRecord, read(buf, page, PAGE_HDR_SIZE + ((i - 1) * recordSize), recordSize), recordSize);
        write(buf, page, PAGE_HDR_SIZE + (i - 1) * recordSize, recordSize, curRecord, recordSize);
    }    
    return 0;
}

int seqDeleteRecord(Buffer *buf, DiskAddress page, int recordId) {
    shiftRecordsTowardsFront(buf, page, recordId);
    pHDecrementRecords(buf, page);
    return 0;
}

int shiftRecordsTowardsEnd(Buffer *buf, DiskAddress page, int shiftIndex) {
    int recordSize, numRecords, i;
    char *curRecord, *nextRecord;
    
    sequentialHeaderGetRecordSize(buf, page.FD, &recordSize);
    pHGetNumRecords(buf, page, &numRecords);


    memcpy(nextRecord, read(buf, page, PAGE_HDR_SIZE + (shiftIndex * recordSize), recordSize), recordSize);
    
    for(i = shiftIndex; i < numRecords; i++) {
        memcpy(curRecord, nextRecord, recordSize);
        memcpy(nextRecord, read(buf, page, PAGE_HDR_SIZE + ((i + 1) * recordSize), recordSize), recordSize);
        write(buf, page, PAGE_HDR_SIZE + (i + 1) * recordSize, recordSize, curRecord, recordSize);
    }
    
    write(buf, page, PAGE_HDR_SIZE + i * recordSize, recordSize, nextRecord, recordSize);
    return 0;
}


int seqHeaderGetIndexName(Buffer *buf, int fd, char *name) {
   SequentialFileHeader *header = getSequentialFileHeader(buf, fd);
   if (!header)
      return -1;

   memcpy(name, &header->indexName, sizeof(NAME_LEN));
   return 0;
}

int sequentialHeaderGetRecordSize(Buffer *buf, fileDescriptor fd, int *recordSize) {
   SequentialFileHeader *header = getSequentialFileHeader(buf, fd);
   if (!header)
      return -1;

   *recordSize = header->recordSize;
   return 0;
}

int pHGetRecordSize(Buffer *buf, DiskAddress page) {
    SequentialFileHeader *header = getSequentialFileHeader(buf, page.FD);
    if(!header)
        return -1;
        
        return header->recordSize;
}

int pHSetRecordSize(Buffer *buf, DiskAddress page, int recordSize) {
    SequentialFileHeader *header = getSequentialFileHeader(buf, page.FD);
    if(!header)
        return -1;
        
    header->recordSize = recordSize;
    return 0;
}