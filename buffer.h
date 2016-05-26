#ifndef BUFFER_H
#define BUFFER_H

typedef struct{
    char[BLOCK_SIZE] block;
    DiskAddress diskAddress;
} Block;

typedef struct {
    fileDescriptor FD;
    int pageId;
} DiskAddress;

typedef struct {
    char *database;
    int nBlocks;
    Block[BUFFER_SIZE];
    long[BUFFER_SIZE] timestamp;
    char[BUFFER_SIZE] pin;
    char[BUFFER_SIZE] dirty;
    int numOccupied;
} Buffer;

int writePage(Buffer *buf, DiskAddress diskPage);

#endif