#ifndef READWRITELAYER_H
#define READWRITELAYER_H

char *getPage(Buffer *buf, DiskAddress page);

int putPage(Buffer *buf, DiskAddress page, char *data, int dataSize);

char *read(Buffer *buf, DiskAddress page, int startOffset, int nBytes);

int write(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char *data, int dataSize);

char *readVolatile(Buffer *buf, DiskAddress page, int startOffset, int nBytes);

int writeVolatile(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char *data, int dataSize);

char *readPersistent(Buffer *buf, DiskAddress page, int startOffset, int nBytes);

int writePersistent(Buffer *buf, DiskAddress page, int startOffset, int nBytes, char *data, int dataSize);

#endif