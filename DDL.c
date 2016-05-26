#include "heap.h"
#include "sequential.h"

int createTable(Buffer *buf, char *tableCreate) {
    createHeapFile(buf, tableCreate);
}

int dropTable(Buffer *buf, char *tableName) {
    deleteHeapFile(buf, tableName);
}

int createIndex(Buffer *buf, )

