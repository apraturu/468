#ifndef MAIN_H
#define MAIN_H

#include "bufferManager.h"

extern Buffer *buffer;

int runStatement(char *query, bool *shouldDelete);

#endif
