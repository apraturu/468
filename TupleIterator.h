#ifndef TUPLEITERATOR_H
#define TUPLEITERATOR_H

#include <vector>
#include <string>
#include <map>
#include "heap.h"
#include "FLOPPY_statements/statements.h"

using namespace std;

struct RecordField {
   ColumnType type;
   bool bVal;
   int iVal;
   double fVal;
   string sVal;

   RecordField(int i);
   RecordField(ColumnType t, double f);
   RecordField(string s);
   RecordField(bool b);
   RecordField();
   RecordField(const RecordField& r);
   ~RecordField();

   RecordField& operator=(const RecordField&);

};

bool operator<(const RecordField r1, const RecordField r2);
bool operator>(const RecordField r1, const RecordField r2);
bool operator<=(const RecordField r1, const RecordField r2);
bool operator>=(const RecordField r1, const RecordField r2);
bool operator==(const RecordField r1, const RecordField r2);
bool operator!=(const RecordField r1, const RecordField r2);
RecordField operator+(const RecordField r1, const RecordField r2);
RecordField operator-(const RecordField r1, const RecordField r2);
RecordField operator*(const RecordField r1, const RecordField r2);
RecordField operator/(const RecordField r1, const RecordField r2);
RecordField operator%(const RecordField r1, const RecordField r2);

class Record {
public:
   Record();
   Record(char bytes[], RecordDesc recordDesc, DiskAddress page, int ndx);

   char *getBytes(RecordDesc recordDesc);

   map<string, RecordField> fields;
   DiskAddress page;
   int ndx;

   RecordDesc recordDesc;
};


class TupleIterator {
public:
   TupleIterator(int fd);
   ~TupleIterator();

   Record *next();
   int pageNdx();
   void goToPage(int page);

private:
   void startPage();

   int fd;
   int recordSize;
   int recordsPerPage;
   RecordDesc recordDesc;
   char *bitmap;
   char *recordBytes;
   vector<DiskAddress> pages;
   DiskAddress curPage;
   int _pageNdx;
   int curRecord;
};

#endif
