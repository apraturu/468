#ifndef TUPLEITERATOR_H
#define TUPLEITERATOR_H

#include <vector>
#include <string>
#include <map>
#include "heap.h"

using namespace std;

struct RecordField {
   enum FieldType {
      INT, FLOAT, VARCHAR, DATETIME, BOOLEAN
   } type;
   union {
      bool bVal;
      int iVal;
      double fVal;
      string sVal;
   };

   RecordField(int i) : type(INT), iVal(i) {}
   RecordField(FieldType t, double f) : type(t), fVal(f) {}
   RecordField(string s) : type(VARCHAR), sVal(s) {}
   RecordField(bool b) : type(BOOLEAN), bVal(b) {}
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
   Record(char bytes[], RecordDesc recordDesc);

   char *getBytes(); // TODO I haven't implemented this yet.

   map<string, RecordField> fields;

private:
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
   char bitmap[];
   char recordBytes[];
   vector<DiskAddress> pages;
   DiskAddress curPage;
   int _pageNdx;
   int curRecord;
};


#endif
