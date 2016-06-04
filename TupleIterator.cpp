#include <cstddef>
#include "TupleIterator.h"
#include "main.h"

Record::Record(char bytes[], RecordDesc recordDesc) : recordDesc(recordDesc) {
   // TODO
}

bool operator<(const RecordField r1, const RecordField r2) {
   switch (r1.type) {
      case INT:
         if (r2.type == INT)
            return r1.iVal < r2.iVal;
         else if (r2.type == FLOAT)
            return r1.iVal < r2.fVal;
         else
            return false;
      case FLOAT:
         if (r2.type == INT)
            return r1.fVal < r2.iVal;
         else if (r2.type == FLOAT)
            return r1.fVal < r2.fVal;
         else
            return false;
      case VARCHAR:
         return r1.sVal < r2.sVal;
      case DATETIME:
         return r1.fVal < r2.fVal;
      case BOOLEAN:
         return r1.bVal < r2.bVal;
   }
   return false;
}
bool operator>(const RecordField r1, const RecordField r2) {return r2 < r1;}
bool operator<=(const RecordField r1, const RecordField r2) {return !(r2 < r1);}
bool operator>=(const RecordField r1, const RecordField r2) {return !(r1 < r2);}
bool operator==(const RecordField r1, const RecordField r2) {return r1 >= r2 && r1 <= r2;}
bool operator!=(const RecordField r1, const RecordField r2) {return !(r1 == r2);}

RecordField operator+(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal + r2.fVal);
      else
         return RecordField(INT, r1.iVal + r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal + r2.fVal);
      else
         return RecordField(INT, r1.fVal + r2.iVal);
   }
}
RecordField operator-(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal - r2.fVal);
      else
         return RecordField(INT, r1.iVal - r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal - r2.fVal);
      else
         return RecordField(INT, r1.fVal - r2.iVal);
   }
}
RecordField operator*(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal * r2.fVal);
      else
         return RecordField(INT, r1.iVal * r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal * r2.fVal);
      else
         return RecordField(INT, r1.fVal * r2.iVal);
   }
}
RecordField operator/(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal / r2.fVal);
      else
         return RecordField(INT, r1.iVal / r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal / r2.fVal);
      else
         return RecordField(INT, r1.fVal / r2.iVal);
   }
}
RecordField operator%(const RecordField r1, const RecordField r2) {
   return RecordField(r1.iVal % r2.iVal);
}

// Returns nonzero if the given index in the bitmap is set
static int bitmapIsValid(char *bitmap, int ndx) {
   int mask = 0x80 >> (ndx % 8);
   return bitmap[ndx / 8] & mask;
}


TupleIterator::TupleIterator(int fd) : fd(fd) {
   heapHeaderGetNextPage(buffer, fd, &curPage);
   heapHeaderGetRecordDesc(buffer, fd, &recordDesc);

   heapHeaderGetRecordSize(buffer, fd, &recordSize);
   recordBytes = new char[recordSize];

   pHGetMaxRecords(buffer, curPage, &recordsPerPage);
   bitmap = new char[recordsPerPage / 8 + 1];

   startPage();
}

TupleIterator::~TupleIterator() {
   delete recordBytes;
   delete bitmap;
}

Record *TupleIterator::next() {
   if (curRecord == -1)
      return NULL;

   getRecord(buffer, curPage, curRecord, recordBytes);
   Record *record = new Record(recordBytes, recordDesc);

   for (curRecord = curRecord + 1; curRecord < recordsPerPage; curRecord++) {
      if (bitmapIsValid(bitmap, curRecord)) {
         break;
      }
   }
   if (curRecord == recordsPerPage) { // no more records on page
      pHGetNextPage(buffer, curPage, &curPage);
      startPage();
   }

   return record;
}

int TupleIterator::pageNdx() {return _pageNdx;}

void TupleIterator::goToPage(int page) {
   _pageNdx = page - 1;
   curPage = pages[page - 1];
   startPage();
}

void TupleIterator::startPage() {
   _pageNdx++;
   if (_pageNdx > pages.size())
      pages.push_back(curPage);

   pHGetBitmap(buffer, curPage, bitmap);
   for (curRecord = 0; curRecord < recordsPerPage; curRecord++) {
      if (bitmapIsValid(bitmap, curRecord)) {
         break;
      }
   }
   if (curRecord == recordsPerPage)
      curRecord = -1;
}
