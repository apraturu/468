#include <cstddef>
#include <cstring>
#include "TupleIterator.h"
#include "main.h"
#include "heap.h"
#include "bufferManager.h"

RecordField::RecordField(int i) : type(INT), iVal(i) {}
RecordField::RecordField(ColumnType t, double f) : type(t), fVal(f) {}
RecordField::RecordField(string s) : type(VARCHAR), sVal(s) {}
RecordField::RecordField(bool b) : type(BOOLEAN), bVal(b) {}

RecordField::RecordField() {}
RecordField::RecordField(const RecordField& r) : type(r.type) {
   if (type == INT)
      iVal = r.iVal;
   else if (type == BOOLEAN)
      bVal = r.bVal;
   else if (type == FLOAT || type == DATETIME)
      fVal = r.fVal;
   else
      sVal = r.sVal;
}
RecordField::~RecordField() {}

RecordField& RecordField::operator=(const RecordField& r) {
   type = r.type;
   if (type == INT)
      iVal = r.iVal;
   else if (type == BOOLEAN)
      bVal = r.bVal;
   else if (type == FLOAT || type == DATETIME)
      fVal = r.fVal;
   else
      sVal = r.sVal;
   return *this;
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
         return RecordField(r1.iVal + r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal + r2.fVal);
      else
         return RecordField(FLOAT, r1.fVal + r2.iVal);
   }
}
RecordField operator-(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal - r2.fVal);
      else
         return RecordField(r1.iVal - r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal - r2.fVal);
      else
         return RecordField(FLOAT, r1.fVal - r2.iVal);
   }
}
RecordField operator*(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal * r2.fVal);
      else
         return RecordField(r1.iVal * r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal * r2.fVal);
      else
         return RecordField(FLOAT, r1.fVal * r2.iVal);
   }
}
RecordField operator/(const RecordField r1, const RecordField r2) {
   if (r1.type == INT) {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.iVal / r2.fVal);
      else
         return RecordField(r1.iVal / r2.iVal);
   }
   else {
      if (r2.type == FLOAT)
         return RecordField(FLOAT, r1.fVal / r2.fVal);
      else
         return RecordField(FLOAT, r1.fVal / r2.iVal);
   }
}
RecordField operator%(const RecordField r1, const RecordField r2) {
   return RecordField(r1.iVal % r2.iVal);
}

Record::Record() {}

Record::Record(char bytes[], RecordDesc recordDesc, DiskAddress page, int ndx) :
      recordDesc(recordDesc), page(page), ndx(ndx) {
   int byte = 0;
   for (int i = 0; i < recordDesc.numFields; i++) {
      Field field = recordDesc.fields[i];
      if (field.type == VARCHAR) {
         fields[field.name] = RecordField(string(&bytes[byte]));
         byte += field.size;
      }
      else if (field.type == INT) {
         int remainder = byte % 4;
         if (remainder)
            byte += 4 - remainder;
         int val;
         memcpy(&val, &bytes[byte], sizeof(int));
         fields[field.name] = RecordField(val);
         byte += 4;
      }
      else if (field.type == BOOLEAN) {
         int remainder = byte % 4;
         if (remainder)
            byte += 4 - remainder;
         int val;
         memcpy(&val, &bytes[byte], sizeof(int));
         fields[field.name] = RecordField((bool)val);
         byte += 4;
      }
      else {
         int remainder = byte % 8;
         if (remainder)
            byte += 8 - remainder;
         double val;
         memcpy(&val, &bytes[byte], sizeof(double));
         fields[field.name] = RecordField((ColumnType)field.type, val);
         byte += 8;
      }
   }
}

char *Record::getBytes(RecordDesc desc) {
   char *rtn = new char[sizeOfRecordDesc(desc)];

   for (int i = 0, byte = 0; i < desc.numFields; i++) {
      Field field = desc.fields[i];

      if (field.type == VARCHAR) {
         strncpy(&rtn[byte], fields[field.name].sVal.c_str(), field.size);
         rtn[byte + field.size - 1] = '\0'; // ensure null-terminated
         byte += field.size;
      }
      else if (field.type == INT) {
         int remainder = byte % 4;
         if (remainder)
            byte += 4 - remainder;
         int val = fields[field.name].iVal;
         memcpy(&rtn[byte], &val, sizeof(int));
         byte += 4;
      }
      else if (field.type == BOOLEAN) {
         int remainder = byte % 4;
         if (remainder)
            byte += 4 - remainder;
         int val = (int)fields[field.name].bVal;
         memcpy(&rtn[byte], &val, sizeof(int));
         byte += 4;
      }
      else {
         int remainder = byte % 8;
         if (remainder)
            byte += 8 - remainder;
         memcpy(&rtn[byte], &fields[field.name].fVal, sizeof(double));
         byte += 8;
      }
   }

   return rtn;
}


TupleIterator::TupleIterator(int fd) : fd(fd), _pageNdx(0) {
   heapHeaderGetNextPage(buffer, fd, &curPage);
   heapHeaderGetRecordDesc(buffer, fd, &recordDesc);

   heapHeaderGetRecordSize(buffer, fd, &recordSize);
   recordBytes = new char[recordSize];

   pHGetMaxRecords(buffer, curPage, &recordsPerPage);
   bitmap = new char[recordsPerPage / 8 + 1];

   startPage();
}

TupleIterator::~TupleIterator() {
   delete[] recordBytes;
   delete[] bitmap;
}

Record *TupleIterator::next() {
   if (curRecord == -1)
      return NULL;

   getRecord(buffer, curPage, curRecord, recordBytes);
   Record *record = new Record(recordBytes, recordDesc, curPage, curRecord);

   for (curRecord = curRecord + 1; curRecord < recordsPerPage; curRecord++) {
      if (bitmapIsSet(bitmap, curRecord)) {
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
   if (curPage.pageId == -1) { // no more pages
      curRecord = -1;
      return;
   }

   _pageNdx++;
   if (_pageNdx > pages.size())
      pages.push_back(curPage);

   pHGetBitmap(buffer, curPage, bitmap);
   for (curRecord = 0; curRecord < recordsPerPage; curRecord++) {
      if (bitmapIsSet(bitmap, curRecord)) {
         break;
      }
   }
   if (curRecord == recordsPerPage)
      curRecord = -1;
}
