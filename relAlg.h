#include <vector>
#include "FLOPPYParser.h"
#include "libTinyFS.h"
#include "TupleIterator.h"

using namespace std;

struct Aggregate {
   FLOPPYAggregateOperator op;
   char *attr;

   Aggregate(FLOPPYAggregateOperator op, char *attr) : op(op), attr(attr) {}

   string toString();
};

struct AggResult {
   RecordField field;
   int count;

   AggResult(RecordField field) : field(field), count(0) {}
};

int selectScan(fileDescriptor inTable, FLOPPYNode *condition, fileDescriptor *outTable);
int selectIndex(fileDescriptor inTable, FLOPPYNode *condition, fileDescriptor index, fileDescriptor *outTable);

int project(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes, fileDescriptor *outTable);

int duplicateElimination(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes, fileDescriptor *outTable);

int product(fileDescriptor inTable1, fileDescriptor inTable2, fileDescriptor *outTable);

int joinOnePass(fileDescriptor inTable1, fileDescriptor inTable2, FLOPPYNode *condition, fileDescriptor *outTable);
int joinMultiPass(fileDescriptor inTable1, fileDescriptor inTable2, FLOPPYNode *condition, fileDescriptor *outTable);
int joinNestedLoops(fileDescriptor inTable1, fileDescriptor inTable2, FLOPPYNode *condition, fileDescriptor *outTable);

int groupOnePass(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *group, vector<Aggregate> *aggregates, fileDescriptor *outTable);
int groupMultiPass(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *group, vector<Aggregate> *aggregates, fileDescriptor *outTable);

int sortTable(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes, fileDescriptor *outTable);

int limitTable(fileDescriptor inTable, int k, fileDescriptor *outTable);


RecordField evalExpr(Record *record, FLOPPYNode *expr);
bool checkCondition(Record *record, FLOPPYNode *cond);
