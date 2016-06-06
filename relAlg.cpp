#include <algorithm>
#include <sstream>
#include <cstring>
#include "heap.h"
#include "main.h"
#include "relAlg.h"
#include "TupleIterator.h"
#include "FLOPPY_statements/statements.h"

/* This file contains implementations of relational algebra operations. */


// Creates a new temporary heap file and return the file descriptor. Puts the
// name of the temporary file into filename.
fileDescriptor makeTempTable(Buffer *buf, char **filename, RecordDesc recordDesc);

// Returns true if the given record satisfies the given boolean condition.
bool checkCondition(Record *record, FLOPPYNode *cond);



/* From Justin to people working on this file:
 *
 * Call makeTempTable to create the temporary table into which output will be
 * written. Put the fd into the outTable parameter that each function has.
 * See how I use it in project. Note that the third parameter is a RecordDesc
 * (from heap.h) telling the structure of each output tuple. For simple things
 * like select and sort, you can just reuse the same RecordDesc from the input
 * table. But for most, the output tuples look different than the input tuples,
 * so you'll have to put together your own RecordDesc to pass in to makeTempTable.
 * In project, I do this by looping through the given attributes, and for each one,
 * copying the matching Field from the input RecordDesc into the new RecordDesc.
 *
 * Several of these functions take in a pointer to a vector. Vector is basically
 * C++'s ArrayList. The angle brackets work just like they do with Java's generics,
 * so vector<char *> means a list of char pointers (strings). You can look at how I
 * used the vector in project; here's hopefully the only things you will need
 * to know how to do with them. Given the declaration:  vector<char *> vec;
 *    vec->size() // number of elements
 *    vec->at(i) // get the element at index i
 *    find(vec->begin(), vec->end(), str) != vec->end() // true if vec contains str
 *
 * As you can see in my selectScan and project implementations, I have a TupleIterator
 * class you can use. Construct an iterator for a file with the line:
 *   TupleIterator iter(fd);
 * You can get the next record by calling iter.next(). This returns a Record *, which
 * I talk about below. iter.next() will return NULL when there are no more tuples in
 * the file. This means you can iterate through a file one tuple at a time by writing
 *   for (Record *record = iter.next(); record; record = iter.next() { ... }
 *
 * Some algorithms, especially multi-pass stuff, will need to not iterate straight
 * through the entire file. For example, for one of the files in nested loops join,
 * you need to iterate multiple times through the first chunk of (M - 1) blocks, then
 * repeatedly iterate through the next chunk of (M - 1) blocks, and so on. In order
 * to do this, TupleIterator has two additional methods dealing with page/block numbers.
 * These have 1 as the first page, not 0.
 *  - pageNdx() // returns the page number from which the next tuple will be taken.
 *              // In other words, after taking the last tuple from page 5, this function
 *              // will then return 6.
 *  - goToPage(int page) // Moves back to the top of the given page number. Only ever use
 *                       // this to go backwards, it won't work right for going forward.
 *
 * About the Record thing that TupleIterator.next() returns:
 * record->fields is a map<string, RecordField>. It's a map from attribute name to
 * attribute value, where the value is given in a RecordField.  A RecordField has a
 * `type` field that is an enum {INT, FLOAT, VARCHAR, DATETIME, BOOLEAN}.  If the
 * RecordField is an INT, it contains its int value in the `iVal` field. If its a
 * FLOAT it uses `fVal`. VARCHARs use `sVal`, BOOLEANs use `bVal`, DATETIME is same as FLOAT.
 *
 * A couple things about maps:
 *  - record->fields[str]  gets you the value with key str. It can also be used to insert/update
 *    stuff in the map with an assignment:
 *      record->fields["name"] = RecordField("Justin");
 *      record->fields["age"] = RecordField(5);
 *  - You can iterate through all key/value pairs in a map. See project for a for loop
 *    declaring fIter as an iterator through the fields map.  Once you have that iterator,
 *    fIter->first gets you the key, and fIter->second gets you the value.
 *    That loop is a little complex with an extra |next| variable, because calling erase to
 *    delete something from a map invalidates any iterator that was pointing at the deleted
 *    value. This means you have to already advance an iterator to the following value before
 *    deleting something. If you don't need to delete anything from the map, you can make the
 *    simpler loop:
 *       for (auto iter = record->fields.begin(); iter != record->fields.end(); iter++) { ... }
 *
 * Select and Join operations take in a FLOPPYNode parameter, which is a C++
 * class from Andrew's parser. I am passing it in to represent the boolean
 * condition against which tuples need to be checked. The only thing you
 * ever have to do with it is pass it in as a parameter to the helper function
 * checkCondition, which will return true if the given record satisfies that
 * condition.
 * Note for join operations: checkCondition only takes in one record. Remember
 * that in order to output a tuple from a join, you will have to combine a tuple
 * from each of the two input tables. When you are comparing a tuple from each
 * table to see whether it should get output, combine the two first and then pass
 * in the combined thing into checkCondition.
 *
 * Grouping operations use an Aggregate struct, which contains two fields:
 * 1. op - an enum value to tell which type of aggregation operation it is. Can
 *    be one of the following: CountAggregate, CountStarAggregate,
 *    AverageAggregate, MinAggregate, MaxAggregate, SumAggregate.
 *    CountStarAggregate means COUNT(*). The rest should be self-explanatory.
 * 2. attr - the name of the attribute on which the aggregation is performed.
 *    Ignore this field if op == CountStarAggregate, since COUNT(*) just counts
 *    all the tuples.
 * For grouping operations, if you for example have to output a column giving MAX(age),
 * make it a column literally with the string "MAX(age)" as the attribute name. Note
 * that C++ lets you concatenate strings, so you could form the above string by writing:
 *   string("MAX(") + attrName + ")";
 */



int selectScan(fileDescriptor inTable, FLOPPYNode *cond, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc recordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &recordDesc);

   // can just reuse recordDesc for new file because output tuples have same structure
   *outTable = makeTempTable(buffer, &outFile, recordDesc);

   TupleIterator iter(inTable); // open an iterator on input file

   // Iterate through all tuples, outputting those that match the given condition
   for (Record *record = iter.next(); record; record = iter.next()) {
      if (checkCondition(record, cond))
         insertRecord(buffer, outFile, record->getBytes(recordDesc), &temp);
   }
}

int project(fileDescriptor inTable, vector<char *> *attributes, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc, newRecordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &oldRecordDesc);

   // Construct newRecordDesc, the structure of output tuples, in order to create
   // the output temp table.
   newRecordDesc.numFields = (int)attributes->size();
   for (int i = 0; i < newRecordDesc.numFields; i++) {
      strcpy(newRecordDesc.fields[i].name, attributes->at(i));
      Field field;
      for (int j = 0; j < oldRecordDesc.numFields; j++) {
         if (!strcmp(newRecordDesc.fields[i].name, oldRecordDesc.fields[j].name)) {
            field = oldRecordDesc.fields[j];
            break;
         }
      }
      newRecordDesc.fields[i].size = field.size;
      newRecordDesc.fields[i].type = field.type;
   }

   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter(inTable);

   for (Record *record = iter.next(); record; record = iter.next()) {
      // Remove any fields from this tuple that aren't included in attributes
      for (auto fIter = record->fields.begin(), next = fIter;
           fIter != record->fields.end(); fIter = next) {
         next++;
         if (find(attributes->begin(), attributes->end(), fIter->first) == attributes->end()) {
            record->fields.erase(fIter);
         }
      }

      insertRecord(buffer, outFile, record->getBytes(newRecordDesc), &temp);
   }
}

int duplicateElimination(fileDescriptor inTable, fileDescriptor *outTable) {
   // TODO
}

int product(fileDescriptor inTable1, fileDescriptor inTable2, fileDescriptor *outTable) {
   joinNestedLoops(inTable1, inTable2, NULL, outTable);
}

int joinOnePass(fileDescriptor inTable1, fileDescriptor inTable2,
                FLOPPYNode *condition, fileDescriptor *outTable) {
   // TODO
}

int joinMultiPass(fileDescriptor inTable1, fileDescriptor inTable2,
                  FLOPPYNode *condition, fileDescriptor *outTable) {
   // TODO
}

int joinNestedLoops(fileDescriptor inTable1, fileDescriptor inTable2,
                    FLOPPYNode *condition, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc1, oldRecordDesc2, newRecordDesc;

   // TODO swap(inTable1, inTable2) if necessary to make it so table 1 is the bigger one
   // (the table with bigger numBlocks)

   heapHeaderGetRecordDesc(buffer, inTable1, &oldRecordDesc1);
   heapHeaderGetRecordDesc(buffer, inTable2, &oldRecordDesc2);

   // TODO form newRecordDesc by combining the fields from oldRecordDesc1 and oldRecordDesc2

   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter1(inTable1);
   TupleIterator iter2(inTable2);

   // The basic algorithm:  This is where TupleIterator's pageNdx and goToPage will be useful.
   // for each group of (M - 1) blocks of table 2:
   //    for each tuple in table 1:
   //       join with each tuple in the current group of (M - 1) blocks
   //
   // When checking tuples against the join condition, only actually call checkCondition if
   // condition is not NULL. If condition is NULL, include every tuple without checking. This
   // is because
}

int groupOnePass(fileDescriptor inTable, vector<char *> *group,
                 vector<Aggregate> *aggregates, fileDescriptor *outTable) {
   // TODO
}

int groupMultiPass(fileDescriptor inTable, vector<char *> *group,
                   vector<Aggregate> *aggregates, fileDescriptor *outTable) {
   // TODO
}

int sortTable(fileDescriptor inTable, vector<char *> *attributes, fileDescriptor *outTable) {
   // TODO
}

int limitTable(fileDescriptor inTable, int k, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc recordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &recordDesc);

   *outTable = makeTempTable(buffer, &outFile, recordDesc);

   TupleIterator iter(inTable);

   for (Record *record = iter.next(); record && k; record = iter.next(), k--)
      insertRecord(buffer, outFile, record->getBytes(recordDesc), &temp);
}


// HELPER FUNCTIONS

fileDescriptor makeTempTable(Buffer *buf, char **filename, RecordDesc recordDesc) {
   static int id = 0; // Give every temporary table a unique id to ensure unique names

   stringstream stream;
   stream << "_temp_" << id++;
   *filename = (char *)(new string(stream.str()))->c_str();

   return createHeapFile(buf, *filename, recordDesc, false);
}

RecordField evalExpr(Record *record, FLOPPYNode *expr) {
   if (expr->_type == ValueNode) {
      switch (expr->value->type()) {
         case AttributeValue:
            return record->fields[expr->value->sVal];
         case StringValue:
            return RecordField(expr->value->sVal);
         case IntValue:
            return RecordField((int)expr->value->iVal);
         case FloatValue:
            return RecordField(FLOAT, expr->value->fVal);
         case BooleanValue:
            return RecordField(expr->value->bVal);
      }
   }
   else if (expr->_type == AggregateNode) {
      string str;
      switch (expr->aggregate.op) {
         case CountAggregate:
            str = string("COUNT(") + expr->aggregate.value->sVal + ")";
            break;
         case CountStarAggregate:
            str = "COUNT(*)";
            break;
         case AverageAggregate:
            str = string("AVG(") + expr->aggregate.value->sVal + ")";
            break;
         case MinAggregate:
            str = string("MIN(") + expr->aggregate.value->sVal + ")";
            break;
         case MaxAggregate:
            str = string("MAX(") + expr->aggregate.value->sVal + ")";
            break;
         case SumAggregate:
            str = string("SUM(") + expr->aggregate.value->sVal + ")";
      }
      return record->fields[str];
   }
   else {
      switch (expr->node.op) {
         case ParenthesisOperator:
            return evalExpr(record, expr->node.left);
         case PlusOperator:
            return evalExpr(record, expr->node.left) + evalExpr(record, expr->node.right);
         case MinusOperator:
            return evalExpr(record, expr->node.left) - evalExpr(record, expr->node.right);
         case TimesOperator:
            return evalExpr(record, expr->node.left) * evalExpr(record, expr->node.right);
         case DivideOperator:
            return evalExpr(record, expr->node.left) / evalExpr(record, expr->node.right);
         case ModOperator:
            return evalExpr(record, expr->node.left) % evalExpr(record, expr->node.right);
      }
   }
   return RecordField(0); // shouldn't ever reach this point with well-formed queries.
}

bool checkCondition(Record *record, FLOPPYNode *cond) {
   if (cond == NULL)
      return true;

   if (cond->_type == ValueNode)
      return cond->value->bVal;

   switch (cond->node.op) {
      case AndOperator:
         return checkCondition(record, cond->node.left) &&
                checkCondition(record, cond->node.right);
      case NotOperator:
         return !checkCondition(record, cond->node.left);
      case GreaterThanOperator:
         return evalExpr(record, cond->node.left) > evalExpr(record, cond->node.right);
      case GreaterThanEqualOperator:
         return evalExpr(record, cond->node.left) >= evalExpr(record, cond->node.right);
      case LessThanOperator:
         return evalExpr(record, cond->node.left) < evalExpr(record, cond->node.right);
      case LessThanEqualOperator:
         return evalExpr(record, cond->node.left) <= evalExpr(record, cond->node.right);
      case EqualOperator:
         return evalExpr(record, cond->node.left) == evalExpr(record, cond->node.right);
      case NotEqualOperator:
         return evalExpr(record, cond->node.left) != evalExpr(record, cond->node.right);
      case ParenthesisOperator:
         return checkCondition(record, cond->node.left);
      default:
         return false;
   }
}

