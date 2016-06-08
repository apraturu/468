#include <algorithm>
#include <sstream>
#include <cstring>
#include <cfloat>
#include "heap.h"
#include "main.h"
#include "relAlg.h"

/* This file contains implementations of relational algebra operations. */


// Creates a new temporary heap file and return the file descriptor. Puts the
// name of the temporary file into filename.
fileDescriptor makeTempTable(Buffer *buf, char **filename, RecordDesc recordDesc);

// Find an attribute that may or may not have the table name prepended.
RecordField findAttrInRecord(Record *record, const char *attr);

Field findAttrInRecordDesc(RecordDesc recordDesc, char *attr);

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

int renameTable(fileDescriptor inTable, char *alias, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc, newRecordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &oldRecordDesc);

   newRecordDesc.numFields = oldRecordDesc.numFields;
   for (int i = 0; i < oldRecordDesc.numFields; i++) {
      string oldName(oldRecordDesc.fields[i].name);
      string newName(alias + oldName.substr(oldName.find('.')));
      strcpy(newRecordDesc.fields[i].name, newName.c_str());
      newRecordDesc.fields[i].size = oldRecordDesc.fields[i].size;
      newRecordDesc.fields[i].type = oldRecordDesc.fields[i].type;
   }

   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter(inTable); // open an iterator on input file

   for (Record *record = iter.next(); record; record = iter.next()) {
      Record newRecord;
      for (int i = 0; i < newRecordDesc.numFields; i++) {
         string oldName(oldRecordDesc.fields[i].name);
         string newName(newRecordDesc.fields[i].name);
         newRecord.fields[newName] = record->fields[oldName];
      }
      insertRecord(buffer, outFile, newRecord.getBytes(newRecordDesc), &temp);
   }
}

int project(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc, newRecordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &oldRecordDesc);

   // Construct newRecordDesc, the structure of output tuples, in order to create
   // the output temp table.
   newRecordDesc.numFields = (int)attributes->size();
   for (int i = 0; i < newRecordDesc.numFields; i++) {
      FLOPPYTableAttribute *t = attributes->at(i);

      if (t->tableName) {
         strcpy(newRecordDesc.fields[i].name, (string(t->tableName) + "." + t->attribute).c_str());

         for (int j = 0; j < oldRecordDesc.numFields; j++) {
            if (!strcmp(newRecordDesc.fields[i].name, oldRecordDesc.fields[j].name)) {
               newRecordDesc.fields[i].size = oldRecordDesc.fields[j].size;
               newRecordDesc.fields[i].type = oldRecordDesc.fields[j].type;
               break;
            }
         }
      }
      else {
         strcpy(newRecordDesc.fields[i].name, t->attribute);

         for (int j = 0; j < oldRecordDesc.numFields; j++) {
            string oldName = oldRecordDesc.fields[j].name;
            if (t->attribute == oldName.substr(oldName.find('.') + 1)) {
               newRecordDesc.fields[i].size = oldRecordDesc.fields[j].size;
               newRecordDesc.fields[i].type = oldRecordDesc.fields[j].type;
               break;
            }
         }
      }
   }

   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter(inTable);

   for (Record *record = iter.next(); record; record = iter.next()) {
      Record newRecord;

      for (auto aIter = attributes->begin(); aIter != attributes->end(); aIter++) {
         if ((*aIter)->tableName) {
            for (auto fIter = record->fields.begin(); fIter != record->fields.end(); fIter++) {
               if (fIter->first == string((*aIter)->tableName) + "." + (*aIter)->attribute) {
                  newRecord.fields[fIter->first] = fIter->second;
                  break;
               }
            }
         }
         else {
            for (auto fIter = record->fields.begin(); fIter != record->fields.end(); fIter++) {
               if (fIter->first.substr(fIter->first.find('.') + 1) == (*aIter)->attribute) {
                  newRecord.fields[(*aIter)->attribute] = fIter->second;
                  break;
               }
            }
         }
      }

      insertRecord(buffer, outFile, newRecord.getBytes(newRecordDesc), &temp);
   }
}

int product(fileDescriptor inTable1, fileDescriptor inTable2, fileDescriptor *outTable) {
   //joinNestedLoops(inTable1, inTable2, NULL, outTable);
   joinOnePass(inTable1, inTable2, NULL, outTable);
   // TODO change back to joinNestedLoops
}

/*appends all of record1's fields to record2*/
void combineRecords(Record *rec1, Record *rec2) {
   for (auto fIter = rec1->fields.begin(); fIter != rec1->fields.end(); fIter++) {
      rec2->fields[fIter->first] = fIter->second;
   }
}

int joinOnePass(fileDescriptor inTable1, fileDescriptor inTable2,
                FLOPPYNode *condition, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc1, oldRecordDesc2, newRecordDesc;
   int numBlocksT1, numBlocksT2, i, j;

   // swap(inTable1, inTable2) if necessary to make it so table 1 is the bigger one
   // (the table with bigger numBlocks)
   heapHeaderGetNumBlocks(buffer, inTable1, &numBlocksT1);
   heapHeaderGetNumBlocks(buffer, inTable2, &numBlocksT2);
   
   if (numBlocksT1 < numBlocksT2)
      swap(inTable1, inTable2);
   
   heapHeaderGetRecordDesc(buffer, inTable1, &oldRecordDesc1);
   heapHeaderGetRecordDesc(buffer, inTable2, &oldRecordDesc2);
   
   // forms newRecordDesc by combining the fields from oldRecordDesc1 and oldRecordDesc2 (all of oldRecordDesc1 stuff comes AFTER oldRecordDesc2 stuff in newRecordDesc)
   newRecordDesc.numFields = oldRecordDesc1.numFields + oldRecordDesc2.numFields;
   for (i = 0; i < oldRecordDesc2.numFields; i++) {
      memcpy(newRecordDesc.fields + i, oldRecordDesc2.fields + i, sizeof(Field));
   }
   j = i;
   for (i = 0; i < oldRecordDesc1.numFields; i++, j++) {
      memcpy(newRecordDesc.fields + j, oldRecordDesc1.fields + i, sizeof(Field));
   }
   
   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter1(inTable1);
      
   // Iterate through all tuples, outputting those that match the given condition
   for (Record *record1 = iter1.next(); record1; record1 = iter1.next()) {
      TupleIterator iter2(inTable2);
      for (Record *record2 = iter2.next(); record2; record2 = iter2.next()) {
         combineRecords(record1, record2);
            
         if (checkCondition(record2, condition))
            insertRecord(buffer, outFile, record2->getBytes(newRecordDesc), &temp); 
      }      
   }   
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
   int numBlocksT1, numBlocksT2, i, j;

   // make inTable1 be the bigger table
   heapHeaderGetNumBlocks(buffer, inTable1, &numBlocksT1);
   heapHeaderGetNumBlocks(buffer, inTable2, &numBlocksT2);
   
   if (numBlocksT1 < numBlocksT2)
      swap(inTable1, inTable2);
   
   heapHeaderGetRecordDesc(buffer, inTable1, &oldRecordDesc1);
   heapHeaderGetRecordDesc(buffer, inTable2, &oldRecordDesc2);

   // TODO form newRecordDesc by combining the fields from oldRecordDesc1 and oldRecordDesc2
   // justin double check this [pierson]
   //copies oldRecordDesc2 stuff into newRecordDesc
   //newRecordDesc.numFields = (int)(oldRecordDesc1.numFields + oldRecordDesc2.numFields);
   //for (i = 0; i < oldRecordDesc2.numFields; i++) {
   //   strcpy(newRecordDesc.fields + i, oldRecordDesc2.fields + i, sizeof(fields));
   //}
   ////copies oldRecordDesc1 stuff into newRecordDesc after oldRecordDesc2's stuff
   //j = i;
   //for (i = 0; i < oldRecordDesc1.numFields; i++) {
   //   strcpy(newRecordDesc.fields + j, oldRecordDesc1.fields + i, sizeof(fields));
   //}
   
   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter1(inTable1);
   TupleIterator iter2(inTable2);

   // The basic algorithm:  This is where TupleIterator's pageNdx and goToPage will be useful.
   // for each group of (M - 1) blocks of table 2:
   //    for each tuple in table 1:
   //       join with each tuple in the current group of (M - 1) blocks
}

static vector<AggResult> initAggResults(vector<Aggregate> *aggregates,
                                 RecordDesc recordDesc) {
   vector<AggResult> aggResults;
   for (int i = 0; i < aggregates->size(); i++) {
      switch (aggregates->at(i).op) {
         case CountAggregate:
         case CountStarAggregate:
            aggResults.push_back(RecordField(0));
            break;
         case MaxAggregate:
            if (recordDesc.fields[i].type == INT)
               aggResults.push_back(RecordField(INT32_MIN));
            else
               aggResults.push_back(RecordField(FLOAT, DBL_MIN));
            break;
         case MinAggregate:
            if (recordDesc.fields[i].type == INT)
               aggResults.push_back(RecordField(INT32_MAX));
            else
               aggResults.push_back(RecordField(FLOAT, DBL_MAX));
            break;
         default:
            if (recordDesc.fields[i].type == INT)
               aggResults.push_back(RecordField(0));
            else
               aggResults.push_back(RecordField(FLOAT, 0.0));
            break;
      }
   }

   return aggResults;
}

static void updateAggResults(vector<AggResult> &aggResults, Record *record,
                             vector<Aggregate> *aggregates, RecordDesc newRecordDesc) {
   for (int i = 0; i < aggregates->size(); i++) {
      switch (aggregates->at(i).op) {
         case CountAggregate:
         case CountStarAggregate:
            aggResults[i].field.iVal++;
            break;
         case MaxAggregate:
            if (newRecordDesc.fields[i].type == INT)
               aggResults[i].field.iVal =
                     max(aggResults[i].field.iVal,
                         findAttrInRecord(record, aggregates->at(i).attr).iVal);
            else
               aggResults[i].field.fVal =
                     max(aggResults[i].field.fVal,
                         findAttrInRecord(record, aggregates->at(i).attr).fVal);
            break;
         case MinAggregate:
            if (newRecordDesc.fields[i].type == INT)
               aggResults[i].field.iVal =
                     min(aggResults[i].field.iVal,
                         findAttrInRecord(record, aggregates->at(i).attr).iVal);
            else
               aggResults[i].field.fVal =
                     min(aggResults[i].field.fVal,
                         findAttrInRecord(record, aggregates->at(i).attr).fVal);
            break;
         case SumAggregate:
            if (newRecordDesc.fields[i].type == INT)
               aggResults[i].field.iVal += findAttrInRecord(record, aggregates->at(i).attr).iVal;
            else
               aggResults[i].field.fVal += findAttrInRecord(record, aggregates->at(i).attr).fVal;
            break;
         case AverageAggregate:
            if (newRecordDesc.fields[i].type == INT)
               aggResults[i].field.iVal += findAttrInRecord(record, aggregates->at(i).attr).iVal;
            else
               aggResults[i].field.fVal += findAttrInRecord(record, aggregates->at(i).attr).fVal;
            aggResults[i].count++;
            break;
      }
   }
}

static void addAggsToRecord(Record &record, vector<AggResult> &aggResults,
                            vector<Aggregate> *aggregates, RecordDesc newRecordDesc) {
   for (int i = 0; i < aggregates->size(); i++) {
      if (aggregates->at(i).op == AverageAggregate) {
         if (newRecordDesc.fields[i].type == INT)
            aggResults[i].field.iVal /= aggResults[i].count;
         else
            aggResults[i].field.fVal /= aggResults[i].count;
      }
      record.fields[aggregates->at(i).toString()] = aggResults[i].field;
   }
}

// Note: group can be null, to signify aggregation with no group by clause.
int groupOnePass(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *group,
                 vector<Aggregate> *aggregates, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc oldRecordDesc, newRecordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &oldRecordDesc);

   // Make newRecordDesc
   newRecordDesc.numFields = (group ? group->size() : 0) + aggregates->size();

   int i = 0, j;
   if (group) {
      for (; i < group->size(); i++) {
         FLOPPYTableAttribute *t = group->at(i);

         if (t->tableName) {
            strcpy(newRecordDesc.fields[i].name, (string(t->tableName) + "." + t->attribute).c_str());

            for (j = 0; j < oldRecordDesc.numFields; j++) {
               if (!strcmp(newRecordDesc.fields[i].name, oldRecordDesc.fields[j].name)) {
                  newRecordDesc.fields[i].size = oldRecordDesc.fields[j].size;
                  newRecordDesc.fields[i].type = oldRecordDesc.fields[j].type;
                  break;
               }
            }
         }
         else {
            strcpy(newRecordDesc.fields[i].name, t->attribute);

            for (j = 0; j < oldRecordDesc.numFields; j++) {
               string oldName = oldRecordDesc.fields[j].name;
               if (t->attribute == oldName.substr(oldName.find('.') + 1)) {
                  newRecordDesc.fields[i].size = oldRecordDesc.fields[j].size;
                  newRecordDesc.fields[i].type = oldRecordDesc.fields[j].type;
                  break;
               }
            }
         }
      }
   }
   j = i;
   for (i = 0; i < aggregates->size(); i++, j++) {
      strcpy(newRecordDesc.fields[j].name, aggregates->at(i).toString().c_str());
      if (aggregates->at(i).op == CountAggregate || aggregates->at(i).op == CountStarAggregate) {
         newRecordDesc.fields[j].size = 4;
         newRecordDesc.fields[j].type = INT;
      }
      else {
         Field field = findAttrInRecordDesc(oldRecordDesc, aggregates->at(i).attr);
         newRecordDesc.fields[j].size = field.size;
         newRecordDesc.fields[j].type = field.type;
      }
   }

   *outTable = makeTempTable(buffer, &outFile, newRecordDesc);

   TupleIterator iter(inTable); // open an iterator on input file

   if (!group) {
      vector<AggResult> aggResults = initAggResults(aggregates, newRecordDesc);

      for (Record *record = iter.next(); record; record = iter.next())
         updateAggResults(aggResults, record, aggregates, newRecordDesc);

      Record record;
      addAggsToRecord(record, aggResults, aggregates, newRecordDesc);

      insertRecord(buffer, outFile, record.getBytes(newRecordDesc), &temp);
   }
   else {
      map<vector<RecordField>, vector<AggResult>> groups;

      for (Record *record = iter.next(); record; record = iter.next()) {
         vector<RecordField> groupValues;

         for (i = 0; i < group->size(); i++) {
            string name(group->at(i)->tableName ?
                        string(group->at(i)->tableName) + "." + group->at(i)->attribute :
                        group->at(i)->attribute);
            groupValues.push_back(findAttrInRecord(record, name.c_str()));
         }

         if (!groups.count(groupValues))
            groups[groupValues] = initAggResults(aggregates, newRecordDesc);

         updateAggResults(groups[groupValues], record, aggregates, newRecordDesc);
      }

      for (auto gIter = groups.begin(); gIter != groups.end(); gIter++) {
         Record record;

         for (i = 0; i < gIter->first.size(); i++) {
            string name(group->at(i)->tableName ?
                        string(group->at(i)->tableName) + "." + group->at(i)->attribute :
                        group->at(i)->attribute);
            record.fields[name] = gIter->first[i];
         }

         addAggsToRecord(record, gIter->second, aggregates, newRecordDesc);

         insertRecord(buffer, outFile, record.getBytes(newRecordDesc), &temp);
      }
   }
}

int groupMultiPass(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *group,
                   vector<Aggregate> *aggregates, fileDescriptor *outTable) {
   // TODO
}

class RecordSorter {
public:
   RecordSorter(vector<string> attributes, RecordDesc recordDesc) : attributes(attributes) {}

   bool operator()(Record *r1, Record *r2) {
      for (int i = 0; i < attributes.size(); i++) {
         if (r1->fields[attributes[i]] == r2->fields[attributes[i]])
            continue;
         return r1->fields[attributes[i]] < r2->fields[attributes[i]];
      }
      return false;
   }

private:
   vector<string> attributes;
};


int duplicateElimination(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes,
                         fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc recordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &recordDesc);

   *outTable = makeTempTable(buffer, &outFile, recordDesc);

   vector<Record *> records;
   TupleIterator iter(inTable);

   for (Record *record = iter.next(); record; record = iter.next())
      records.push_back(record);

   vector<string> sortAttributes;
   if (attributes) {
      for (int i = 0; i < attributes->size(); i++) {
         FLOPPYTableAttribute *t = attributes->at(i);
         if (t->tableName) {
            sortAttributes.push_back(string(t->tableName) + "." + t->attribute);
         }
         else {
            Field field = findAttrInRecordDesc(recordDesc, t->attribute);
            sortAttributes.push_back(string(field.name));
         }
      }
   }
   else {
      for (int i = 0; i < recordDesc.numFields; i++)
         sortAttributes.push_back(string(recordDesc.fields[i].name));
   }
   sort(records.begin(), records.end(), RecordSorter(sortAttributes, recordDesc));

   Record *prev = NULL;
   for (int i = 0; i < records.size(); i++) {
      if (prev) {
         bool equal = true;
         for (int j = 0; j < sortAttributes.size(); j++) {
            if (prev->fields[sortAttributes[j]] != records[i]->fields[sortAttributes[j]]) {
               equal = false;
               break;
            }
         }
         if (equal)
            continue;
      }

      insertRecord(buffer, outFile, records[i]->getBytes(recordDesc), &temp);
      prev = records[i];
   }
}

int sortTable(fileDescriptor inTable, vector<FLOPPYTableAttribute *> *attributes, fileDescriptor *outTable) {
   char *outFile;
   DiskAddress temp;
   RecordDesc recordDesc;

   heapHeaderGetRecordDesc(buffer, inTable, &recordDesc);

   *outTable = makeTempTable(buffer, &outFile, recordDesc);

   vector<Record *> records;
   TupleIterator iter(inTable);

   for (Record *record = iter.next(); record; record = iter.next())
      records.push_back(record);

   vector<string> sortAttributes;
   for (int i = 0; i < attributes->size(); i++) {
      FLOPPYTableAttribute *t = attributes->at(i);
      if (t->tableName) {
         sortAttributes.push_back(string(t->tableName) + "." + t->attribute);
      }
      else {
         Field field = findAttrInRecordDesc(recordDesc, t->attribute);
         sortAttributes.push_back(string(field.name));
      }
   }
   sort(records.begin(), records.end(), RecordSorter(sortAttributes, recordDesc));

   for (int i = 0; i < records.size(); i++)
      insertRecord(buffer, outFile, records[i]->getBytes(recordDesc), &temp);
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

   return createHeapFile(buf, *filename, recordDesc, false, NULL, NULL);
}

// Find an attribute that may not have the table name prepended.
RecordField findAttrInRecord(Record *record, const char *attr) {
   for (auto iter = record->fields.begin(); iter != record->fields.end(); iter++) {
      if (iter->first == attr ||
            iter->first.substr(iter->first.find('.') + 1) == attr)
         return iter->second;
   }
}

Field findAttrInRecordDesc(RecordDesc recordDesc, char *attr) {
   for (int i = 0; i < recordDesc.numFields; i++) {
      string name = recordDesc.fields[i].name;
      if (name == attr ||
            name.substr(name.find('.') + 1) == attr)
         return recordDesc.fields[i];
   }
}

string Aggregate::toString() {
   switch (op) {
      case CountAggregate:
         return string("COUNT(") + attr + ")";
      case CountStarAggregate:
         return "COUNT(*)";
      case AverageAggregate:
         return string("AVG(") + attr + ")";
      case MinAggregate:
         return string("MIN(") + attr + ")";
      case MaxAggregate:
         return string("MAX(") + attr + ")";
      case SumAggregate:
         return string("SUM(") + attr + ")";
   }
}

RecordField evalExpr(Record *record, FLOPPYNode *expr) {
   if (expr->_type == ValueNode) {
      FLOPPYTableAttribute *t;
      switch (expr->value->type()) {
         case AttributeValue:
            return findAttrInRecord(record, expr->value->sVal);
         case TableAttributeValue:
            t = expr->value->tableAttribute;
            if (t->tableName)
               return record->fields[string(t->tableName) + "." + t->attribute];
            return findAttrInRecord(record, t->attribute);
         case StringValue:
            return RecordField(string(expr->value->sVal));
         case IntValue:
            return RecordField((int)expr->value->iVal);
         case FloatValue:
            return RecordField(FLOAT, expr->value->fVal);
         case BooleanValue:
            return RecordField(expr->value->bVal);
      }
   }
   else if (expr->_type == AggregateNode) {
      return record->fields[
            Aggregate(expr->aggregate.op,
                      expr->aggregate.value ? expr->aggregate.value->sVal : NULL)
                  .toString()];
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

