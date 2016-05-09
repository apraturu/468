#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "parser.h"

void *err(char *msg) {
   fprintf(stderr, "%s\n", msg);
   return NULL;
}

// return 0 if id is not a valid identifier
int validateId(char *id) {
   if (!id || !isalpha(*id))
      return 0;

   while (*++id) {
      if (!isalnum(*id) && *id != '_')
         return 0;
   }

   return 1;
}

// return 0 if token is not a valid type
int setType(char *token, Attribute *att) {
   char *end;

   if (!strcasecmp(token, "int,")) {
      att->attType = INT;
      att->size = 4;
   }
   else if (!strcasecmp(token, "float,")) {
      att->attType = FLOAT;
      att->size = 8;
   }
   else if (!strncasecmp(token, "varchar(", 8)) {
      att->attType = VARCHAR;
      att->size = strtol(token + 8, &end, 10);
      if (end - token <= 8 || strcasecmp(end, "),"))
         return 0;
   }
   else if (!strcasecmp(token, "datetime,")) {
      att->attType = DATETIME;
      att->size = 8;
   }
   else if (!strcasecmp(token, "boolean,")) {
      att->attType = BOOLEAN;
      att->size = 4;
   }
   else
      return 0;

   return 1;
}

// parses:  (<att>[, <att>]*)
void *parseAttList(char *delim, Attribute **att) {
   char *token, *temp;

   token = strtok(NULL, delim);
   if (!token || *token++ != '(')
      return err("Missing '(' to start primary key");
   while (token && !strchr(token, ')') && (temp = strchr(token, ','))) {
      *temp = '\0'; // erase comma
      if (!validateId(token))
         return err("Invalid attribute name in key");
      *att = calloc(1, sizeof(Attribute));
      (*att)->attName = token;
      att = &(*att)->next;
      token = strtok(NULL, delim);
   }
   // last key attribute
   if (!token || !(temp = strchr(token, ')')))
      return err("Missing ')' to end key");
   *temp = '\0'; // erase paren
   if (!validateId(token))
      return err("Invalid attribute name in key");
   *att = calloc(1, sizeof(Attribute));
   (*att)->attName = token;

   return delim; // just return anything that isn't null
}

// returns null if parsing error
void *parseCreate(char *query) {
   char *token, *temp, *delim = " \n";
   tableDescription *table = calloc(1, sizeof(tableDescription));
   Attribute **att;
   ForeignKey **fk;

   token = strtok(query, delim);
   if (!token || strcasecmp(token, "create"))
      return err("Must start with CREATE");
   token = strtok(NULL, delim);
   if (!token || strcasecmp(token, "table"))
      return err("Must start with CREATE TABLE");

   table->tableName = strtok(NULL, delim);
   if (!validateId(table->tableName))
      return err("Invalid table name");
   
   token = strtok(NULL, delim);
   if (token && !strcasecmp(token, "volatile")) {
      table->isVolatile = 1;
      token = strtok(NULL, delim);
   }

   if (!token || strcasecmp(token, "("))
      return err("Missing '(' to start table description");

   // read attributes
   att = &table->attList;
   while ((token = strtok(NULL, delim)) && strcasecmp(token, "primary")) {
      // attName
      if (!validateId(token))
         return err("Invalid attribute name");
      *att = calloc(1, sizeof(Attribute));
      (*att)->attName = token;

      // attType
      token = strtok(NULL, delim);
      if (!setType(token, *att))
         return err("Invalid attribute type");
      att = &(*att)->next;
   }

   // read primary key
   if (!token)
      return err("Missing primary key");
   token = strtok(NULL, delim);
   if (!token || strcasecmp(token, "key"))
      return err("No 'KEY' following 'PRIMARY'");
   if (!parseAttList(delim, &table->pKey))
      return err("Parsing primary key failed.");

   // read foreign keys
   fk = &table->fKeys;
   while ((token = strtok(NULL, delim)) && strcasecmp(token, ");")) {
      if (strcasecmp(token, "foreign"))
         return err("Invalid start of foreign key");
      token = strtok(NULL, delim);
      if (!token || strcasecmp(token, "key"))
         return err("No 'KEY' following 'FOREIGN'");

      *fk = calloc(1, sizeof(ForeignKey));
      if (!parseAttList(delim, &(*fk)->key))
         return err("Parsing foreign key attributes failed.");

      token = strtok(NULL, delim);
      if (!token || strcasecmp(token, "references"))
         return err("Missing 'REFERENCES' in foreign key");

      token = strtok(NULL, delim);
      if (temp = strchr(token, ','))
         *temp = '\0'; // erase comma at end of table name
      if (!validateId(token))
         return err("Invalid table name in foreign key");
      (*fk)->tableName = token;

      fk = &(*fk)->next;
   }

   if (!token)
      return err("Missing ');' to end create table statement");

   return table;
}

// for testing purposes
void printTable(tableDescription *table) {
   Attribute *att;
   ForeignKey *fk;

   printf("Name: %s, isVolatile: %d\n", table->tableName, table->isVolatile);
   printf("Attributes:\n");
   for (att = table->attList; att; att = att->next)
      printf("\tName: %s, Type: %d, Size: %d\n", att->attName, att->attType, att->size);
   printf("Primary Key:\n");
   for (att = table->pKey; att; att = att->next)
      printf("\t%s\n", att->attName);
   printf("Foreign Keys:\n");
   for (fk = table->fKeys; fk; fk = fk->next) {
      printf("\tTable name: %s\n", fk->tableName);
      printf("\tAttributes:\n");
      for (att = fk->key; att; att = att->next)
         printf("\t\t%s\n", att->attName);
   }
}

// testing...
int main() {
   tableDescription *table;
   char arr[1000], *query = "CREATE TABLE Blah vOLaTile (\n"
   "blah INT,\n"
   "aoeu FLOAT,\n"
   "asneouthaneo VARCHAR(120),\n"
   "sarceoua DATETIME,\n"
   "saoetnnaeo BOOLEAN,\n"
   "PRIMARY KEY (blah, aoeu, asneouthaneo),\n"
   "FOREIGN KEY (blah, aoeu) references aoeu,\n"
   "FOREIGN KEY (blah) references aoeuaueo\n"
   ");";

   // can't pass string literal directly to parseCreate because string literals are read-only
   memcpy(arr, query, strlen(query) + 1);

   table = parseCreate(arr);

   if (table)
      printTable(table);
}
