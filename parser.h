enum Type {INT, FLOAT, VARCHAR, DATETIME, BOOLEAN};

typedef struct Attribute {
   char *attName;
   enum Type attType;
   int size; // only used for varchar
   struct Attribute *next;
} Attribute;

typedef struct FK {
   char *tableName; // table the key references
   Attribute *key;  // key attributes in this table
   struct FK *next; // next key in linked list
} ForeignKey;

typedef struct {
   char *tableName;
   Attribute *attList;
   Attribute *pKey;
   ForeignKey *fKeys;
   int isVolatile;
} tableDescription;

void *parseCreate(char *query);
