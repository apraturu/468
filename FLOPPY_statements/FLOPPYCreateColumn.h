#ifndef __FLOPPY_CREATE_COLUMN_H__
#define __FLOPPY_CREATE_COLUMN_H__

typedef enum {
   INT,
   FLOAT,
   DATETIME,
   VARCHAR,
   BOOLEAN
} ColumnType;

class FLOPPYCreateColumn {
   public:
      FLOPPYCreateColumn();
      virtual ~FLOPPYCreateColumn();
      char *name;
      ColumnType type;
      int size;
};

#endif /* __FLOPPY_CREATE_COLUMN_H__ */
