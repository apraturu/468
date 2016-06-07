#ifndef __FLOPPY_CREATE_TABLE_STATEMENT_H__
#define __FLOPPY_CREATE_TABLE_STATEMENT_H__

#include <string>
#include <vector>

#include "FLOPPYStatement.h"

class FLOPPYCreateColumn;
class FLOPPYPrimaryKey;
class FLOPPYForeignKey;

struct CreateTableAdditionalFunctionality {
   bool volatileFlag;
   bool indexOnlyFlag;
   bool splitFlag;
};

class FLOPPYCreateColumn;

class FLOPPYCreateTableStatement : public FLOPPYStatement {
   public:
      FLOPPYCreateTableStatement();
      virtual ~FLOPPYCreateTableStatement();

      // Make all member fields public, because ez access.
      std::string tableName;
      CreateTableAdditionalFunctionality *flags;
      std::vector<FLOPPYCreateColumn *> *columns;
      FLOPPYPrimaryKey *pk;
      std::vector<FLOPPYForeignKey *> *fk;
}; 
#endif /* __FLOPPY_CREATE_TABLE_STATEMENT_H__ */
