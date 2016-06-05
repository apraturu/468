#ifndef __FLOPPY_STATEMENT_H__
#define __FLOPPY_STATEMENT_H__

#include <vector>

typedef enum {
   ErrorStatement,
   CreateTableStatement,
   DropTableStatement,
   CreateIndexStatement,
   DropIndexStatement,
   InsertStatement,
   DeleteStatement,
   UpdateStatement,
   SelectStatement 
} StatementType;

/**
 * Base struct for every FLOPPY statement
 */
struct FLOPPYStatement {
   public:
      FLOPPYStatement(StatementType type) :
         _type(type) {};

      virtual ~FLOPPYStatement() {}

      virtual StatementType type() {
         return _type;
      }

   private:
      StatementType _type;
};

#endif /* __FLOPPY_STATEMENT_H__ */
