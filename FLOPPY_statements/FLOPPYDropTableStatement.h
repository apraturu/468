#ifndef __FLOPPY_DROP_TABLE_STATEMENT_H__
#define __FLOPPY_DROP_TABLE_STATEMENT_H__

#include "FLOPPYStatement.h"

class FLOPPYDropTableStatement : public FLOPPYStatement {
   public:
      FLOPPYDropTableStatement();
      virtual ~FLOPPYDropTableStatement();

      // Make all member fields public, because ez access.
      char *table;
}; 
#endif /* __FLOPPY_DROP_TABLE_STATEMENT_H__ */
