#ifndef __FLOPPY_DELETE_STATEMENT_H__
#define __FLOPPY_DELETE_STATEMENT_H__

#include "FLOPPYStatement.h"

class FLOPPYNode;

class FLOPPYDeleteStatement : public FLOPPYStatement {
   public:
      FLOPPYDeleteStatement();
      virtual ~FLOPPYDeleteStatement();

      char *name;
      FLOPPYNode *where;
};

#endif /* __FLOPPY_DELETE_STATEMENT_H__ */
