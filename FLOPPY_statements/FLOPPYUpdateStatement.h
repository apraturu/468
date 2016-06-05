#ifndef __FLOPPY_UPDATE_STATEMENT_H__
#define __FLOPPY_UPDATE_STATEMENT_H__

#include "FLOPPYStatement.h"

class FLOPPYNode;

class FLOPPYUpdateStatement : public FLOPPYStatement {
   public:
      FLOPPYUpdateStatement();
      virtual ~FLOPPYUpdateStatement();

      char *tableName;
      char *attributeName;
      FLOPPYNode *attributeExpression;
      FLOPPYNode *whereExpression;

};

#endif  /* __FLOPPY_UPDATE_STATEMENT_H__ */
