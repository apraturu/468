#ifndef __FLOPPY_DROP_INDEX_STATEMENT_H__
#define __FLOPPY_DROP_INDEX_STATEMENT_H__

#include "FLOPPYStatement.h"

class FLOPPYDropIndexStatement : public FLOPPYStatement {
   public:
      FLOPPYDropIndexStatement();
      virtual ~FLOPPYDropIndexStatement();

      char *indexName;
      char *tableName;

};
#endif /* __FLOPPY_DROP_INDEX_STATEMENT_H__ */
