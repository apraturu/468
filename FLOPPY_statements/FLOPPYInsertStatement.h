#ifndef __FLOPPY_INSERT_STATEMENT_H__
#define __FLOPPY_INSERT_STATEMENT_H__
#include <vector>

#include "FLOPPYStatement.h"

class FLOPPYValue;

class FLOPPYInsertStatement : public FLOPPYStatement {
   public:
      FLOPPYInsertStatement();
      virtual ~FLOPPYInsertStatement();

      char *name;
      std::vector<FLOPPYValue *> *values;


};

#endif /* __FLOPPY_INSERT_STATEMENT_H__ */
