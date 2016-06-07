#ifndef __FLOPPY_SELECT_STATEMENT_H__
#define __FLOPPY_SELECT_STATEMENT_H__

#include <vector>

#include "FLOPPYStatement.h"

class FLOPPYSelectItem;
class FLOPPYTableSpec;
class FLOPPYNode;
class FLOPPYTableAttribute;

struct FLOPPYGroupBy {
   std::vector<FLOPPYTableAttribute *> *groupByAttributes;
   FLOPPYNode *havingCondition;
};

class FLOPPYSelectStatement : public FLOPPYStatement {
   public:
      FLOPPYSelectStatement();
      virtual ~FLOPPYSelectStatement();

      bool distinct;
      std::vector<FLOPPYSelectItem *> *selectItems;
      std::vector<FLOPPYTableSpec *> *tableSpecs;
      FLOPPYNode *whereCondition;
      FLOPPYGroupBy *groupBy;
      std::vector<FLOPPYTableAttribute *> *orderBys;
      int limit;


};

#endif /* __FLOPPY_SELECT_STATEMENT_H__ */
