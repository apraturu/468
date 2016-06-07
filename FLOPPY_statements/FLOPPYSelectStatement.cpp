#include "FLOPPYSelectStatement.h"
#include "FLOPPYSelectItem.h"
#include "FLOPPYTableSpec.h"
#include "FLOPPYNode.h"
#include "FLOPPYTableAttribute.h"

FLOPPYSelectStatement::FLOPPYSelectStatement() : FLOPPYStatement(SelectStatement) {}
FLOPPYSelectStatement::~FLOPPYSelectStatement() {
   if (this->selectItems) {
      for (auto itr = this->selectItems->begin() ; itr != this->selectItems->end(); itr++) {
         delete (*itr);
      }

      delete this->selectItems;
   }

   if (this->tableSpecs) {
      for (auto itr = this->tableSpecs->begin() ; itr != this->tableSpecs->end(); itr++) {
         delete (*itr);
      }

      delete this->tableSpecs;
   }

   if (whereCondition) {
      delete whereCondition;
   }

   if (groupBy) {
      delete groupBy;
   }
   
   if (this->orderBys) {
      for (auto itr = this->orderBys->begin() ; itr != this->orderBys->end(); itr++) {
         delete (*itr);
      }

      delete orderBys;
   }
}
