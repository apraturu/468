#include "FLOPPYInsertStatement.h"
#include "FLOPPYValue.h"

FLOPPYInsertStatement::FLOPPYInsertStatement() : FLOPPYStatement(InsertStatement) {}
FLOPPYInsertStatement::~FLOPPYInsertStatement() {
   delete name;
   for (auto itr = this->values->begin() ; itr != this->values->end(); itr++) {
      delete (*itr);
   }

   delete values;

}
