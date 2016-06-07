#include "FLOPPYCreateIndexStatement.h"

FLOPPYCreateIndexStatement::FLOPPYCreateIndexStatement() : FLOPPYStatement(CreateIndexStatement) {

}

FLOPPYCreateIndexStatement::~FLOPPYCreateIndexStatement() {
   delete this->name;
   delete this->tableReference;
   for (auto itr = this->attributes->begin() ; itr != this->attributes->end(); itr++) {
      delete (*itr);
   }
   delete this->attributes;
}
