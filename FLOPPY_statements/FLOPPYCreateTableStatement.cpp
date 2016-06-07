#include "FLOPPYCreateTableStatement.h"
#include "FLOPPYForeignKey.h"
#include "FLOPPYCreateColumn.h"
#include "FLOPPYPrimaryKey.h"

FLOPPYCreateTableStatement::FLOPPYCreateTableStatement() : 
   FLOPPYStatement(CreateTableStatement) {

}

FLOPPYCreateTableStatement::~FLOPPYCreateTableStatement() {
   delete flags;
   
   for (auto itr = this->columns->begin() ; itr != this->columns->end(); itr++) {
      delete (*itr);
   }

   delete columns;

   delete pk;

   for (auto itr = this->fk->begin() ; itr != this->fk->end(); itr++) {
      delete (*itr);
   }

   delete fk;
}
