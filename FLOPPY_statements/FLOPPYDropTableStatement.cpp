#include "FLOPPYDropTableStatement.h"

FLOPPYDropTableStatement::FLOPPYDropTableStatement() : 
   FLOPPYStatement(DropTableStatement) {
}

FLOPPYDropTableStatement::~FLOPPYDropTableStatement() {
   delete this->table;
}
