#include "FLOPPYDeleteStatement.h"
#include "FLOPPYNode.h"

FLOPPYDeleteStatement::FLOPPYDeleteStatement() : FLOPPYStatement(DeleteStatement) {}
FLOPPYDeleteStatement::~FLOPPYDeleteStatement() {
   delete name;
   delete where;
}
