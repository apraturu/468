#include "FLOPPYUpdateStatement.h"
#include "FLOPPYNode.h"

FLOPPYUpdateStatement::FLOPPYUpdateStatement() : FLOPPYStatement(UpdateStatement) {

}
FLOPPYUpdateStatement::~FLOPPYUpdateStatement() {
   delete this->tableName;
   delete this->attributeName;
   delete this->attributeExpression;
   delete this->whereExpression;
}
