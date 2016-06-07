#include "FLOPPYDropIndexStatement.h"

FLOPPYDropIndexStatement::FLOPPYDropIndexStatement() : FLOPPYStatement(DropIndexStatement) {

}
FLOPPYDropIndexStatement::~FLOPPYDropIndexStatement() {
   delete indexName;
   delete tableName;
}
