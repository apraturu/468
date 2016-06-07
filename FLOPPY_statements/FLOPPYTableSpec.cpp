#include "FLOPPYTableSpec.h"

FLOPPYTableSpec::FLOPPYTableSpec() {}
FLOPPYTableSpec::~FLOPPYTableSpec() {
   delete tableName;
   delete alias;
}
