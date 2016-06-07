#include "FLOPPYValue.h"

FLOPPYValue::FLOPPYValue(ValueType type) : _type(type) {}
FLOPPYValue::~FLOPPYValue() {
   if (_type == StringValue) {
      delete sVal;
   }
}
