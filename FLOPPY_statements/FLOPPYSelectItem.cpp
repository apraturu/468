#include "FLOPPYSelectItem.h"
#include "FLOPPYValue.h"
#include "FLOPPYTableAttribute.h"

FLOPPYSelectItem::FLOPPYSelectItem(FLOPPYSelectItemType type) : _type(type) {}
FLOPPYSelectItem::~FLOPPYSelectItem() {
   if (_type == AggregateType) {
      delete aggregate.value;
   } else if (_type == TableAttributeType) {
      delete tableAttribute;
   } else {
      delete attribute;
   }
}
