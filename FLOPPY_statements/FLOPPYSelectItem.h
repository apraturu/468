#ifndef __FLOPPY_SELECT_ITEM_H__
#define __FLOPPY_SELECT_ITEM_H__

#include "FLOPPYAggregate.h"

class FLOPPYValue;
class FLOPPYTableAttribute;

typedef enum {
   AttributeType,
   TableAttributeType,
   AggregateType,
   StarType
} FLOPPYSelectItemType;

class FLOPPYSelectItem {
   public:
      FLOPPYSelectItem(FLOPPYSelectItemType type);
      virtual ~FLOPPYSelectItem();

      union {
         char *attribute;
         FLOPPYTableAttribute *tableAttribute;
         struct {
            FLOPPYAggregateOperator op;
            FLOPPYValue *value;
         } aggregate;
      };

      FLOPPYSelectItemType _type;
};

#endif /* __FLOPPY_SELECT_ITEM_H__ */
