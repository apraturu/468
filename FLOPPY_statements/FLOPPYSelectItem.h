#ifndef __FLOPPY_SELECT_ITEM_H__
#define __FLOPPY_SELECT_ITEM_H__

#include "FLOPPYAggregate.h"

class FLOPPYValue;

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
         struct {
            char *tableName;
            char *attribute;
         } tableAttribute;
         struct {
            FLOPPYAggregateOperator op;
            FLOPPYValue *value;
         } aggregate;
      };

      FLOPPYSelectItemType _type;
};

#endif /* __FLOPPY_SELECT_ITEM_H__ */
