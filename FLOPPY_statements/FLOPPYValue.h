#ifndef __FLOPPY_VALUE_H__
#define __FLOPPY_VALUE_H__
#include <stdint.h>

class FLOPPYTableAttribute;

typedef enum {
   AttributeValue,
   TableAttributeValue,
   StringValue,
   IntValue,
   FloatValue,
   BooleanValue,
   NullValue
} ValueType;

class FLOPPYValue {
   public:
      FLOPPYValue(ValueType type);
      virtual ~FLOPPYValue();

      virtual ValueType type() {
         return _type;
      };

      union {
         FLOPPYTableAttribute *tableAttribute;
         char *sVal;
         uint64_t iVal;
         float fVal;
         bool bVal;
      };

   private:
      ValueType _type;

};
#endif /* __FLOPPY_VALUE_H__ */
