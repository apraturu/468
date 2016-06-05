#ifndef __FLOPPY_VALUE_H__
#define __FLOPPY_VALUE_H__
#include <stdint.h>

typedef enum {
   AttributeValue,
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
         char *sVal;
         uint64_t iVal;
         float fVal;
         bool bVal;
      };

   private:
      ValueType _type;

};
#endif /* __FLOPPY_VALUE_H__ */
