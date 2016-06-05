#ifndef __FLOPPY_NODE_H__
#define __FLOPPY_NODE_H__

#include "FLOPPYAggregate.h"

class FLOPPYValue;

typedef enum {
   AndOperator,
   NotOperator,
   GreaterThanOperator,
   GreaterThanEqualOperator,
   LessThanOperator,
   LessThanEqualOperator,
   EqualOperator,
   NotEqualOperator,
   ParenthesisOperator,
   PlusOperator,
   MinusOperator,
   TimesOperator,
   DivideOperator,
   ModOperator
} FLOPPYNodeOperator;

typedef enum {
   ConditionNode,
   ExpressionNode,
   AggregateNode,
   ValueNode
} FLOPPYNodeType;

class FLOPPYNode {
   public:
      FLOPPYNode(FLOPPYNodeType type);
      virtual ~FLOPPYNode();

      union {
         struct {
            FLOPPYNode *left;
            FLOPPYNode *right;
            FLOPPYNodeOperator op;
         } node;

         struct {
            FLOPPYAggregateOperator op;
            FLOPPYValue *value;
         } aggregate;


         FLOPPYValue *value;
      };

      FLOPPYNodeType _type;
};

#endif /* __FLOPPY_NODE_H__ */
