#include "FLOPPYNode.h"
#include "FLOPPYValue.h"

FLOPPYNode::FLOPPYNode(FLOPPYNodeType type) : _type(type) {}
FLOPPYNode::~FLOPPYNode() {
   if (_type == AggregateNode) {
      delete this->aggregate.value;
   } else if (_type == ValueNode) {
      delete this->value;
   } else {
      if (this->node.left) {
         delete this->node.left;
      }
      if (this->node.right) {
         delete this->node.right;
      }
   }
}
