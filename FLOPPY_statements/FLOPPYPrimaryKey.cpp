#include "FLOPPYPrimaryKey.h"

FLOPPYPrimaryKey::FLOPPYPrimaryKey() {}
FLOPPYPrimaryKey::~FLOPPYPrimaryKey() {
   for (auto itr = this->attributes->begin() ; itr != this->attributes->end(); itr++) {
      delete (*itr);
   }
   delete this->attributes;
}
