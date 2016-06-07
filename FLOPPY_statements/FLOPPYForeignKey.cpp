#include "FLOPPYForeignKey.h"

FLOPPYForeignKey::FLOPPYForeignKey() {}
FLOPPYForeignKey::~FLOPPYForeignKey() {
   for (auto itr = this->attributes->begin() ; itr != this->attributes->end(); itr++) {
      delete (*itr);
   }

   delete attributes;

   delete refTableName;
}
