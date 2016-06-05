#ifndef __FLOPPY_PRIMARY_KEY_H__
#define __FLOPPY_PRIMARY_KEY_H__

#include <vector>

class FLOPPYPrimaryKey {
   public:
      FLOPPYPrimaryKey();
      virtual ~FLOPPYPrimaryKey();

      std::vector<char *> *attributes;
};

#endif /* __FLOPPY_PRIMARY_KEY_H__ */
