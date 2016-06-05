#ifndef __FLOPPY_FOREIGN_KEY_H__
#define __FLOPPY_FOREIGN_KEY_H__
#include <vector>

class FLOPPYForeignKey {
   public:
      FLOPPYForeignKey();
      virtual ~FLOPPYForeignKey();

      std::vector<char *> *attributes;
      char *refTableName;
};

#endif /* __FLOPPY_FOREIGN_KEY_H__ */
