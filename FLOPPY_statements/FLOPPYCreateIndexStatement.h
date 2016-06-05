#ifndef __FLOPPY_CREATE_INDEX_STATEMENT_H__
#define __FLOPPY_CREATE_INDEX_STATEMENT_H__

#include <vector>
#include "FLOPPYStatement.h"

class FLOPPYCreateIndexStatement : public FLOPPYStatement {
   public:
      FLOPPYCreateIndexStatement();
      virtual ~FLOPPYCreateIndexStatement();

      char *name;
      char *tableReference;
      std::vector<char *> *attributes;


   private:
};

#endif /* __FLOPPY_CREATE_INDEX_STATEMENT_H__ */
