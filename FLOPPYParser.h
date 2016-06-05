#ifndef __FLOPPYPARSER_H_
#define __FLOPPYPARSER_H_

#include "FLOPPYOutput.h"
#include "FLOPPY_statements/statements.h"

/**
 * Main class for parsing FLOPPY strings
 */
class FLOPPYParser {
   public:
      static FLOPPYOutput* parseFLOPPYString(const char *stmt);
      static FLOPPYOutput* parseFLOPPYString(const std::string &stmt);

   private:
      FLOPPYParser();
};


#endif
