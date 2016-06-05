#ifndef __FLOPPY_OUTPUT_H_
#define __FLOPPY_OUTPUT_H_

#include <stddef.h>
#include "FLOPPY_statements/FLOPPYStatement.h"

/**
 * Represents the result of the FLOPPYParser.
 * If parsing was successful it contains a list of FLOPPYStatement.
 */
class FLOPPYOutput {
   public:

      FLOPPYOutput ();
      FLOPPYOutput(FLOPPYStatement* stmt);
      virtual ~FLOPPYOutput();

      size_t size();

      // public properties
      FLOPPYStatement *statement;
      bool isValid;
};

#endif /* __FLOPPY_OUTPUT_H_ */
