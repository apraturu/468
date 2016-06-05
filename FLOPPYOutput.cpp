
#include "FLOPPYOutput.h"

FLOPPYOutput::FLOPPYOutput() : isValid(true) {
}


FLOPPYOutput::FLOPPYOutput(FLOPPYStatement* stmt) : isValid(true) {
   statement = stmt;
};


FLOPPYOutput::~FLOPPYOutput() {
   delete statement;
}
