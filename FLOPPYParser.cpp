#include "FLOPPYParser.h"
#include "lex_parse/FLOPPY_parser.h"
#include "lex_parse/FLOPPY_lexer.h"
#include <stdio.h>
#include <string>

FLOPPYOutput *FLOPPYParser::parseFLOPPYString(const char *stmt) {
   FLOPPYOutput* result = NULL;
   YY_BUFFER_STATE state;
   yyscan_t scanner;

   if (yylex_init(&scanner)) {
      fprintf(stderr, "[Error] FLOPPYParser: Error when initializing lexer!\n");
      return NULL;
   }

   state = yy_scan_string(stmt, scanner);

   if (yyparse(&result, scanner)) {
      return result;
   }

   // Clean up
   yy_delete_buffer(state, scanner);
   yylex_destroy(scanner);

   return result;
}


FLOPPYOutput *FLOPPYParser::parseFLOPPYString(const std::string &stmt) {
   const char *str = stmt.c_str();
   return parseFLOPPYString(str);
}
