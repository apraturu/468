/*
 * FLOPPY Lexer
 *
 * For CPE 468 Spring 2016 (Dekhtyar)
 * Written by Andrew Wang
 */
%{
#include "FLOPPY_parser.h"
#include <stdio.h>
#include <stdlib.h>
%}

/* Remove stupid y.def.h naming to be constant FLOPPY_ */
%option header-file="FLOPPY_lexer.h"
%option outfile="FLOPPY_lexer.cpp"

/* Flags to make it work */
%option reentrant
%option bison-bridge
%option never-interactive
%option batch
%option case-insensitive
%option bison-locations 
%option warn
%option noyywrap
%option nounput

/* Create Comment and BlockComment States */
%s COMMENT 
%s BLOCKCOMMENT 
%%
--                      BEGIN(COMMENT);
<COMMENT>[^\n]*         /* skip anything thats in a comment */;
<COMMENT>\n             BEGIN(INITIAL);

\/\*                    BEGIN(BLOCKCOMMENT);
<BLOCKCOMMENT>.|\n      /* skip anything thats in a comment block */;
<BLOCKCOMMENT>\*\/      BEGIN(INITIAL);

[ \r\t\n]+              /* skip isspace() */;

CREATE                  { return FLOPPY_CREATE; }
TABLE                   { return FLOPPY_TABLE; }
VOLATILE                { return FLOPPY_VOLATILE; }
PRIMARY                 { return FLOPPY_PRIMARY; }
FOREIGN                 { return FLOPPY_FOREIGN; }
KEY                     { return FLOPPY_KEY; }
REFERENCES              { return FLOPPY_REFERENCES; }
INT|INTEGER             { return FLOPPY_INT; }
FLOAT                   { return FLOPPY_FLOAT; }
BOOLEAN                 { return FLOPPY_BOOLEAN; }
DATETIME                { return FLOPPY_DATETIME; }
VARCHAR                 { return FLOPPY_VARCHAR; }

SELECT                  { return FLOPPY_SELECT; }
INDEX                   { return FLOPPY_INDEX; }
ONLY                    { return FLOPPY_ONLY; }
SPLIT                   { return FLOPPY_SPLIT; }
DROP                    { return FLOPPY_DROP; }
ON                      { return FLOPPY_ON; }
INTO                    { return FLOPPY_INTO; }
VALUES                  { return FLOPPY_VALUES; }
DELETE                  { return FLOPPY_DELETE; }
FROM                    { return FLOPPY_FROM; }
WHERE                   { return FLOPPY_WHERE; }
UPDATE                  { return FLOPPY_UPDATE; }
SET                     { return FLOPPY_SET; }
GROUP                   { return FLOPPY_GROUP; }
BY                      { return FLOPPY_BY; }
HAVING                  { return FLOPPY_HAVING; }
ORDER                   { return FLOPPY_ORDER; }
LIMIT                   { return FLOPPY_LIMIT; }
DISTINCT                { return FLOPPY_DISTINCT; }
INSERT                  { return FLOPPY_INSERT; }
AS                      { return FLOPPY_AS; }

AND                     { return FLOPPY_AND; }
COUNT                   { return FLOPPY_COUNT; }
AVERAGE                 { return FLOPPY_AVERAGE; }
MAX                     { return FLOPPY_MAX; }
MIN                     { return FLOPPY_MIN; }
SUM                     { return FLOPPY_SUM; }
NULL                    { return FLOPPY_NULL; }

NOT                     { return FLOPPY_NOT; }
MOD                     { return FLOPPY_MOD; }
"<="                    { return FLOPPY_LE; }
">="                    { return FLOPPY_GE; }
"!="                    { return FLOPPY_NE; }
"<>"                    { return FLOPPY_NE; }

TRUE                    { return FLOPPY_TRUE; }
FALSE                   { return FLOPPY_FALSE; }

[+-/*=<>(){},;]           { return yytext[0]; }

[0-9]+"."[0-9]* |
"."[0-9]*               {
                           yylval->fval = atof(yytext);
                           return FLOPPY_FLOATVAL;
                        }

[0-9]+                  {
                           yylval->ival = atol(yytext);
                           return FLOPPY_INTVAL;
                        }

\"[^\"]*\"              {
                           yylval->sval = (char *) calloc(strlen(yytext + 1) - 1, sizeof(char));
                           memcpy(yylval->sval, yytext + 1, strlen(yytext + 1) - 1);
                           return FLOPPY_STRING;
                        }

'[^']*'                 {
                           yylval->sval = (char *) calloc(strlen(yytext + 1) - 1, sizeof(char));
                           memcpy(yylval->sval, yytext + 1, strlen(yytext + 1) - 1);
                           return FLOPPY_STRING;
                        }


[A-Za-z][A-Za-z0-9_]*   {
                           yylval->sval = strdup(yytext);
                           return FLOPPY_ID;
                        }


.                       { 
                           fprintf(stderr, "FLOPPY Lex Error Unknown Token: %c\n", yytext[0]); 
                           return 0;
                        }
%%
int yyerror(const char *errmsg) {
    fprintf(stderr, "[FLOPPY-Lexer-Error] %s\n", errmsg);
    return 0;
}
