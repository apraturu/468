/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_YY_FLOPPY_PARSER_H_INCLUDED
# define YY_YY_FLOPPY_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif
/* "%code requires" blocks.  */
#line 24 "FLOPPY_parser.y" /* yacc.c:1915  */

#include "../FLOPPY_statements/statements.h"
#include "../FLOPPYOutput.h"
#ifndef YYtypeDEF_YY_SCANNER_T
#define YYtypeDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#line 53 "FLOPPY_parser.h" /* yacc.c:1915  */

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    FLOPPY_INTVAL = 258,
    FLOPPY_FLOATVAL = 259,
    FLOPPY_ID = 260,
    FLOPPY_STRING = 261,
    FLOPPY_CREATE = 262,
    FLOPPY_TABLE = 263,
    FLOPPY_VOLATILE = 264,
    FLOPPY_PRIMARY = 265,
    FLOPPY_FOREIGN = 266,
    FLOPPY_KEY = 267,
    FLOPPY_REFERENCES = 268,
    FLOPPY_INDEX = 269,
    FLOPPY_ONLY = 270,
    FLOPPY_SPLIT = 271,
    FLOPPY_DROP = 272,
    FLOPPY_ON = 273,
    FLOPPY_INTO = 274,
    FLOPPY_VALUES = 275,
    FLOPPY_DELETE = 276,
    FLOPPY_INSERT = 277,
    FLOPPY_SELECT = 278,
    FLOPPY_FROM = 279,
    FLOPPY_WHERE = 280,
    FLOPPY_UPDATE = 281,
    FLOPPY_SET = 282,
    FLOPPY_GROUP = 283,
    FLOPPY_BY = 284,
    FLOPPY_HAVING = 285,
    FLOPPY_ORDER = 286,
    FLOPPY_LIMIT = 287,
    FLOPPY_DISTINCT = 288,
    FLOPPY_COUNT = 289,
    FLOPPY_AVERAGE = 290,
    FLOPPY_MAX = 291,
    FLOPPY_MIN = 292,
    FLOPPY_SUM = 293,
    FLOPPY_NULL = 294,
    FLOPPY_AS = 295,
    FLOPPY_NOT = 296,
    FLOPPY_MOD = 297,
    FLOPPY_LE = 298,
    FLOPPY_GE = 299,
    FLOPPY_NE = 300,
    FLOPPY_TRUE = 301,
    FLOPPY_FALSE = 302,
    FLOPPY_AND = 303,
    FLOPPY_INT = 304,
    FLOPPY_FLOAT = 305,
    FLOPPY_BOOLEAN = 306,
    FLOPPY_DATETIME = 307,
    FLOPPY_VARCHAR = 308
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 44 "FLOPPY_parser.y" /* yacc.c:1915  */

	int64_t ival;
	float fval;
   bool bval;
   char *sval;

	FLOPPYStatement *statement;
   FLOPPYCreateTableStatement *create_table_statement;
   FLOPPYDropTableStatement *drop_table_statement;
   FLOPPYCreateIndexStatement *create_index_statement;
   FLOPPYDropIndexStatement *drop_index_statement;
   FLOPPYInsertStatement *insert_statement;
   FLOPPYDeleteStatement *delete_statement;
   FLOPPYUpdateStatement *update_statement;
   FLOPPYSelectStatement *select_statement;
   FLOPPYForeignKey *foreign_key;
   FLOPPYSelectItem *select_item;
   FLOPPYTableSpec *table_spec;
   FLOPPYGroupBy *group_by;
   FLOPPYTableAttribute *table_attribute;

   std::vector<char *> *str_vec;
   std::vector<FLOPPYCreateColumn *> *create_column_vec;
   std::vector<FLOPPYForeignKey *> *foreign_key_vec;
   std::vector<FLOPPYValue *> *value_vec;
   std::vector<FLOPPYSelectItem *> *select_item_vec;
   std::vector<FLOPPYTableSpec *> *table_spec_vec;
   std::vector<FLOPPYTableAttribute *> *table_attr_vec;
   CreateTableAdditionalFunctionality *flags;

   FLOPPYCreateColumn *create_column;
   FLOPPYPrimaryKey *primary_key;

   FLOPPYValue *value;
   FLOPPYNode *node;

#line 156 "FLOPPY_parser.h" /* yacc.c:1915  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int yyparse (FLOPPYOutput** result, yyscan_t scanner);

#endif /* !YY_YY_FLOPPY_PARSER_H_INCLUDED  */
