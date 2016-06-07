%{
/* 
 * FLOPPY Parser
 * For CPE468 Spring 2016 (Dekhtyar)
 * Written by Andrew Wang
 */
#include "FLOPPY_parser.h"
#include "FLOPPY_lexer.h"

#include <vector>
#include <string>
#include <stdio.h>
#include <stdlib.h>

int yyerror(YYLTYPE* llocp, FLOPPYOutput ** result, yyscan_t scanner, const char *msg) {
	FLOPPYOutput *fo = new FLOPPYOutput();
	fo->isValid = false;
   
   *result = fo;
	return 0;
}

%}
%code requires {
#include "../FLOPPY_statements/statements.h"
#include "../FLOPPYOutput.h"
#ifndef YYtypeDEF_YY_SCANNER_T
#define YYtypeDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%output  "FLOPPY_parser.cpp"
%defines "FLOPPY_parser.h"

%define api.pure full
%define api.token.prefix {FLOPPY_}
%define parse.error verbose
%locations
%lex-param   { yyscan_t scanner }
%parse-param { FLOPPYOutput** result }
%parse-param { yyscan_t scanner }

%union {
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
}

%token <ival> INTVAL
%token <fval> FLOATVAL
%token <sval> ID STRING

%token CREATE TABLE VOLATILE PRIMARY FOREIGN KEY REFERENCES
%token INDEX ONLY SPLIT DROP ON INTO VALUES DELETE INSERT SELECT
%token FROM WHERE UPDATE SET GROUP BY HAVING ORDER LIMIT DISTINCT
%token COUNT AVERAGE MAX MIN SUM NULL AS
%token NOT MOD LE GE NE TRUE FALSE AND
%token INT FLOAT BOOLEAN DATETIME VARCHAR 

%type <flags>                    opt_volatile opt_index_only opt_split
%type <ival>                     int_literal opt_column_size column_type opt_limit
%type <bval>                     opt_distinct

%type <statement> 	            statement 
%type <create_table_statement>   create_table_statement
%type <drop_table_statement>     drop_table_statement
%type <create_index_statement>   create_index_statement
%type <drop_index_statement>     drop_index_statement
%type <insert_statement>         insert_statement
%type <delete_statement>         delete_statement
%type <update_statement>         update_statement
%type <select_statement>         select_statement

%type <create_column>            column_def
%type <node>                     condition atomic_condition expression atomic_expression opt_where opt_having
%type <select_item>              select_item
%type <table_spec>               table_spec
%type <group_by>                 opt_group_by
%type <table_attribute>          table_attribute

%type <primary_key>              primary_key
%type <foreign_key>              foreign_key
%type <value>                    value constant

%type <create_column_vec>        column_def_commalist
%type <foreign_key_vec>          opt_foreign_key_list
%type <str_vec>                  attribute_list
%type <value_vec>                value_list
%type <select_item_vec>          select_item_list star_or_select_item_list
%type <table_spec_vec>           table_spec_list
%type <table_attr_vec>           table_attribute_list opt_order_by

/*********************
 * Precendence 
 *********************/

%left AND
%left NOT
%right '=' NE
%nonassoc '>' '<' GE LE

%left '+' '-'
%left '*' '/' MOD

%left '(' ')'
%left '.'

%%
input:
	statement ';' { 
      *result = new FLOPPYOutput($1); 
      (*result)->isValid = true;
   }
	;

statement:
	   create_table_statement { $$ = $1; }
   |  drop_table_statement { $$ = $1; }
   |  create_index_statement { $$ = $1; }
   |  drop_index_statement { $$ = $1; }
   |  insert_statement { $$ = $1; }
   |  delete_statement { $$ = $1; }
   |  update_statement { $$ = $1; }
   |  select_statement { $$ = $1; }
	;

/******************************
 * Create Statement
 * CREATE TABLE <Table> [VOLATILE, [ INDEX ONLY[, SPLIT]]] (
 *    <Attribute> <Type>,
 *    ...
 *    <Attribute> <Type>,
 *    PRIMARY KEY (<Attribute>[, <Attribute>]*)
 *    [, FOREIGN KEY (<Attribute[, <Attribute>]*) REFERENCES <TableName>]*
 ******************************/
create_table_statement:
      CREATE TABLE ID opt_volatile '(' 
         column_def_commalist ','
         primary_key
         opt_foreign_key_list
      ')'   {
               $$ = new FLOPPYCreateTableStatement();
               $$->tableName = $3;
               $$->flags = $4;
               $$->columns = $6;
               $$->pk = $8;
               $$->fk = $9;
            }
	;

opt_volatile:
		VOLATILE opt_index_only { 
         $$ = $2;
         $$->volatileFlag = true;
      }
	|	/* empty */ { 
         $$ = new CreateTableAdditionalFunctionality(); 
         $$->volatileFlag = false;
         $$->indexOnlyFlag = false;
         $$->splitFlag = false;
      }
	;

opt_index_only:
      ',' INDEX ONLY opt_split {
         $$ = $4;
         $$->indexOnlyFlag = true;
      }
	|	/* empty */ { 
         $$ = new CreateTableAdditionalFunctionality(); 
         $$->indexOnlyFlag = false;
         $$->splitFlag = false;
      }
   ;

opt_split:
      ',' SPLIT {
         $$ = new CreateTableAdditionalFunctionality(); 
         $$->splitFlag = true;
      }
	|	/* empty */ { 
         $$ = new CreateTableAdditionalFunctionality(); 
         $$->splitFlag = false;
      }
      

column_def_commalist:
      column_def  {
         $$ = new std::vector<FLOPPYCreateColumn *>(); 
         $$->push_back($1); 
      }
   |  column_def_commalist ',' column_def {
         $1->push_back($3);
         $$ = $1;
      }
	;

column_def:
      ID column_type opt_column_size {
         $$ = new FLOPPYCreateColumn();
         $$->name = $1;
         $$->type = (ColumnType) $2;
         $$->size = $3;
      }
	; 

attribute_list:
      ID { 
         $$ = new std::vector<char *>(); 
         $$->push_back($1); 
      }
   |  attribute_list ',' ID {
         $1->push_back($3);
         $$ = $1;
      }
   ;

primary_key:
      PRIMARY KEY '(' attribute_list ')' {
         $$ = new FLOPPYPrimaryKey();
         $$->attributes = $4;
      }
   ;

opt_foreign_key_list:
      opt_foreign_key_list ',' foreign_key {
         $$ = $1;
         $$->push_back($3);
      }
	|	/* empty */ {
         $$ = new std::vector<FLOPPYForeignKey *>();
      }
   ;

foreign_key:
      FOREIGN KEY '(' attribute_list ')' REFERENCES ID {
         $$ = new FLOPPYForeignKey();
         $$->attributes = $4;
         $$->refTableName = $7;
      }
   ;

column_type:
      INT { 
         $$ = ColumnType::INT; 
      }
   |  FLOAT { 
         $$ = ColumnType::FLOAT; 
      }
   |  VARCHAR { 
         $$ = ColumnType::VARCHAR; 
      }
   |  DATETIME { 
         $$ = ColumnType::DATETIME; 
      }
   |  BOOLEAN { 
         $$ = ColumnType::BOOLEAN; 
      }
	;

opt_column_size:
   '(' int_literal ')' { 
      $$ = $2; 
   }
	|	/* empty */ {
      $$ = 0;
   }

int_literal:
      INTVAL { 
         $$ = $1; 
      }
   ;

/******************************
 * Drop Table Statement
 * DROP TABLE <TableName>;
 ******************************/

drop_table_statement:
      DROP TABLE ID {
         $$ = new FLOPPYDropTableStatement();
         $$->table = $3;
      }
   ;

/******************************
 * Create Index Statement
 * CREATE INDEX <IndexName>
 * ON <TableName> (<AttributeName>[, <AttributeName>]*);
 ******************************/
create_index_statement:
   CREATE INDEX ID 
   ON ID '(' attribute_list ')' {
      $$ = new FLOPPYCreateIndexStatement();
      $$->name = $3;
      $$->tableReference = $5;
      $$->attributes = $7;
   }

/******************************
 * Drop Index Statement
 * DROP INDEX <IndexName> on <TableName>;
 ******************************/
drop_index_statement:
      DROP INDEX ID ON ID {
         $$ = new FLOPPYDropIndexStatement();
         $$->indexName = $3;
         $$->tableName = $5;
      }
   ;

/******************************
 * Insert Statement
 * INSERT INTO <TableName>
 * VALUES (<V>[, <V>]*);
 ******************************/
insert_statement:
      INSERT INTO ID
      VALUES '(' value_list ')' {
         $$ = new FLOPPYInsertStatement();
         $$->name = $3;
         $$->values = $6;
      }
   ;

value_list:
      value {
         $$ = new std::vector<FLOPPYValue *>();
         $$->push_back($1);
      }
   | value_list ',' value {
         $$ = $1;
         $$->push_back($3);
      }
   ;

value:
      constant { $$ = $1; }
   |  TRUE {
         $$ = new FLOPPYValue(BooleanValue);
         $$->bVal = true;
      }
   |  FALSE {
         $$ = new FLOPPYValue(BooleanValue);
         $$->bVal = false;
      }
   ;

constant:
      INTVAL {
         $$ = new FLOPPYValue(IntValue);
         $$->iVal = $1;
      }
   |  '-' INTVAL {
         $$ = new FLOPPYValue(IntValue);
         $$->iVal = -$2;
      }
   |  FLOATVAL {
         $$ = new FLOPPYValue(FloatValue);
         $$->fVal = $1;
      }
   |  STRING {
         $$ = new FLOPPYValue(StringValue);
         $$->sVal = $1;
      }
   |  NULL {
         $$ = new FLOPPYValue(NullValue);
         $$->sVal = NULL;
      }


/******************************
 * Delete Statement
 * DELETE FROM <TableName>
 * WHERE <Condition>;
 ******************************/
delete_statement:
      DELETE FROM ID
      WHERE condition {
         $$ = new FLOPPYDeleteStatement();
         $$->name = $3;
         $$->where = $5;
      }
   ;

condition:
      atomic_condition { $$ = $1; }
   |  condition AND condition {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::AndOperator;
         $$->node.right= $3;
      }
   |  NOT condition {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $2;
         $$->node.op = FLOPPYNodeOperator::NotOperator;
      }
   |  '(' condition ')' {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $2;
         $$->node.op = FLOPPYNodeOperator::ParenthesisOperator;
   }
   ;

atomic_condition:
      expression '<' expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::LessThanOperator;
         $$->node.right= $3;
      }
   |  expression '>' expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::GreaterThanOperator;
         $$->node.right= $3;
      }
   |  expression '=' expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::EqualOperator;
         $$->node.right= $3;
      }
   |  expression LE expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::LessThanEqualOperator;
         $$->node.right= $3;
      }
   |  expression GE expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::GreaterThanEqualOperator;
         $$->node.right= $3;
      }
   |  expression NE expression {
         $$ = new FLOPPYNode(ConditionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::NotEqualOperator;
         $$->node.right= $3;
      }
   |  TRUE { 
         $$ = new FLOPPYNode(ValueNode); 
         $$->value = new FLOPPYValue(BooleanValue);
         $$->value->bVal = true;
      }
   |  FALSE {
         $$ = new FLOPPYNode(ValueNode); 
         $$->value = new FLOPPYValue(BooleanValue);
         $$->value->bVal = false;
      }
   ;

expression:
      atomic_expression { $$ = $1; }
   |  expression '+' expression {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::PlusOperator;
         $$->node.right= $3;
      }
   |  expression '-' expression {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::MinusOperator;
         $$->node.right= $3;
      }
   |  expression '*' expression {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::TimesOperator;
         $$->node.right= $3;
      }
   |  expression '/' expression {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::DivideOperator;
         $$->node.right= $3;
      }
   |  expression MOD expression {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.left = $1;
         $$->node.op = FLOPPYNodeOperator::ModOperator;
         $$->node.right= $3;
      }
   | '(' expression ')' {
         $$ = new FLOPPYNode(ExpressionNode);
         $$->node.op = FLOPPYNodeOperator::ParenthesisOperator;
         $$->node.left = $2;
      }
   ;

atomic_expression:
   table_attribute {
         $$ = new FLOPPYNode(ValueNode);
         $$->value = new FLOPPYValue(TableAttributeValue);
         $$->value->tableAttribute = $1;
      }
   |  constant {
         $$ = new FLOPPYNode(ValueNode);
         $$->value = $1;
      }
   |  MIN '(' ID ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::MinAggregate;
         $$->aggregate.value = new FLOPPYValue(AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  MAX '(' ID ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::MaxAggregate;
         $$->aggregate.value = new FLOPPYValue(AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  SUM '(' ID ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::SumAggregate;
         $$->aggregate.value = new FLOPPYValue(AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  AVERAGE '(' ID ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::AverageAggregate;
         $$->aggregate.value = new FLOPPYValue(AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  COUNT '(' ID ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::CountAggregate;
         $$->aggregate.value = new FLOPPYValue(AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  COUNT '(' '*' ')' {
         $$ = new FLOPPYNode(AggregateNode);
         $$->aggregate.op = FLOPPYAggregateOperator::CountStarAggregate;
         $$->aggregate.value = NULL;
      }
   ;

/******************************
 * Update Statement
 * UPDATE <TableName>
 * SET <AttributeName> = <Expression>
 * WHERE <Condition>
 ******************************/
update_statement:
      UPDATE ID
      SET ID '=' expression
      WHERE condition {
         $$ = new FLOPPYUpdateStatement();
         $$->tableName = $2;
         $$->attributeName = $4;
         $$->attributeExpression= $6;
         $$->whereExpression = $8;
      }
   ;

/******************************
 * Select Statement
 * SELECT [DISTINCT] <SelectItem>[, <SelectItem>]*
 * FROM <TableSpec>{, <TableSpec>]*
 * [ WHERE <Condition>]
 * [ GROUP BY <Attribute>[, <Attribute>]*
 * [ HAVING <Condition>] ]
 * [ ORDER BY <OrderTerm>[, <OrderTerm>]]
 * [ LIMIT <Number>]
 ******************************/
select_statement:
      SELECT opt_distinct star_or_select_item_list
      FROM table_spec_list
      opt_where
      opt_group_by
      opt_order_by
      opt_limit {
         $$ = new FLOPPYSelectStatement();
         $$->distinct = $2;
         $$->selectItems = $3;
         $$->tableSpecs = $5;
         $$->whereCondition = $6;
         $$->groupBy = $7;
         $$->orderBys = $8;
         $$->limit= $9;
      }
   ;

star_or_select_item_list:
      '*' {
         $$ = new std::vector<FLOPPYSelectItem *>();
         $$->push_back(new FLOPPYSelectItem(FLOPPYSelectItemType::StarType));
      }
   |  select_item_list { $$ = $1; }

select_item_list:
      select_item {
         $$ = new std::vector<FLOPPYSelectItem *>(); 
         $$->push_back($1);
      }
   |  select_item_list ',' select_item {
         $$ = $1;
         $$->push_back($3);
      }
   ;

select_item:
   table_attribute {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::TableAttributeType);
         $$->tableAttribute = $1;
      }
   |  COUNT '(' ID ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::CountAggregate;
         $$->aggregate.value = new FLOPPYValue(ValueType::AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  AVERAGE '(' ID ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::AverageAggregate;
         $$->aggregate.value = new FLOPPYValue(ValueType::AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  MAX '(' ID ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::MaxAggregate;
         $$->aggregate.value = new FLOPPYValue(ValueType::AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  MIN '(' ID ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::MinAggregate;
         $$->aggregate.value = new FLOPPYValue(ValueType::AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  SUM '(' ID ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::SumAggregate;
         $$->aggregate.value = new FLOPPYValue(ValueType::AttributeValue);
         $$->aggregate.value->sVal = $3;
      }
   |  COUNT '(' '*' ')' {
         $$ = new FLOPPYSelectItem(FLOPPYSelectItemType::AggregateType);
         $$->aggregate.op = FLOPPYAggregateOperator::CountStarAggregate;
         $$->aggregate.value = NULL;
      }
   ;

table_spec_list:
      table_spec {
         $$ = new std::vector<FLOPPYTableSpec *>();
         $$->push_back($1);
      }
   |  table_spec_list ',' table_spec {
         $$ = $1;
         $$->push_back($3);
      }
   ;

table_spec:
      ID {
         $$ = new FLOPPYTableSpec();
         $$->tableName = $1;
         $$->alias = NULL;
      }
   |  ID ID {
         $$ = new FLOPPYTableSpec();
         $$->tableName = $1;
         $$->alias = $2;
      }
   |  ID AS ID {
         $$ = new FLOPPYTableSpec();
         $$->tableName = $1;
         $$->alias = $3;
      }
   ;

opt_distinct:
      DISTINCT    { $$ = true; }
	|	/* empty */ { $$ = false; }
   ;

opt_where:
      WHERE condition {
         $$ = $2;
      }
   |  /* empty */ {
         $$ = NULL;
      }
   ;

opt_group_by:
      GROUP BY table_attribute_list
      opt_having {
         $$ = new FLOPPYGroupBy();
         $$->groupByAttributes = $3;
         $$->havingCondition = $4;
      }
   |	/* empty */ { 
         $$ = NULL;
      }
   ;

opt_having:
      HAVING condition {
         $$ = $2;
      }
   |	/* empty */ { 
         $$ = NULL;
      }
   ;

opt_order_by:
      ORDER BY table_attribute_list {
         $$ = $3;
      }
   |  /* empty */ { 
         $$ = NULL;
      }
   ;

opt_limit:
      LIMIT INTVAL {
         $$ = $2;
      }
   |	/* empty */ { 
         $$ = -1;
      }
   ;

table_attribute_list:
   table_attribute {
         $$ = new std::vector<FLOPPYTableAttribute *>();
         $$->push_back($1);
      }
   |  table_attribute_list ',' table_attribute {
         $$ = $1;
         $$->push_back($3);
      }
   ;

table_attribute:
      ID {
         $$ = new FLOPPYTableAttribute();
         $$->tableName = NULL;
         $$->attribute = $1;
      }
   |  ID '.' ID {
         $$ = new FLOPPYTableAttribute();
         $$->tableName = $1;
         $$->attribute = $3;
      }
   ;

%%
