DROP SCHEMA IF EXISTS test_db_1 CASCADE;
DROP SCHEMA IF EXISTS test_db_2 CASCADE;
DROP SCHEMA IF EXISTS test_db_3 CASCADE;
DROP SCHEMA IF EXISTS test_db_4 CASCADE;
DROP SCHEMA IF EXISTS test_db_5 CASCADE;
DROP SCHEMA IF EXISTS other_test_db_1 CASCADE;

CREATE SCHEMA test_db_1;
CREATE SCHEMA test_db_2;
CREATE SCHEMA test_db_3;
CREATE SCHEMA test_db_4;
CREATE SCHEMA test_db_5;
CREATE SCHEMA other_test_db_1;

CREATE TABLE test_db_1.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE test_db_1.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 

CREATE TABLE test_db_2.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE test_db_2.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 

CREATE TABLE test_db_3.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE test_db_3.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 

CREATE TABLE test_db_4.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE test_db_4.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 

CREATE TABLE test_db_5.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE test_db_5.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 

CREATE TABLE other_test_db_1.one_pk_no_uk_1 (pk serial, val numeric(20,8), PRIMARY KEY(pk)); 
CREATE TABLE other_test_db_1.one_pk_no_uk_2 (pk serial, val numeric(20,8), PRIMARY KEY(pk));

-- Wildcards, explicit regex, and otherwise literal names.
DROP SCHEMA IF EXISTS wildchar_filter CASCADE;
CREATE SCHEMA wildchar_filter;
CREATE TABLE "wildchar_filter"."star" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."star_many" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."prestar" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."question" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."question1" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."question12" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."regex_1" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."regex_12" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."regex_x" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."regex_12tail" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."exact_name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."exact_name_tail" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."plus+name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."plusname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."paren(name)" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."parenname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."bracket[12]" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."bracket1" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."brace{2}" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."bracee" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."caret^name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."caretname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."dollar$name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."dollarname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."pipe|name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."pipe_tail" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."quoted*name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."quotedXname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."quoted?name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."quotedYname" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_star" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_star_many" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skipXstar" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_question" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_question1" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_question12" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_regex_1" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip_regex_x" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skip+name" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."skipname" (id int NOT NULL PRIMARY KEY, val int);

-- Other metacharacters stay literal when combined with a wildcard.
CREATE TABLE "wildchar_filter"."plus+star" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."plus+star_many" (id int NOT NULL PRIMARY KEY, val int);
CREATE TABLE "wildchar_filter"."plusstar" (id int NOT NULL PRIMARY KEY, val int);
