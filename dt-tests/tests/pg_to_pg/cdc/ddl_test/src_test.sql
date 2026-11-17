
INSERT INTO test_db_1.tb_1 VALUES (1,1);

-- add column
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_2 int DEFAULT NULL;
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_3 int DEFAULT NULL;

INSERT INTO test_db_1.tb_1 VALUES (2,2,2,2);

-- drop column
ALTER TABLE test_db_1.tb_1 DROP COLUMN f_2;

INSERT INTO test_db_1.tb_1 VALUES (3,3,3);

-- truncate table
TRUNCATE test_db_1.truncate_tb_1;
TRUNCATE TABLE test_db_1.truncate_tb_2;
TRUNCATE test_db_1.truncate_parent;

-- rename table
ALTER TABLE test_db_1.rename_tb_1 RENAME TO dst_rename_tb_1;

INSERT INTO test_db_1.dst_rename_tb_1 VALUES(1, 1);

ALTER TABLE test_db_1.rename_tb_2 SET SCHEMA test_db_2;

INSERT INTO test_db_2.rename_tb_2 VALUES(1, 1);

-- drop table
DROP TABLE test_db_1.drop_tb_1;

-- drop schema 
DROP SCHEMA test_db_3 CASCADE;

-- create schema

-- in target, should execute: create schema Test_db_4;
DROP SCHEMA IF EXISTS Test_db_4 CASCADE;
CREATE SCHEMA Test_db_4;
-- in target, should execute: create schema Test_db_4; 
CREATE TABLE Test_db_4.Tb_1( f_0 int, f_1 int DEFAULT NULL, "F_2" int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 
INSERT INTO Test_db_4.Tb_1 VALUES (1,1,1);

-- in target, should execute: create schema "Test_db_4"
DROP SCHEMA IF EXISTS "Test_db_4" CASCADE;
CREATE SCHEMA "Test_db_4";
-- in target, should execute: create schema "Test_db_4"."Tb_1"; 
CREATE TABLE "Test_db_4"."Tb_1"( f_0 int, f_1 int DEFAULT NULL, "F_2" int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 
INSERT INTO "Test_db_4"."Tb_1" VALUES (2,2,2);

-- create table
CREATE TABLE test_db_2.tb_1 ( f_0 int, f_1 int DEFAULT NULL, f_2 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 

INSERT INTO test_db_2.tb_1 VALUES (1,1,1);

-- add index
ALTER TABLE test_db_2.tb_1 ADD CONSTRAINT idx_f_1 UNIQUE (f_1);

CREATE INDEX idx_f_2 ON test_db_2.tb_1 (f_2);

-- create schema with special character
CREATE SCHEMA "中文database!@$%^&*()_+";

-- create table with chinese character
CREATE TABLE "中文database!@$%^&*()_+"."中文" ( f_0 int, f_1 int DEFAULT NULL, f_2 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 

INSERT INTO "中文database!@$%^&*()_+"."中文" VALUES(1, 1, 1);

-- contain db system keywords
CREATE TABLE test_db_2.match (select_id SERIAL PRIMARY KEY, "table" VARCHAR(255) NOT NULL, "column" VARCHAR(255) NOT NULL, "offset" INT NOT NULL, unique_col VARCHAR(255), "match" INT, check_col INT, constraint_col INT);
CREATE INDEX idx_index_on_index ON test_db_2.match("offset");
CREATE INDEX idx_key_col ON test_db_2.match("match");
CREATE UNIQUE INDEX uniq_unique_col ON test_db_2.match(unique_col);

-- Raw blocks preserve comment markers and line breaks for the DDL parser.
```
CREATE TABLE test_db_1.ddl_comment_literals (
    id integer PRIMARY KEY,
    plain_note text DEFAULT 'it''s /* literal */ -- # text'
        CHECK (plain_note = 'it''s /* literal */ -- # text'),
    path_note text DEFAULT 'C:\' CHECK (path_note = 'C:\'),
    escape_note text DEFAULT E'it\'s /* literal */ -- # text\nnext line'
        CHECK (escape_note = E'it\'s /* literal */ -- # text\nnext line'),
    continued_note text DEFAULT E'first '
        -- continuation keeps E-string escapes
        'it\'s /* literal */ -- # text'
        CHECK (continued_note = E'first it\'s /* literal */ -- # text'),
    "note/*--#" text DEFAULT 'quoted /* literal */ -- #'
        CHECK ("note/*--#" = 'quoted /* literal */ -- #')
);
```
INSERT INTO test_db_1.ddl_comment_literals (id) VALUES (1);

```
CREATE TABLE test_db_1.ddl_comment_dollar (
    id integer PRIMARY KEY,
    plain_note text DEFAULT $$it's /* literal */ -- #; text$$
        CHECK (plain_note = $$it's /* literal */ -- #; text$$),
    tagged_note text DEFAULT $Tag_1$中文 /* literal */ -- #
$inner$nested$inner$ $$ $tag_1$ text$Tag_1$
        CHECK (tagged_note = $Tag_1$中文 /* literal */ -- #
$inner$nested$inner$ $$ $tag_1$ text$Tag_1$)
);
```
INSERT INTO test_db_1.ddl_comment_dollar (id) VALUES (1);

```
CREATE/* outer /* nested */ tail */TABLE test_db_1.ddl_comment_nested (
    id/* type */integer PRIMARY KEY,
    value integer DEFAULT (8 # 3) CHECK (value = 11) --no space required
); -- trailing comment
```
```
ALTER--no space required
TABLE test_db_1.ddl_comment_nested
ADD/* outer /* nested */ tail */COLUMN note text
DEFAULT $note$keep /* literal */ -- #$note$
CHECK (note = $note$keep /* literal */ -- #$note$);
```
INSERT INTO test_db_1.ddl_comment_nested (id) VALUES (1);
