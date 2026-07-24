DROP SCHEMA IF EXISTS test_db_1 CASCADE;
DROP SCHEMA IF EXISTS test_db_2 CASCADE;
DROP SCHEMA IF EXISTS test_db_3 CASCADE;
DROP SCHEMA IF EXISTS test_db_4 CASCADE;
DROP SCHEMA IF EXISTS "中文database!@$%^&*()_+" CASCADE;
CREATE SCHEMA test_db_1;
CREATE SCHEMA test_db_2;
CREATE SCHEMA test_db_3;

CREATE TABLE test_db_1.tb_1 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 

CREATE TABLE test_db_1.drop_tb_1 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 

CREATE TABLE test_db_1.rename_tb_1 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) );

CREATE TABLE test_db_1.rename_tb_2 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) );

CREATE TABLE test_db_1.truncate_tb_1 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 
INSERT INTO test_db_1.truncate_tb_1 VALUES (1, 1);

CREATE TABLE test_db_1.truncate_tb_2 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 
INSERT INTO test_db_1.truncate_tb_2 VALUES (1, 1);

CREATE TABLE test_db_1.truncate_parent ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) );
CREATE TABLE test_db_1.truncate_child () INHERITS (test_db_1.truncate_parent);
INSERT INTO test_db_1.truncate_parent VALUES (1, 1);
INSERT INTO test_db_1.truncate_child VALUES (2, 2);

CREATE TABLE test_db_2.truncate_tb_1 ( f_0 int, f_1 int DEFAULT NULL, PRIMARY KEY (f_0) ) ; 
