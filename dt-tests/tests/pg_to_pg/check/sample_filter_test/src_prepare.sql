DROP SCHEMA IF EXISTS test_db_1 CASCADE;
DROP SCHEMA IF EXISTS test_db_2 CASCADE;

CREATE SCHEMA test_db_1;
CREATE SCHEMA test_db_2;

CREATE TABLE test_db_1.one_pk_no_uk_1 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) );
CREATE TABLE test_db_2.one_pk_no_uk_1 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) );
