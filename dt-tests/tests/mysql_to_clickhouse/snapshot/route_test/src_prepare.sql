DROP DATABASE IF EXISTS test_db_2;

CREATE DATABASE test_db_2;

CREATE TABLE test_db_2.router_test_1 (
  pk INT,
  tinyint_col INT,
  PRIMARY KEY(pk)
);

CREATE TABLE test_db_2.router_test_2 (
  pk INT,
  tinyint_col INT,
  PRIMARY KEY(pk)
);