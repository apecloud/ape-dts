DROP DATABASE IF EXISTS test_db_1;
DROP DATABASE IF EXISTS test_db_2;

CREATE DATABASE test_db_1;
CREATE DATABASE test_db_2;

CREATE TABLE test_db_1.one_pk_no_uk ( 
    f_0 tinyint, 
    f_1 smallint DEFAULT NULL, 
    f_2 mediumint DEFAULT NULL, 
    f_3 int DEFAULT NULL, 
    f_4 bigint DEFAULT NULL, 
    f_5 decimal(10,4) DEFAULT NULL, 
    f_6 float(6,2) DEFAULT NULL, 
    f_7 double(8,3) DEFAULT NULL, 
    f_8 bit(64) DEFAULT NULL,
    f_9 datetime(6) DEFAULT NULL, 
    f_10 time(6) DEFAULT NULL, 
    f_11 date DEFAULT NULL, 
    f_12 year DEFAULT NULL, 
    f_13 timestamp(6) NULL DEFAULT NULL, 
    f_14 char(255) DEFAULT NULL, 
    f_15 varchar(255) DEFAULT NULL, 
    f_16 binary(255) DEFAULT NULL, 
    f_17 varbinary(255) DEFAULT NULL, 
    f_18 tinytext, 
    f_19 text, 
    f_20 mediumtext, 
    f_21 longtext, 
    f_22 tinyblob, 
    f_23 blob, 
    f_24 mediumblob, 
    f_25 longblob, 
    f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, 
    f_27 set('a','b','c','d','e') DEFAULT NULL, 
    f_28 json DEFAULT NULL,
    PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_2.router_test_1 (
  pk INT,
  col_1 INT,
  PRIMARY KEY(pk)
);

CREATE TABLE test_db_2.router_test_2 (
  pk INT,
  col_1 INT,
  PRIMARY KEY(pk)
);