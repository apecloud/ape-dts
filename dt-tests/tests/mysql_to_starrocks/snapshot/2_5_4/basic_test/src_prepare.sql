DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

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
    f_18 tinytext, 
    f_19 text, 
    f_20 mediumtext, 
    f_21 longtext, 
    f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, 
    f_27 set('a','b','c','d','e') DEFAULT NULL, 
    f_28 json DEFAULT NULL,
    PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;