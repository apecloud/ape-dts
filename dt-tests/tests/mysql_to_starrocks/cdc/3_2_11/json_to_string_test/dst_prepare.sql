DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.json_test ( 
    f_0 TINYINT,
    f_1 STRING) ENGINE=OLAP PRIMARY KEY(f_0) DISTRIBUTED BY HASH(f_0);
