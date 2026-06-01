DROP DATABASE IF EXISTS test_db_1;
DROP DATABASE IF EXISTS test_db_2;

CREATE DATABASE test_db_1;
CREATE DATABASE test_db_2;

CREATE TABLE test_db_1.one_pk_no_uk_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE test_db_2.one_pk_no_uk_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
