DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.no_pk_no_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_no_uk ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_one_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_multi_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_multi_uk ( f_0 tinyint, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0), UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE test_db_1.col_has_special_character_table (`p:k` tinyint, `col"1` text, `col,2` text, `col\3` text, PRIMARY KEY(`p:k`));

CREATE TABLE test_db_1.numeric_table ( f_0 tinyint, f_1 tinyint unsigned, f_2 smallint, f_3 smallint unsigned, f_4 mediumint, f_5 mediumint unsigned, f_6 int, f_7 int unsigned, f_8 bigint, f_9 bigint unsigned, PRIMARY KEY(f_0));

```
CREATE TABLE test_db_1.date_time_table( f_0 tinyint, 
    f_1 datetime DEFAULT NULL, 
    f_2 datetime(6) DEFAULT NULL, 
    f_3 time DEFAULT NULL, 
    f_4 time(6) DEFAULT NULL, 
    f_5 timestamp NULL DEFAULT NULL,
    f_6 timestamp(6) NULL DEFAULT NULL,
    f_7 date DEFAULT NULL, 
    f_8 year DEFAULT NULL,
    PRIMARY KEY(f_0));
```

```
CREATE TABLE test_db_1.set_table( f_0 tinyint,
    f_1 SET('a','b','c','d','e'),
    PRIMARY KEY(f_0));
```

CREATE TABLE test_db_1.ignore_cols_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, f_3 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 
CREATE TABLE test_db_1.ignore_cols_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, f_3 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 