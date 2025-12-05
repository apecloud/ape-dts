DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;

DROP DATABASE IF EXISTS Upper_Case_DB;
CREATE DATABASE Upper_Case_DB;

CREATE TABLE test_db_1.no_pk_no_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_no_uk ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_one_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_multi_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_multi_uk ( f_0 tinyint, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0), UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE test_db_1.col_has_special_character_table (`p:k` tinyint, `col"1` text, `col,2` text, `col\3` text, PRIMARY KEY(`p:k`));

CREATE TABLE test_db_1.numeric_table ( f_0 tinyint, f_1 tinyint unsigned, f_2 smallint, f_3 smallint unsigned, f_4 mediumint, f_5 mediumint unsigned, f_6 int, f_7 int unsigned, f_8 bigint, f_9 bigint unsigned, PRIMARY KEY(f_0));

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

CREATE TABLE test_db_1.set_table( f_0 tinyint,
    f_1 SET('a','b','c','d','e'),
    PRIMARY KEY(f_0));

CREATE TABLE test_db_1.ignore_cols_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, f_3 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 
CREATE TABLE test_db_1.ignore_cols_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, f_3 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE Upper_Case_DB.Upper_Case_TB (
    Id INT, 
    FIELD_1 INT,
    field_2 INT,
    Field_3 INT,
    field_4 INT,
    PRIMARY KEY(Id),
    UNIQUE KEY(FIELD_1, field_2, Field_3)
);

CREATE TABLE test_db_1.where_condition_1 ( f_0 int, f_1 int ); 
CREATE TABLE test_db_1.where_condition_2 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.where_condition_3 ( f_0 int, f_1 int ); 

-- TODO: need some sql_mode settings
-- ```
-- CREATE TABLE test_db_1.zero_date_time_table ( f_0 int, 
--     f_1 datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
--     f_2 time NOT NULL DEFAULT '00:00:00',
--     f_3 date NOT NULL DEFAULT '0000-00-00',
--     f_4 timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
--     PRIMARY KEY(f_0)
-- );
-- ```

-- test foreign key
-- CREATE TABLE test_db_1.fk_tb_2 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));
-- CREATE TABLE test_db_1.fk_tb_1 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));
-- ALTER TABLE test_db_1.fk_tb_1 ADD CONSTRAINT fk_tb_1_1 FOREIGN KEY (f_1) REFERENCES test_db_1.fk_tb_2 (f_1);
-- ALTER TABLE test_db_1.fk_tb_1 ADD CONSTRAINT fk_tb_1_2 FOREIGN KEY (f_2) REFERENCES test_db_1.fk_tb_2 (f_2);

-- test view filtered
CREATE OR REPLACE VIEW test_db_1.one_pk_no_uk_view AS SELECT * FROM test_db_1.one_pk_no_uk;

-- test composite primary key
CREATE TABLE test_db_1.composite_pk_table (pk1 int, pk2 varchar(10), val int, PRIMARY KEY(pk1, pk2));

-- test non-nullable composite unique key
CREATE TABLE test_db_1.composite_unique_key_table (uk1 int not null, uk2 varchar(10) not null, val int, UNIQUE(uk1, uk2));

--test nullable composite unique key
CREATE TABLE test_db_1.composite_unique_key_table_2 (val int, uk2 varchar(10), uk1 int, UNIQUE(uk1, uk2));

--test multi primary and single unique key
CREATE TABLE test_db_1.multi_primary_and_single_unique_table (pk1 int, pk2 varchar(10), uk1 int not null, uk2 varchar(10), val int, PRIMARY KEY(pk1, pk2), UNIQUE(uk1), UNIQUE(uk2));