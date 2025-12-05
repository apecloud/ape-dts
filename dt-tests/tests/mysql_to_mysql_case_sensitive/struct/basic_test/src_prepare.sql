drop database if exists STRUCT_it_mysql2mysql_1;

create database if not exists STRUCT_it_mysql2mysql_1;

-- full column type
CREATE TABLE STRUCT_it_mysql2mysql_1.FULL_column_type (
    ID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    VARCHAR_col VARCHAR(255) NOT NULL COMMENT 'varchar_col_comment',
    CHAR_col CHAR(10) COMMENT 'char_col_comment',
    tinytext_col TINYTEXT COMMENT 'tinytext_col_comment',
    mediumtext_col MEDIUMTEXT COMMENT 'mediumtext_col_comment',
    longtext_col LONGTEXT COMMENT 'longtext_col_comment',
    text_col TEXT COMMENT 'text_col_comment',
    tinyint_col TINYINT DEFAULT 0 COMMENT 'tinyint_col_comment',
    smallint_col SMALLINT COMMENT 'smallint_col_comment',
    mediumint_col MEDIUMINT COMMENT 'mediumint_col_comment',
    int_col INT COMMENT 'int_col_comment',
    bigint_col BIGINT COMMENT 'bigint_col_comment',
    float_col FLOAT(8, 2) COMMENT 'float_col_comment',
    double_col DOUBLE(16, 4) COMMENT 'double_col_comment',
    bit_col BIT(64) DEFAULT NULL COMMENT 'bit_col_comment',
    decimal_col DECIMAL(10, 2) COMMENT 'decimal_col_comment',
    date_col DATE COMMENT 'date_col_comment',
    datetime_col DATETIME COMMENT 'datetime_col_comment',
    datetime_col2 DATETIME(6) COMMENT 'datetime_col2_comment',
    timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'timestamp_col_comment',
    timestamp_co2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP COMMENT 'timestamp_co2_comment',
    time_col TIME COMMENT 'time_col_comment',
    time_col2 TIME(2) COMMENT 'time_col2_comment',
    year_col YEAR COMMENT 'year_col_comment',
    binary_col BINARY(16) COMMENT 'binary_col_comment',
    varbinary_col VARBINARY(255) COMMENT 'varbinary_col_comment',
    blob_col BLOB COMMENT 'blob_col_comment',
    tinyblob_col TINYBLOB COMMENT 'tinyblob_col_comment',
    mediumblob_col MEDIUMBLOB COMMENT 'mediumblob_col_comment',
    longblob_col LONGBLOB COMMENT 'longblob_col_comment',
    enum_col ENUM('value1', 'value2', 'value3') COMMENT 'enum_col_comment',
    set_col SET('option1', 'option2', 'option3') COMMENT 'set_col_comment',
    json_col JSON DEFAULT NULL COMMENT 'json_col_comment'
); 

-- full index type
CREATE TABLE STRUCT_it_mysql2mysql_1.FULL_index_type(
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    F_1 int, 
    F_2 char(128),
    f_3 varchar(128),
    f_4 varchar(128),
    f_5 varchar(128),
    f_6 TEXT,
    f_7 TEXT, 
    f_8 TEXT, 
    f_9 POINT NOT NULL
);

-- unique key with multiple columns
CREATE UNIQUE INDEX IDX_unique_1 ON STRUCT_it_mysql2mysql_1.FULL_index_type(F_1, F_2, f_3);

-- unique key with 1 column
CREATE UNIQUE INDEX IDX_unique_2 ON STRUCT_it_mysql2mysql_1.FULL_index_type(f_3);

-- HASH indexes are only for in-memory tables (or NDB) but not myISAM or InnoDB 
-- CREATE UNIQUE INDEX idx_unique_3 USING HASH ON STRUCT_it_mysql2mysql_1.FULL_index_type(f_4, f_5);

-- fulltext key with multiple columns
CREATE FULLTEXT INDEX IDX_full_text_1 ON STRUCT_it_mysql2mysql_1.FULL_index_type(f_6, f_7, f_8);

-- fulltext key with 1 columns
CREATE FULLTEXT INDEX IDX_full_text_2 ON STRUCT_it_mysql2mysql_1.FULL_index_type(f_8);

-- spatial index
-- only 1 column supported in spatial key
CREATE SPATIAL INDEX IDX_spatial_1 ON STRUCT_it_mysql2mysql_1.FULL_index_type(f_9);

-- full constraint
CREATE TABLE STRUCT_it_mysql2mysql_1.CONSTRAINT_table (
  ID INT PRIMARY KEY AUTO_INCREMENT, 
  USERNAME VARCHAR(50) NOT NULL UNIQUE, 
  PASSWORD VARCHAR(50) NOT NULL, 
  email VARCHAR(100) NOT NULL, 
  age INT, 
  gender ENUM('Male', 'Female', 'Other') NOT NULL, 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
  CONSTRAINT CHK_age CHECK (age >= 18), 
  CONSTRAINT CHK_email CHECK (email LIKE '%@%.%')
);