drop database if exists struct_it_mysql2mysql_1;

create database if not exists struct_it_mysql2mysql_1;

-- full column type
CREATE TABLE struct_it_mysql2mysql_1.full_column_type (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    varchar_col VARCHAR(255) NOT NULL COMMENT 'varchar_col_comment',
    char_col CHAR(10) COMMENT 'char_col_comment',
    tinytext_col TINYTEXT COMMENT 'tinytext_col_comment',
    mediumtext_col MEDIUMTEXT COMMENT 'mediumtext_col_comment',
    longtext_col LONGTEXT COMMENT 'longtext_col_comment',
    text_col TEXT COMMENT 'text_col_comment',
    tinyint_col TINYINT COMMENT 'tinyint_col_comment',
    smallint_col SMALLINT COMMENT 'smallint_col_comment',
    mediumint_col MEDIUMINT COMMENT 'mediumint_col_comment',
    int_col INT COMMENT 'int_col_comment',
    bigint_col BIGINT COMMENT 'bigint_col_comment',
    float_col FLOAT(8, 2) COMMENT 'float_col_comment',
    double_col DOUBLE(16, 4) COMMENT 'double_col_comment',
    bit_col BIT(64) COMMENT 'bit_col_comment',
    decimal_col DECIMAL(10, 2) COMMENT 'decimal_col_comment',
    date_col DATE COMMENT 'date_col_comment',
    datetime_col DATETIME COMMENT 'datetime_col_comment',
    datetime_col2 DATETIME(6) COMMENT 'datetime_col2_comment',
    timestamp_col TIMESTAMP COMMENT 'timestamp_col_comment',
    timestamp_col2 TIMESTAMP(6) COMMENT 'timestamp_col2_comment',
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
    json_col JSON COMMENT 'json_col_comment'
); 

-- full column type with default value
-- The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
CREATE TABLE struct_it_mysql2mysql_1.full_column_type_with_default (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    varchar_col VARCHAR(255) NOT NULL DEFAULT 'abc中文' COMMENT 'varchar_col_comment',
    char_col CHAR(255) DEFAULT 'abc中文' COMMENT 'char_col_comment',
    tinytext_col TINYTEXT COMMENT 'tinytext_col_comment',
    mediumtext_col MEDIUMTEXT COMMENT 'mediumtext_col_comment',
    longtext_col LONGTEXT COMMENT 'longtext_col_comment',
    text_col TEXT COMMENT 'text_col_comment',
    tinyint_col TINYINT DEFAULT 10 COMMENT 'tinyint_col_comment',
    smallint_col SMALLINT DEFAULT 100 COMMENT 'smallint_col_comment',
    mediumint_col MEDIUMINT DEFAULT 1000 COMMENT 'mediumint_col_comment',
    int_col INT DEFAULT 10000 COMMENT 'int_col_comment',
    bigint_col BIGINT DEFAULT 100000 COMMENT 'bigint_col_comment',
    float_col FLOAT(8, 2) DEFAULT 1.01 COMMENT 'float_col_comment',
    double_col DOUBLE(16, 4) DEFAULT 1.0001 COMMENT 'double_col_comment',
    bit_col BIT(1) DEFAULT b'1' COMMENT 'bit_col_comment',
    bit_col2 BIT(1) DEFAULT B'0' COMMENT 'bit_col_comment',
    bit_col3 BIT(64) DEFAULT 13 COMMENT 'bit_col_comment',
    bit_col4 BIT(64) DEFAULT 'abcde' COMMENT 'bit_col_comment',
    decimal_col DECIMAL(10, 2) DEFAULT 1.01 COMMENT 'decimal_col_comment',
    date_col DATE DEFAULT '1970-01-01' COMMENT 'date_col_comment',
    datetime_col DATETIME DEFAULT '1970-01-01 00:00:00' COMMENT 'datetime_col_comment',
    datetime_col2 DATETIME(6) DEFAULT '1970-01-01 00:00:00.000000' COMMENT 'datetime_col2_comment',
    datetime_col3 DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'datetime_col2_comment',
    timestamp_col TIMESTAMP DEFAULT '2024-01-01 00:00:00' COMMENT 'timestamp_col_comment',
    timestamp_col2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'timestamp_col2_comment',
    timestamp_col3 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'timestamp_col3_comment',
    time_col TIME DEFAULT '00:00:00' COMMENT 'time_col_comment',
    time_col2 TIME(2) DEFAULT '01:01:01.01' COMMENT 'time_col2_comment',
    year_col YEAR DEFAULT 1970 COMMENT 'year_col_comment',

    -- TODO: binary_col and varbinary_col default value is not supported
    binary_col BINARY(16) COMMENT 'binary_col_comment',
    varbinary_col VARBINARY(255) COMMENT 'varbinary_col_comment',
    -- binary_col BINARY(16) DEFAULT x'0123456789ABCDEF' COMMENT 'binary_col_comment',
    -- binary_col BINARY(16) DEFAULT 0x1234 COMMENT 'binary_col_comment',
    -- varbinary_col VARBINARY(255) DEFAULT x'0123456789ABCDEF' COMMENT 'varbinary_col_comment',
    -- varbinary_col VARBINARY(255) DEFAULT 0x1234 COMMENT 'varbinary_col_comment',

    blob_col BLOB COMMENT 'blob_col_comment',
    tinyblob_col TINYBLOB COMMENT 'tinyblob_col_comment',
    mediumblob_col MEDIUMBLOB COMMENT 'mediumblob_col_comment',
    longblob_col LONGBLOB COMMENT 'longblob_col_comment',
    enum_col ENUM('value1', 'value2', 'value3') DEFAULT 'value1' COMMENT 'enum_col_comment',
    set_col SET('option1', 'option2', 'option3') DEFAULT 'option1' COMMENT 'set_col_comment',
    json_col JSON DEFAULT NULL COMMENT 'json_col_comment'
) DEFAULT CHARSET=utf8mb4;

-- default value and comment
CREATE TABLE struct_it_mysql2mysql_1.special_default_and_comment (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    f_1 VARCHAR(255) NOT NULL DEFAULT 'abc''中文''' COMMENT '中文注释''f_1''' 
) COMMENT='中文注释''special_default_and_comment''';

-- full index type
CREATE TABLE struct_it_mysql2mysql_1.full_index_type(
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    f_1 int, 
    f_2 char(128),
    f_3 varchar(128),
    f_4 varchar(128),
    f_5 varchar(128),
    f_6 TEXT,
    f_7 TEXT, 
    f_8 TEXT, 
    f_9 POINT NOT NULL,
    f_10 varchar(10),
    f_11 varchar(10),
    f_12 varchar(10),
    f_13 varchar(10),
    f_14 TEXT,
    f_15 BLOB,
    f_16 varchar(10),
    KEY idx_btree_text_1 (f_10)
);

-- unique key with multiple columns
CREATE UNIQUE INDEX idx_unique_1 ON struct_it_mysql2mysql_1.full_index_type(f_1, f_2, f_3);

-- unique key with 1 column
CREATE UNIQUE INDEX idx_unique_2 ON struct_it_mysql2mysql_1.full_index_type(f_3);

-- HASH indexes are only for in-memory tables (or NDB) but not myISAM or InnoDB 
-- CREATE UNIQUE INDEX idx_unique_3 USING HASH ON struct_it_mysql2mysql_1.full_index_type(f_4, f_5);

-- fulltext key with multiple columns
CREATE FULLTEXT INDEX idx_full_text_1 ON struct_it_mysql2mysql_1.full_index_type(f_6, f_7, f_8);

-- fulltext key with 1 columns
CREATE FULLTEXT INDEX idx_full_text_2 ON struct_it_mysql2mysql_1.full_index_type(f_8);

-- spatial index
-- only 1 column supported in spatial key
CREATE SPATIAL INDEX idx_spatial_1 ON struct_it_mysql2mysql_1.full_index_type(f_9);

CREATE INDEX idx_btree_text_2 ON struct_it_mysql2mysql_1.full_index_type(f_11);

CREATE INDEX idx_btree_text_3 ON struct_it_mysql2mysql_1.full_index_type(f_13, f_12);

-- index with prefix length
CREATE INDEX idx_prefix_1 ON struct_it_mysql2mysql_1.full_index_type(f_14(10), f_15(12));
CREATE INDEX idx_prefix_2 ON struct_it_mysql2mysql_1.full_index_type(f_16(3));

-- full constraint
CREATE TABLE struct_it_mysql2mysql_1.constraint_table (
  id INT PRIMARY KEY AUTO_INCREMENT, 
  username VARCHAR(50) NOT NULL UNIQUE, 
  password VARCHAR(50) NOT NULL, 
  email VARCHAR(100) NOT NULL, 
  age INT, 
  gender ENUM('Male', 'Female', 'Other') NOT NULL, 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
  CONSTRAINT chk_age CHECK (age >= 18), 
  CONSTRAINT chk_email CHECK (email LIKE '%@%.%')
);

-- test view filtered
CREATE VIEW struct_it_mysql2mysql_1.full_column_type_view AS SELECT * FROM struct_it_mysql2mysql_1.full_column_type;

-- case sensitive column name
CREATE TABLE struct_it_mysql2mysql_1.case_sensitive_column_name (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT 'jack',
  `Age` int(11) NOT NULL DEFAULT '100',
  `GRADE` int(11) NOT NULL DEFAULT '100',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

-- contain db system keywords
CREATE TABLE struct_it_mysql2mysql_1.match (
  select_id INT PRIMARY KEY AUTO_INCREMENT,
  `table` VARCHAR(50) NOT NULL,
  `column` VARCHAR(100) NOT NULL,
  offset INT NOT NULL,
  unique_col VARCHAR(20),
  `match` INT,
  check_col INT,
  constraint_col INT
);

CREATE INDEX idx_index_on_index ON struct_it_mysql2mysql_1.match(offset);
CREATE INDEX idx_key_col ON struct_it_mysql2mysql_1.match(`match`);
CREATE UNIQUE INDEX uniq_unique_col ON struct_it_mysql2mysql_1.match(unique_col);

