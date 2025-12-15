drop database if exists struct_check_test_1;

create database struct_check_test_1;

-- simple test
CREATE TABLE struct_check_test_1.match_table_test(
  id integer, 
  text varchar(10) comment 'col comment test', 
  primary key (id)
) comment 'table comment test';

-- full column type
CREATE TABLE struct_check_test_1.match_full_column_type (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
  varchar_col VARCHAR(255) NOT NULL, 
  char_col CHAR(10), 
  text_col TEXT, 
  tinyint_col TINYINT DEFAULT 0, 
  smallint_col SMALLINT, 
  mediumint_col MEDIUMINT, 
  int_col INT, 
  bigint_col BIGINT, 
  float_col FLOAT(8, 2), 
  double_col DOUBLE(16, 4), 
  decimal_col DECIMAL(10, 2), 
  date_col DATE, 
  datetime_col DATETIME, 
  timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  time_col TIME, 
  year_col YEAR, 
  binary_col BINARY(16), 
  varbinary_col VARBINARY(255), 
  blob_col BLOB, 
  tinyblob_col TINYBLOB, 
  mediumblob_col MEDIUMBLOB, 
  longblob_col LONGBLOB, 
  enum_col ENUM('value1', 'value2', 'value3'), 
  set_col SET('option1', 'option2', 'option3')
);

-- full index type
CREATE TABLE struct_check_test_1.match_full_index_type(id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    f_1 int, 
    f_2 char(128),
    f_3 varchar(128),
    f_4 varchar(128),
    f_5 varchar(128),
    f_6 TEXT,
    f_7 TEXT, 
    f_8 TEXT, 
    f_9 POINT NOT NULL
);

-- unique key with multiple columns
CREATE UNIQUE INDEX idx_unique_1 ON struct_check_test_1.match_full_index_type(f_1, f_2, f_3);

-- unique key with 1 column
CREATE UNIQUE INDEX idx_unique_2 ON struct_check_test_1.match_full_index_type(f_3);

-- HASH indexes are only for in-memory tables (or NDB) but not myISAM or InnoDB 
-- CREATE UNIQUE INDEX idx_unique_3 USING HASH ON struct_check_test_1.match_full_index_type(f_4, f_5);

-- fulltext key with multiple columns
CREATE FULLTEXT INDEX idx_full_text_1 ON struct_check_test_1.match_full_index_type(f_6, f_7, f_8);

-- fulltext key with 1 columns
CREATE FULLTEXT INDEX idx_full_text_2 ON struct_check_test_1.match_full_index_type(f_8);

-- spatial index
-- only 1 column supported in spatial key
CREATE SPATIAL INDEX idx_spatial_1 ON struct_check_test_1.match_full_index_type(f_9);

-- not match: table miss
-- CREATE TABLE struct_check_test_1.not_match_miss(id integer, text varchar(10) ,primary key (id)); 

-- not match: column
CREATE TABLE struct_check_test_1.not_match_column (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
  char_col CHAR(10), 
  text_col TEXT, 
  tinyint_col TINYINT DEFAULT 0, 
  smallint_col SMALLINT, 
  mediumint_col MEDIUMINT, 
  int_col INT, 
  bigint_col BIGINT, 
  float_col FLOAT(8, 2), 
  double_col DOUBLE(16, 4), 
  decimal_col DECIMAL(10, 2), 
  datetime_col DATETIME, 
  timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
  time_col TIME, 
  year_col YEAR, 
  binary_col BINARY(16), 
  varbinary_col VARBINARY(255), 
  blob_col BLOB, 
  tinyblob_col TINYBLOB, 
  mediumblob_col MEDIUMBLOB, 
  longblob_col LONGBLOB, 
  enum_col ENUM('value1', 'value2', 'value3'), 
  set_col SET('option1', 'option2', 'option3')
);

-- not match: index
CREATE TABLE struct_check_test_1.not_match_index (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
  unique_col VARCHAR(255) NOT NULL, 
  index_col VARCHAR(255), 
  fulltext_col TEXT, 
  spatial_col POINT NOT NULL, 
  simple_index_col VARCHAR(255), 
  composite_index_col1 VARCHAR(255), 
  composite_index_col2 VARCHAR(255), 
  composite_index_col3 VARCHAR(255)
);

CREATE INDEX i4_diff_order ON struct_check_test_1.not_match_index (composite_index_col3, composite_index_col2, composite_index_col1);
CREATE INDEX i5_diff_name_dst ON struct_check_test_1.not_match_index (index_col);
-- CREATE INDEX i6_miss ON struct_check_test_1.not_match_index (index_col);