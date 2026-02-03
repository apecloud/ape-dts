struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8 */

struct_it_mysql2mysql_1.full_column_type
CREATE TABLE `full_column_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `varchar_col` varchar(255) NOT NULL COMMENT 'varchar_col_comment',
  `char_col` char(10) DEFAULT NULL COMMENT 'char_col_comment',
  `tinytext_col` tinytext COMMENT 'tinytext_col_comment',
  `mediumtext_col` mediumtext COMMENT 'mediumtext_col_comment',
  `longtext_col` longtext COMMENT 'longtext_col_comment',
  `text_col` text COMMENT 'text_col_comment',
  `tinyint_col` tinyint(4) DEFAULT NULL COMMENT 'tinyint_col_comment',
  `smallint_col` smallint(6) DEFAULT NULL COMMENT 'smallint_col_comment',
  `mediumint_col` mediumint(9) DEFAULT NULL COMMENT 'mediumint_col_comment',
  `int_col` int(11) DEFAULT NULL COMMENT 'int_col_comment',
  `bigint_col` bigint(20) DEFAULT NULL COMMENT 'bigint_col_comment',
  `float_col` float(8,2) DEFAULT NULL COMMENT 'float_col_comment',
  `double_col` double(16,4) DEFAULT NULL COMMENT 'double_col_comment',
  `bit_col` bit(64) DEFAULT NULL COMMENT 'bit_col_comment',
  `decimal_col` decimal(10,2) DEFAULT NULL COMMENT 'decimal_col_comment',
  `date_col` date DEFAULT NULL COMMENT 'date_col_comment',
  `datetime_col` datetime DEFAULT NULL COMMENT 'datetime_col_comment',
  `datetime_col2` datetime(6) DEFAULT NULL COMMENT 'datetime_col2_comment',
  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'timestamp_col_comment',
  `timestamp_col2` timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000' COMMENT 'timestamp_col2_comment',
  `time_col` time DEFAULT NULL COMMENT 'time_col_comment',
  `time_col2` time(2) DEFAULT NULL COMMENT 'time_col2_comment',
  `year_col` year(4) DEFAULT NULL COMMENT 'year_col_comment',
  `binary_col` binary(16) DEFAULT NULL COMMENT 'binary_col_comment',
  `varbinary_col` varbinary(255) DEFAULT NULL COMMENT 'varbinary_col_comment',
  `blob_col` blob COMMENT 'blob_col_comment',
  `tinyblob_col` tinyblob COMMENT 'tinyblob_col_comment',
  `mediumblob_col` mediumblob COMMENT 'mediumblob_col_comment',
  `longblob_col` longblob COMMENT 'longblob_col_comment',
  `enum_col` enum('value1','value2','value3') DEFAULT NULL COMMENT 'enum_col_comment',
  `set_col` set('option1','option2','option3') DEFAULT NULL COMMENT 'set_col_comment',
  `json_col` json DEFAULT NULL COMMENT 'json_col_comment',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.full_column_type_with_default
CREATE TABLE `full_column_type_with_default` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `varchar_col` varchar(255) NOT NULL DEFAULT 'abc中文' COMMENT 'varchar_col_comment',
  `char_col` char(255) DEFAULT 'abc中文' COMMENT 'char_col_comment',
  `tinytext_col` tinytext COMMENT 'tinytext_col_comment',
  `mediumtext_col` mediumtext COMMENT 'mediumtext_col_comment',
  `longtext_col` longtext COMMENT 'longtext_col_comment',
  `text_col` text COMMENT 'text_col_comment',
  `tinyint_col` tinyint(4) DEFAULT '10' COMMENT 'tinyint_col_comment',
  `smallint_col` smallint(6) DEFAULT '100' COMMENT 'smallint_col_comment',
  `mediumint_col` mediumint(9) DEFAULT '1000' COMMENT 'mediumint_col_comment',
  `int_col` int(11) DEFAULT '10000' COMMENT 'int_col_comment',
  `bigint_col` bigint(20) DEFAULT '100000' COMMENT 'bigint_col_comment',
  `float_col` float(8,2) DEFAULT '1.01' COMMENT 'float_col_comment',
  `double_col` double(16,4) DEFAULT '1.0001' COMMENT 'double_col_comment',
  `bit_col` bit(1) DEFAULT b'1' COMMENT 'bit_col_comment',
  `bit_col2` bit(1) DEFAULT b'0' COMMENT 'bit_col_comment',
  `bit_col3` bit(64) DEFAULT b'1101' COMMENT 'bit_col_comment',
  `bit_col4` bit(64) DEFAULT b'110000101100010011000110110010001100101' COMMENT 'bit_col_comment',
  `decimal_col` decimal(10,2) DEFAULT '1.01' COMMENT 'decimal_col_comment',
  `date_col` date DEFAULT '1970-01-01' COMMENT 'date_col_comment',
  `datetime_col` datetime DEFAULT '1970-01-01 00:00:00' COMMENT 'datetime_col_comment',
  `datetime_col2` datetime(6) DEFAULT '1970-01-01 00:00:00.000000' COMMENT 'datetime_col2_comment',
  `datetime_col3` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'datetime_col2_comment',
  `timestamp_col` timestamp NOT NULL DEFAULT '2024-01-01 00:00:00' COMMENT 'timestamp_col_comment',
  `timestamp_col2` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'timestamp_col2_comment',
  `timestamp_col3` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'timestamp_col3_comment',
  `time_col` time DEFAULT '00:00:00' COMMENT 'time_col_comment',
  `time_col2` time(2) DEFAULT '01:01:01.01' COMMENT 'time_col2_comment',
  `year_col` year(4) DEFAULT '1970' COMMENT 'year_col_comment',
  `binary_col` binary(16) DEFAULT NULL COMMENT 'binary_col_comment',
  `varbinary_col` varbinary(255) DEFAULT NULL COMMENT 'varbinary_col_comment',
  `blob_col` blob COMMENT 'blob_col_comment',
  `tinyblob_col` tinyblob COMMENT 'tinyblob_col_comment',
  `mediumblob_col` mediumblob COMMENT 'mediumblob_col_comment',
  `longblob_col` longblob COMMENT 'longblob_col_comment',
  `enum_col` enum('value1','value2','value3') DEFAULT 'value1' COMMENT 'enum_col_comment',
  `set_col` set('option1','option2','option3') DEFAULT 'option1' COMMENT 'set_col_comment',
  `json_col` json DEFAULT NULL COMMENT 'json_col_comment',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

struct_it_mysql2mysql_1.special_default_and_comment
CREATE TABLE `special_default_and_comment` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `f_1` varchar(255) NOT NULL DEFAULT 'abc''中文''' COMMENT '中文注释''f_1''',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='中文注释''special_default_and_comment'''

struct_it_mysql2mysql_1.full_index_type
CREATE TABLE `full_index_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `f_1` int(11) DEFAULT NULL,
  `f_2` char(128) DEFAULT NULL,
  `f_3` varchar(128) DEFAULT NULL,
  `f_4` varchar(128) DEFAULT NULL,
  `f_5` varchar(128) DEFAULT NULL,
  `f_6` text,
  `f_7` text,
  `f_8` text,
  `f_9` point NOT NULL,
  `f_10` varchar(10) DEFAULT NULL,
  `f_11` varchar(10) DEFAULT NULL,
  `f_12` varchar(10) DEFAULT NULL,
  `f_13` varchar(10) DEFAULT NULL,
  `f_14` text,
  `f_15` blob,
  `f_16` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique_1` (`f_1`,`f_2`,`f_3`),
  UNIQUE KEY `idx_unique_2` (`f_3`),
  SPATIAL KEY `idx_spatial_1` (`f_9`),
  FULLTEXT KEY `idx_full_text_1` (`f_6`,`f_7`,`f_8`),
  FULLTEXT KEY `idx_full_text_2` (`f_8`),
  KEY `idx_btree_text_1` (`f_10`),
  KEY `idx_btree_text_2` (`f_11`),
  KEY `idx_btree_text_3` (`f_13`,`f_12`),
  KEY `idx_prefix_1` (`f_14`(10),`f_15`(12)),
  KEY `idx_prefix_2` (`f_16`(3))
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.constraint_table
CREATE TABLE `constraint_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `password` varchar(50) NOT NULL,
  `email` varchar(100) NOT NULL,
  `age` int(11) DEFAULT NULL,
  `gender` enum('Male','Female','Other') NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.case_sensitive_column_name
CREATE TABLE `case_sensitive_column_name` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT 'jack',
  `Age` int(11) NOT NULL DEFAULT '100',
  `GRADE` int(11) NOT NULL DEFAULT '100',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.match
CREATE TABLE `match` (
  `select_id` int NOT NULL AUTO_INCREMENT,
  `table` varchar(50) NOT NULL,
  `column` varchar(100) NOT NULL,
  `offset` int NOT NULL,
  `unique_col` varchar(20) DEFAULT NULL,
  `match` int DEFAULT NULL,
  `check_col` int DEFAULT NULL,
  `constraint_col` int DEFAULT NULL,
  PRIMARY KEY (`select_id`),
  UNIQUE KEY `uniq_unique_col` (`unique_col`),
  KEY `idx_index_on_index` (`offset`),
  KEY `idx_key_col` (`match`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8