drop database if exists struct_it_mysql2mysql_1;

create database if not exists struct_it_mysql2mysql_1;

-- create table with only primary and unique indexes
CREATE TABLE struct_it_mysql2mysql_1.full_index_type (
  `id` int unsigned NOT NULL AUTO_INCREMENT, 
  `unique_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL, 
  `index_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, 
  `fulltext_col` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci, 
  `spatial_col` point NOT NULL, 
  `simple_index_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, 
  `composite_index_col1` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, 
  `composite_index_col2` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, 
  `composite_index_col3` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL, 
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3

CREATE UNIQUE INDEX unique_index ON struct_it_mysql2mysql_1.full_index_type (unique_col);

-- create table without constraints
CREATE TABLE struct_it_mysql2mysql_1.`constraint_table` (
  `id` int NOT NULL AUTO_INCREMENT, 
  `username` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL, 
  `password` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL, 
  `email` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL, 
  `age` int DEFAULT NULL, 
  `gender` enum('Male', 'Female', 'Other') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL, 
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP, 
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
  PRIMARY KEY (`id`), 
  UNIQUE KEY `username` (`username`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb3;

-- foreign constraints
CREATE TABLE struct_it_mysql2mysql_1.foreign_key_parent (
  pk int, 
  parent_col_1 int UNIQUE, 
  parent_col_2 int UNIQUE, 
  PRIMARY KEY(pk)
);

CREATE TABLE struct_it_mysql2mysql_1.foreign_key_child (
  pk int, 
  child_col_1 int UNIQUE, 
  child_col_2 int UNIQUE, 
  PRIMARY KEY(pk)
);