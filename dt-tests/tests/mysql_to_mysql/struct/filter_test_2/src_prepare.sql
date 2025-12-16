drop database if exists struct_it_mysql2mysql_1;

create database if not exists struct_it_mysql2mysql_1;

-- full index type
CREATE TABLE struct_it_mysql2mysql_1.full_index_type (
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

CREATE UNIQUE INDEX unique_index ON struct_it_mysql2mysql_1.full_index_type (unique_col);

CREATE INDEX index_index ON struct_it_mysql2mysql_1.full_index_type (index_col);

CREATE INDEX simple_index ON struct_it_mysql2mysql_1.full_index_type (simple_index_col);

CREATE INDEX composite_index ON struct_it_mysql2mysql_1.full_index_type (composite_index_col1, composite_index_col2, composite_index_col3);

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

ALTER TABLE struct_it_mysql2mysql_1.foreign_key_child ADD CONSTRAINT fk_test_1 FOREIGN KEY (child_col_1) REFERENCES struct_it_mysql2mysql_1.foreign_key_parent (parent_col_1);
ALTER TABLE struct_it_mysql2mysql_1.foreign_key_child ADD CONSTRAINT fk_test_2 FOREIGN KEY (child_col_2) REFERENCES struct_it_mysql2mysql_1.foreign_key_parent (parent_col_2);