DROP DATABASE IF EXISTS struct_it_mysql2mysql_1;

CREATE DATABASE struct_it_mysql2mysql_1 DEFAULT CHARACTER SET utf32 DEFAULT COLLATE utf32_polish_ci;

-- simple test
CREATE TABLE struct_it_mysql2mysql_1.table_test(
  col1 varchar(10), 
  col2 varchar(10) CHARACTER SET latin1 DEFAULT '', 
  col3 varchar(10) CHARACTER SET latin1 COLLATE latin1_spanish_ci DEFAULT 'bbb'
) DEFAULT CHARSET = utf16 COLLATE = utf16_unicode_ci;