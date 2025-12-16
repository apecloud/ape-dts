DROP DATABASE IF EXISTS struct_it_mysql2mysql_1;

-- Unknown character set: 'utf32'
CREATE DATABASE struct_it_mysql2mysql_1 DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci;

-- simple test
-- Unknown character set: 'utf16'
-- Unsupported collation when new collation is enabled: 'latin1_swedish_ci'
-- If COLLATE is NOT set for latin1 columns in MySQL, the default COLLATE will be latin1_swedish_ci
CREATE TABLE struct_it_mysql2mysql_1.table_test(
  col1 varchar(10), 
  col2 varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '', 
  col3 varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT 'bbb'
) DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;