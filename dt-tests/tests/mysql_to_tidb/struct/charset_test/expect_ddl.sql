struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */

struct_it_mysql2mysql_1.table_test
CREATE TABLE `table_test` (
  `col1` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `col2` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT '',
  `col3` varchar(10) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT 'bbb'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
