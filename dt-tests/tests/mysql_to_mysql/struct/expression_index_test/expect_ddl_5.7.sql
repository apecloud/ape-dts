struct_mysql_expression
CREATE DATABASE `struct_mysql_expression` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_bin */

struct_mysql_expression.expression_rows
CREATE TABLE `expression_rows` (
  `id` int(11) NOT NULL,
  `code` varchar(30) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `val` varchar(30) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  UNIQUE KEY `ordinary_uk` (`id`,`code`(5))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
