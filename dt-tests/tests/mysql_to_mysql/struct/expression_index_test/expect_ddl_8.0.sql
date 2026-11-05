struct_mysql_expression
CREATE DATABASE `struct_mysql_expression` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_bin */ /*!80016 DEFAULT ENCRYPTION='N' */

struct_mysql_expression.expression_rows
CREATE TABLE `expression_rows` (
  `id` int NOT NULL,
  `code` varchar(30) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `val` varchar(30) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  UNIQUE KEY `escaped_uk` ((coalesce(`val`,_latin1'it\'s\\ok'))),
  UNIQUE KEY `ordinary_uk` (`id`,`code`(5)),
  UNIQUE KEY `mixed_uk` (`id`,(coalesce(`code`,_latin1''))),
  UNIQUE KEY `expression_uk` ((lower(`code`))),
  UNIQUE KEY `multi_expression_uk` ((lower(`code`)),`id`,(coalesce(`val`,_latin1''))),
  KEY `expression_idx` ((length(`val`)))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
