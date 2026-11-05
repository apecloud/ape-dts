DROP DATABASE IF EXISTS struct_mysql_expression;
CREATE DATABASE struct_mysql_expression CHARACTER SET latin1 COLLATE latin1_bin;
CREATE TABLE struct_mysql_expression.expression_rows (
  id INT NOT NULL,
  code VARCHAR(30),
  val VARCHAR(30),
  UNIQUE KEY ordinary_uk (id, code(5))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

-- Older versions keep the ordinary prefix UK and skip functional indexes.
/*!80013 CREATE UNIQUE INDEX mixed_uk ON struct_mysql_expression.expression_rows (id, (COALESCE(code, ''))) */;
/*!80013 CREATE UNIQUE INDEX expression_uk ON struct_mysql_expression.expression_rows ((LOWER(code))) */;
/*!80013 CREATE UNIQUE INDEX multi_expression_uk ON struct_mysql_expression.expression_rows ((LOWER(code)), id, (COALESCE(val, ''))) */;
/*!80013 CREATE INDEX expression_idx ON struct_mysql_expression.expression_rows ((LENGTH(val))) */;
/*!80013 CREATE UNIQUE INDEX escaped_uk ON struct_mysql_expression.expression_rows ((COALESCE(val, 'it''s\\ok'))) */;
