drop database if exists struct_it_mysql2mysql_1;

create database if not exists struct_it_mysql2mysql_1;

-- expression defaults are surpported in mysql 8.0
-- https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
-- The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
CREATE TABLE struct_it_mysql2mysql_1.expression_defaults (
  -- literal defaults
  i INT         DEFAULT 0,
  c VARCHAR(10) DEFAULT '',
  -- expression defaults
  f FLOAT       DEFAULT (RAND() * RAND()),
  b BINARY(16)  DEFAULT (UUID_TO_BIN(UUID())),
  d DATE        DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR),
  p POINT       DEFAULT (Point(0,0)),
  j JSON        DEFAULT (JSON_ARRAY())
);
