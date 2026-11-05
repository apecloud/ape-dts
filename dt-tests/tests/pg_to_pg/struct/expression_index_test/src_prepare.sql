DROP SCHEMA IF EXISTS struct_pg_expression CASCADE;
CREATE SCHEMA struct_pg_expression;
CREATE TABLE struct_pg_expression.expression_rows (id INT NOT NULL, code VARCHAR(30), val VARCHAR(30));
CREATE UNIQUE INDEX ordinary_uk ON struct_pg_expression.expression_rows (id, code);
CREATE UNIQUE INDEX mixed_uk ON struct_pg_expression.expression_rows (id, (COALESCE(code, '')));
CREATE UNIQUE INDEX expression_uk ON struct_pg_expression.expression_rows ((LOWER(code)));
CREATE UNIQUE INDEX multi_expression_uk ON struct_pg_expression.expression_rows ((LOWER(code)), id, (COALESCE(val, '')));
CREATE INDEX expression_idx ON struct_pg_expression.expression_rows ((LENGTH(val)));
CREATE UNIQUE INDEX escaped_uk ON struct_pg_expression.expression_rows ((COALESCE(val, 'it''s')));
