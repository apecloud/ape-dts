DROP SCHEMA IF EXISTS expression_uk_conflict CASCADE;
CREATE SCHEMA expression_uk_conflict;

CREATE TABLE expression_uk_conflict.rows (id INT NOT NULL, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX rows_uk ON expression_uk_conflict.rows (id, COALESCE(code, ''));
ALTER TABLE expression_uk_conflict.rows REPLICA IDENTITY FULL;
INSERT INTO expression_uk_conflict.rows VALUES (1, NULL, 'before_update');
