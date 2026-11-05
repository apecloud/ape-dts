DROP SCHEMA IF EXISTS expression_uk_replace CASCADE;
CREATE SCHEMA expression_uk_replace;

CREATE TABLE expression_uk_replace.mixed_rows (id INT NOT NULL, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX mixed_rows_uk ON expression_uk_replace.mixed_rows (id, COALESCE(code, ''));
INSERT INTO expression_uk_replace.mixed_rows VALUES (1, NULL, 'keep_null'), (1, 'code1', 'before_update');

CREATE TABLE expression_uk_replace.ordinary_rows (id INT NOT NULL UNIQUE, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX ordinary_rows_uk ON expression_uk_replace.ordinary_rows (lower(code));
INSERT INTO expression_uk_replace.ordinary_rows VALUES (1, 'A', 'before_update');

CREATE TABLE expression_uk_replace.primary_rows (id INT PRIMARY KEY, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX primary_rows_uk ON expression_uk_replace.primary_rows (lower(code));
INSERT INTO expression_uk_replace.primary_rows VALUES (1, 'A', 'before_update');

ALTER TABLE expression_uk_replace.mixed_rows REPLICA IDENTITY FULL;
ALTER TABLE expression_uk_replace.ordinary_rows REPLICA IDENTITY FULL;

-- Existing target conflicts must be ignored without a PK, including NULL/empty equivalence.
INSERT INTO expression_uk_replace.mixed_rows VALUES (2, 'replay', 'target_value'), (3, '', 'target_value');
-- A matching ordinary UK must not override a mismatched full before image.
INSERT INTO expression_uk_replace.ordinary_rows VALUES (2, 'drifted', 'before_update');
-- With a PK, replace must retain the existing payload-update behavior.
INSERT INTO expression_uk_replace.primary_rows VALUES (2, 'B', 'target_value');
