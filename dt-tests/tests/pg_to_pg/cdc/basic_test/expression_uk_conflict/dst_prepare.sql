DROP SCHEMA IF EXISTS expression_uk_conflict CASCADE;
CREATE SCHEMA expression_uk_conflict;

CREATE TABLE expression_uk_conflict.rows (id INT NOT NULL, code VARCHAR(30), val TEXT);
CREATE UNIQUE INDEX rows_uk ON expression_uk_conflict.rows (id, COALESCE(code, ''));
ALTER TABLE expression_uk_conflict.rows REPLICA IDENTITY FULL;
INSERT INTO expression_uk_conflict.rows VALUES (1, NULL, 'before_update'), (1, 'code1', 'sibling');

CREATE TABLE expression_uk_conflict.plain_rows (id INT, j JSON, x XML, jb JSONB, val TEXT);
CREATE TABLE expression_uk_conflict.expression_rows (id INT NOT NULL UNIQUE, code VARCHAR(30), j JSON, x XML, jb JSONB, val TEXT);
CREATE UNIQUE INDEX expression_rows_uk ON expression_uk_conflict.expression_rows ((COALESCE(code, '')));
ALTER TABLE expression_uk_conflict.plain_rows REPLICA IDENTITY FULL;
ALTER TABLE expression_uk_conflict.expression_rows REPLICA IDENTITY FULL;
-- Reverse target row order to expose predicates that omit JSON/XML or normalize JSON.
INSERT INTO expression_uk_conflict.plain_rows VALUES
(6, '{"a":1}', '<x/>', '{"ok":true}', 'values_before'),
(5, NULL, NULL, NULL, 'null_before'),
(4, '{ "a": 1 }', '<x/>', '{"ok":true}', 'same'),
(4, '{"a":1}', '<x/>', '{"ok":true}', 'same'),
(3, '{"b":2,"a":1}', '<x/>', '{"ok":true}', 'same'),
(3, '{"a":1,"b":2}', '<x/>', '{"ok":true}', 'same'),
(2, '{"kind":"xml"}', '<x>second</x>', '{"ok":true}', 'same'),
(2, '{"kind":"xml"}', '<x>first</x>', '{"ok":true}', 'same'),
(1, '{"kind":"second"}', '<x/>', '{"ok":true}', 'same'),
(1, '{"kind":"first"}', '<x/>', '{"ok":true}', 'same');
INSERT INTO expression_uk_conflict.expression_rows VALUES
(1, NULL, '{"a":1}', '<x/>', '{"ok":true}', 'same'),
(2, 'B', NULL, NULL, NULL, 'null_before'),
(3, 'C', '{"a":1}', '<x/>', '{"ok":true}', 'values_before');
