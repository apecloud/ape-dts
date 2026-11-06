INSERT INTO expression_uk_conflict.rows VALUES (2, NULL, 'inserted');
UPDATE expression_uk_conflict.rows SET val='after_update' WHERE id=1 AND code IS NULL;

INSERT INTO expression_uk_conflict.plain_rows VALUES (8, '[1,2]', '<new/>', '"new"', 'inserted');
INSERT INTO expression_uk_conflict.expression_rows VALUES (4, 'D', '[1,2]', '<new/>', '"new"', 'inserted');

UPDATE expression_uk_conflict.plain_rows SET val='json_updated' WHERE id=1 AND j::text='{"kind":"first"}';
UPDATE expression_uk_conflict.plain_rows SET val='xml_updated' WHERE id=2 AND x::text='<x>first</x>';
UPDATE expression_uk_conflict.plain_rows SET val='key_order_updated' WHERE id=3 AND j::text='{"a":1,"b":2}';
UPDATE expression_uk_conflict.plain_rows SET val='whitespace_updated' WHERE id=4 AND j::text='{"a":1}';
UPDATE expression_uk_conflict.plain_rows SET j='null', x='', jb='null', val='null_to_values' WHERE id=5;
UPDATE expression_uk_conflict.plain_rows SET j=NULL, x=NULL, jb=NULL, val='values_to_null' WHERE id=6;
UPDATE expression_uk_conflict.expression_rows SET code='', j='{"a":2}', x='<updated/>', val='expression_updated' WHERE id=1;
UPDATE expression_uk_conflict.expression_rows SET j='null', x='', jb='null', val='null_to_values' WHERE id=2;
UPDATE expression_uk_conflict.expression_rows SET j=NULL, x=NULL, jb=NULL, val='values_to_null' WHERE id=3;

DELETE FROM expression_uk_conflict.plain_rows WHERE id=1 AND j::text='{"kind":"second"}';
DELETE FROM expression_uk_conflict.plain_rows WHERE id=2 AND x::text='<x>second</x>';
DELETE FROM expression_uk_conflict.plain_rows WHERE id=3 AND j::text='{"b":2,"a":1}';
DELETE FROM expression_uk_conflict.plain_rows WHERE id=4 AND j::text='{ "a": 1 }';
DELETE FROM expression_uk_conflict.plain_rows WHERE id IN (5, 6);
DELETE FROM expression_uk_conflict.expression_rows WHERE id IN (2, 3);

DELETE FROM expression_uk_conflict.rows WHERE id=1 AND code='code1';
