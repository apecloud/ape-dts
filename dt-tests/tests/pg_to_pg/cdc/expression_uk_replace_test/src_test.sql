INSERT INTO expression_uk_replace.mixed_rows VALUES (2, 'replay', 'replayed'), (3, NULL, 'replayed_null'), (4, '', 'replayed_empty');
INSERT INTO expression_uk_replace.mixed_rows VALUES (5, 'new_code', 'inserted');
INSERT INTO expression_uk_replace.primary_rows VALUES (2, 'B', 'source_value');

-- NULL and empty string have the same expression key, but the raw column must change.
UPDATE expression_uk_replace.mixed_rows SET code='', val='null_to_empty' WHERE id=1 AND code IS NULL;
UPDATE expression_uk_replace.mixed_rows SET val='after_update' WHERE id=1 AND code='code1';
UPDATE expression_uk_replace.mixed_rows SET code='changed', val='after_update' WHERE id=3 AND code IS NULL;
-- The ordinary UK does not prevent fallback when an expression UK also exists.
UPDATE expression_uk_replace.ordinary_rows SET code='B', val='after_update' WHERE id=1;
UPDATE expression_uk_replace.ordinary_rows SET val='after_update' WHERE id=2;
-- Keep expression-column and payload updates covered on the PK path.
UPDATE expression_uk_replace.primary_rows SET code='D', val='after_update' WHERE id=1;
UPDATE expression_uk_replace.primary_rows SET val='after_update' WHERE id=2;

DELETE FROM expression_uk_replace.mixed_rows WHERE id=1 AND code='code1';
DELETE FROM expression_uk_replace.ordinary_rows WHERE id=2;
DELETE FROM expression_uk_replace.primary_rows WHERE id=2;
