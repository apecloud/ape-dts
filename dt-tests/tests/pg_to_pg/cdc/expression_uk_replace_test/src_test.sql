UPDATE expression_uk_replace.mixed_rows SET val='after_update' WHERE id=1 AND code='code1';
INSERT INTO expression_uk_replace.mixed_rows VALUES (2, 'replay', 'source_value'), (3, NULL, 'source_value');
UPDATE expression_uk_replace.ordinary_rows SET code='B', val='after_update' WHERE id=1;
UPDATE expression_uk_replace.ordinary_rows SET val='must_not_overwrite' WHERE id=2;
UPDATE expression_uk_replace.primary_rows SET val='after_update' WHERE id=1;
INSERT INTO expression_uk_replace.primary_rows VALUES (2, 'B', 'source_value');
