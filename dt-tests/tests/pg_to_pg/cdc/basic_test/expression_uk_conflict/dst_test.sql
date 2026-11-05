-- Replace the locked row after the CDC DELETE has taken its statement snapshot.
WITH old_row AS (
    DELETE FROM expression_uk_conflict.rows WHERE id=1 RETURNING id, code, val
)
INSERT INTO expression_uk_conflict.rows SELECT id, code, val FROM old_row;
