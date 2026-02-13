-- INSERT: new record
INSERT INTO check_test VALUES (4, 'david', 400);

-- UPDATE: modify existing record
UPDATE check_test SET name = 'alice_updated', value = 150 WHERE id = 1;

-- DELETE: remove existing record (checker validates row is deleted from target)
DELETE FROM check_test WHERE id = 3;

-- INSERT after DELETE: add new record
INSERT INTO check_test VALUES (5, 'eve', 500);

-- UPDATE multiple columns
UPDATE check_test SET value = 250 WHERE id = 2;
