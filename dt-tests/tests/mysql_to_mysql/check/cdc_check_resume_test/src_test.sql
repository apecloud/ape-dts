-- INSERTs captured via CDC from source
INSERT INTO test_db_1.check_test VALUES (1, 'alice', 100);
INSERT INTO test_db_1.check_test VALUES (2, 'bob', 200);
INSERT INTO test_db_1.check_test VALUES (3, 'charlie', 300);
INSERT INTO test_db_1.check_test VALUES (4, 'david', 400);
INSERT INTO test_db_1.check_test VALUES (5, 'eve', 500);

-- DELETE captured via CDC from source
DELETE FROM test_db_1.check_test WHERE id = 3;
DELETE FROM test_db_1.check_test WHERE id = 5;
