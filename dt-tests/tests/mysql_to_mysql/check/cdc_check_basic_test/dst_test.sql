-- Pre-insert data (replace=false, INSERT conflict preserves dst data)
-- id=1,2: consistent
INSERT INTO test_db_1.check_test VALUES (1, 'alice', 100);
INSERT INTO test_db_1.check_test VALUES (2, 'bob', 200);
-- id=3: different value, CDC INSERT conflict keeps this -> diff (src=300 vs dst=999)
INSERT INTO test_db_1.check_test VALUES (3, 'charlie', 999);
