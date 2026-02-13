-- Use stored procedure to generate 1000 independent INSERT events for CDC
DROP PROCEDURE IF EXISTS test_db_1.generate_large_data;

```
CREATE PROCEDURE test_db_1.generate_large_data()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 1000 DO
        INSERT INTO test_db_1.check_large_test (id, name, value, data)
        VALUES (i, CONCAT('user_', i), i * 100, CONCAT('data_', i));
        SET i = i + 1;
    END WHILE;
END;
```

CALL test_db_1.generate_large_data();

DROP PROCEDURE IF EXISTS test_db_1.generate_large_data;
