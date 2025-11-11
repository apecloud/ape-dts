-- REPLACE INTO `test_db_1`.`replace_deadlock` (user_code_1, user_code_2, user_code_3, user_code_4, user_code_5, user_code_6, user_code_7, data) VALUES (15000, 15001, 15002, 15003, 15004, 15005, 15006, 'data'), (30000, 30001, 30002, 30003, 30004, 30005, 30006, 'data'), (45000, 45001, 45002, 45003, 45004, 45005, 45006, 'data'), (60000, 60001, 60002, 60003, 60004, 60005, 60006, 'data');
REPLACE INTO `test_db_1`.`replace_deadlock` (user_code_1, user_code_2, user_code_3, user_code_4, user_code_5, user_code_6, user_code_7, data) VALUES (30000, 30001, 30002, 30003, 30004, 30005, 30006, 'data');

```
USE `test_db_1`;
DROP PROCEDURE IF EXISTS sp_load_replace_data;

CREATE PROCEDURE sp_load_replace_data()
BEGIN

    DECLARE i INT DEFAULT 0;
    
    WHILE i < 100 DO
        SET @user_code = i;

        REPLACE INTO `replace_deadlock` (id, user_code_1, user_code_2, user_code_3, user_code_4, user_code_5, user_code_6, user_code_7, data) 
        VALUES (i, FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), FLOOR(RAND() * 65536), CONCAT('data-', @user_code));

        SET i = i + 1;
    END WHILE;
    
END;
CALL sp_load_replace_data();
```