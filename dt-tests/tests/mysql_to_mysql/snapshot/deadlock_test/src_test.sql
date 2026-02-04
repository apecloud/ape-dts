```
USE `test_db_2`;
DROP PROCEDURE IF EXISTS sp_load_replace_data;

CREATE PROCEDURE sp_load_replace_data()
BEGIN

    DECLARE i INT DEFAULT 0;
    
    WHILE i < 1000 DO
        SET @user_code = i;
            -- dst table already has most of data.
            -- sink data from resuming, unique key user_code_1 may conflict during REPLACE, primary index and unique index got gap locks.
        REPLACE INTO `replace_deadlock` (id, user_code_1, data) 
        VALUES (i*10, (1000-i)*10, CONCAT('data-', @user_code));

        SET i = i + 1;
    END WHILE;
    
END;
CALL sp_load_replace_data();
```