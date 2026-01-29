DROP DATABASE IF EXISTS test_db_2;
CREATE DATABASE test_db_2;

-- 创建一个用于实验的表

```
CREATE TABLE `test_db_2`.`replace_deadlock` (
  `id` int(11) NOT NULL,
  `user_code_1` int UNSIGNED NOT NULL,
  `data` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_code_1` (`user_code_1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

```
USE `test_db_2`;
DROP PROCEDURE IF EXISTS sp_load_replace_data;

CREATE PROCEDURE sp_load_replace_data()
BEGIN

    DECLARE i INT DEFAULT 0;
    
    WHILE i < 990 DO
        SET @user_code = i;

            REPLACE INTO `replace_deadlock` (id, user_code_1, data) 
            VALUES (i*10, (1000-i)*10, CONCAT('data-', @user_code));

        SET i = i + 1;
    END WHILE;
    
END;
CALL sp_load_replace_data();
```