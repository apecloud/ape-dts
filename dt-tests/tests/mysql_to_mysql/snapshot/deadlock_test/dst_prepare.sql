DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;

-- 确保你的 MySQL 隔离级别是默认的 REPEATABLE READ
-- SET GLOBAL tx_isolation = 'REPEATABLE-READ';

-- 创建一个用于实验的表
```
CREATE TABLE `test_db_1`.`replace_deadlock` (
  `id` int(11) NOT NULL,
  `user_code_1` smallint UNSIGNED NOT NULL,
  `user_code_2` smallint UNSIGNED NOT NULL,
  `user_code_3` smallint UNSIGNED NOT NULL,
  `user_code_4` smallint UNSIGNED NOT NULL,
  `user_code_5` smallint UNSIGNED NOT NULL,
  `user_code_6` smallint UNSIGNED NOT NULL,
  `user_code_7` smallint UNSIGNED NOT NULL,
  `data` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_code_1` (`user_code_1`),
  UNIQUE KEY `uk_user_code_2` (`user_code_2`),
  UNIQUE KEY `uk_user_code_3` (`user_code_3`),
  UNIQUE KEY `uk_user_code_4` (`user_code_4`),
  UNIQUE KEY `uk_user_code_5` (`user_code_5`),
  UNIQUE KEY `uk_user_code_6` (`user_code_6`),
  UNIQUE KEY `uk_user_code_7` (`user_code_7`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```