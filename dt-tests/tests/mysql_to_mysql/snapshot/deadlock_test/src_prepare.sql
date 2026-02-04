DROP DATABASE IF EXISTS test_db_2;
CREATE DATABASE test_db_2;

-- SET GLOBAL tx_isolation = 'REPEATABLE-READ';

```
CREATE TABLE `test_db_2`.`replace_deadlock` (
  `id` int(11) NOT NULL,
  `user_code_1` int UNSIGNED NOT NULL,
  `data` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_code_1` (`user_code_1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```