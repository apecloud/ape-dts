DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

-- STRING == varchar(65533)
CREATE TABLE IF NOT EXISTS `test_db_1`.`json_test` (
	`f_0` INT NOT NULL,
	`f_1` STRING
) UNIQUE KEY (`f_0`) DISTRIBUTED BY HASH(`f_0`) PROPERTIES ("replication_num" = "1")
