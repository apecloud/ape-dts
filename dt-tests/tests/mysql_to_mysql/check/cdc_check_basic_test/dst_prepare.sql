DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.check_test (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
