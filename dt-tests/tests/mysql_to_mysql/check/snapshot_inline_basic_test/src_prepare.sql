DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.inline_check_table (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
