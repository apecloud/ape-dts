DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;

CREATE TABLE test_db_1.inline_check_table (
    id BIGINT PRIMARY KEY,
    name TEXT,
    age INT
);
