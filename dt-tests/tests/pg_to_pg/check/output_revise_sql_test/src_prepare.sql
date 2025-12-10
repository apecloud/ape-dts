DROP SCHEMA IF EXISTS test_schema_1 CASCADE;

CREATE SCHEMA test_schema_1;

CREATE TABLE test_schema_1.test_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    email VARCHAR(100)
);
