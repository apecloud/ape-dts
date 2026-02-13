DROP TABLE IF EXISTS check_test;

CREATE TABLE check_test (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value INT
);

-- Pre-populate some data for UPDATE/DELETE operations
INSERT INTO check_test VALUES (1, 'alice', 100);
INSERT INTO check_test VALUES (2, 'bob', 200);
INSERT INTO check_test VALUES (3, 'charlie', 300);
