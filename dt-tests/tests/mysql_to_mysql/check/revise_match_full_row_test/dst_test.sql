-- Insert data with differences in destination
INSERT INTO test_db_1.test_table (id, name, age, email) VALUES 
(1, 'Alice', 30, 'alice@example.com'),
(2, 'Bob', 99, 'bob_updated@example.com');
-- id=2 has different age and email (will generate diff log with UPDATE SQL using full row match)
