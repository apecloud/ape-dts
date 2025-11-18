-- Insert partial data in destination (missing id=3, different value for id=2)
INSERT INTO test_schema_1.test_table (id, name, age, email) VALUES 
(1, 'Alice', 30, 'alice@example.com'),
(2, 'Bob', 99, 'bob_updated@example.com');
-- id=3 is missing (will generate miss log with INSERT SQL)
-- id=2 has different age and email (will generate diff log with UPDATE SQL using PK match)
