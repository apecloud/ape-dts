-- Generate large batch of INSERTs (10000 rows)
INSERT INTO check_large_test (id, name, value, data)
SELECT i,
       'user_' || i::text,
       i * 100,
       'data_' || i::text
FROM generate_series(1, 10000) AS i;
