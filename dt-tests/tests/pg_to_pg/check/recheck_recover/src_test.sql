INSERT INTO recheck_table (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
INSERT INTO recheck_table (id, name) VALUES (2, 'Bob')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
