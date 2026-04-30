INSERT INTO test_db_1.large_packet_table VALUES(1, 'seed');
INSERT INTO test_db_1.large_packet_table VALUES(2, 'seed');
INSERT INTO test_db_1.large_packet_table VALUES(3, 'seed');
INSERT INTO test_db_1.large_packet_table VALUES(4, 'seed');
INSERT INTO test_db_1.large_packet_table VALUES(5, 'seed');
UPDATE test_db_1.large_packet_table SET payload = repeat('x', 10 * 1024 * 1024) WHERE id <= 5;
