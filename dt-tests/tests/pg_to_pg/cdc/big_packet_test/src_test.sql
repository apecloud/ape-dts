INSERT INTO test_db_1.large_packet_table SELECT 1, repeat('x', 20 * 1024 * 1024);
