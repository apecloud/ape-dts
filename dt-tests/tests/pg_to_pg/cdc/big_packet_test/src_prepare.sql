DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;
CREATE TABLE test_db_1.large_packet_table (id int, payload text, PRIMARY KEY(id));
