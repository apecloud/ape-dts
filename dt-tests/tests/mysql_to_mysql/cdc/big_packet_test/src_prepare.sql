DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;
CREATE TABLE test_db_1.large_packet_table ( id int, payload longtext, PRIMARY KEY(id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
