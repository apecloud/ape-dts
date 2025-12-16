DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

-- `id` int(11), can be extracted parallelly, with empty data
CREATE TABLE test_db_1.tb_0 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- `id` int(11), can be extracted parallelly 
CREATE TABLE test_db_1.tb_1 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- `id` varchar(255), can not be extracted parallelly
CREATE TABLE test_db_1.tb_2 (`id` varchar(255) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- no primary key, can not be extracted parallelly
CREATE TABLE test_db_1.tb_3 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL); 

CREATE TABLE test_db_1.where_condition_1 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.where_condition_2 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 