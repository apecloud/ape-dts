-- user_orders: enum in test_db_1 schema
INSERT INTO test_db_1.user_orders (user_name, current_status) VALUES ('one', 'active'::test_db_1.my_status);
INSERT INTO test_db_1.user_orders (user_name, current_status) VALUES ('two', 'inactive'::test_db_1.my_status);
UPDATE test_db_1.user_orders SET current_status = 'active'::test_db_1.my_status WHERE order_id = 2;
UPDATE test_db_1.user_orders SET current_status = NULL WHERE order_id = 1;
DELETE FROM test_db_1.user_orders WHERE order_id = 1;

-- task_list: enum only in test_db_1
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Fix bug', 'high'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Write docs', 'low'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Deploy', 'critical'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Cleanup', NULL);
UPDATE test_db_1.task_list SET priority = 'medium'::test_db_1.priority_level WHERE task_id = 2;
UPDATE test_db_1.task_list SET priority = NULL WHERE task_id = 1;
DELETE FROM test_db_1.task_list WHERE task_id = 4;

-- composite_type_table
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (ROW('123 Main St', 'Springfield', '62704', 'US')::test_db_1.address_type, ROW('456 Oak Ave', 'Shelbyville', '62705', 'US')::test_db_1.address_type, ROW('John', 'Doe')::test_db_1.full_name);
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (ROW('789 Elm Rd', 'Capital City', '10001', 'US')::test_db_1.address_type, NULL, ROW('Jane', 'Smith')::test_db_1.full_name);
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (NULL, NULL, NULL);
UPDATE test_db_1.composite_type_table SET person_name = ROW('Jane', 'Doe')::test_db_1.full_name WHERE pk = 2;
UPDATE test_db_1.composite_type_table SET home_addr = NULL WHERE pk = 1;
DELETE FROM test_db_1.composite_type_table WHERE pk = 3;

-- custom_range_table
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('[1.5, 9.9)');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('(0.0, 100.0]');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('empty');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES (NULL);
UPDATE test_db_1.custom_range_table SET price_range = '[2.0, 10.0)' WHERE pk = 1;
UPDATE test_db_1.custom_range_table SET price_range = NULL WHERE pk = 2;
DELETE FROM test_db_1.custom_range_table WHERE pk = 4;

-- domain_type_table
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (42, 'user@example.com', 'production');
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (1, 'admin@test.org', 'staging');
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (NULL, NULL, 'default');
UPDATE test_db_1.domain_type_table SET quantity = 100, contact = 'updated@example.com' WHERE pk = 1;
UPDATE test_db_1.domain_type_table SET quantity = NULL, contact = NULL WHERE pk = 2;
DELETE FROM test_db_1.domain_type_table WHERE pk = 3;

-- special_type_name_table: type names with special characters
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES ('a'::test_db_1."type:colon", 'x'::test_db_1."type,comma", ROW(1, 'hello')::test_db_1."type space", 10);
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES ('b'::test_db_1."type:colon", 'y'::test_db_1."type,comma", ROW(2, 'world')::test_db_1."type space", 0);
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES (NULL, NULL, NULL, NULL);
UPDATE test_db_1.special_type_name_table SET col_colon = 'c'::test_db_1."type:colon", col_space = ROW(3, 'updated')::test_db_1."type space" WHERE pk = 1;
UPDATE test_db_1.special_type_name_table SET col_colon = NULL, col_comma = NULL WHERE pk = 2;
DELETE FROM test_db_1.special_type_name_table WHERE pk = 3;
