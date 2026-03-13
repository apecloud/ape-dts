INSERT INTO test_db_1.user_orders (user_name, current_status) VALUES ('one', 'active'::test_db_1.my_status);

-- test enum only in test_db_1
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Fix bug', 'high'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Write docs', 'low'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Deploy', 'critical'::test_db_1.priority_level);
INSERT INTO test_db_1.task_list (title, priority) VALUES ('Cleanup', NULL);

-- test composite type
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (ROW('123 Main St', 'Springfield', '62704', 'US')::test_db_1.address_type, ROW('456 Oak Ave', 'Shelbyville', '62705', 'US')::test_db_1.address_type, ROW('John', 'Doe')::test_db_1.full_name);
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (ROW('789 Elm Rd', 'Capital City', '10001', 'US')::test_db_1.address_type, NULL, ROW('Jane', 'Smith')::test_db_1.full_name);
INSERT INTO test_db_1.composite_type_table (home_addr, work_addr, person_name) VALUES (NULL, NULL, NULL);

-- test custom range type
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('[1.5, 9.9)');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('(0.0, 100.0]');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES ('empty');
INSERT INTO test_db_1.custom_range_table (price_range) VALUES (NULL);

-- test domain type
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (42, 'user@example.com', 'production');
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (1, 'admin@test.org', 'staging');
INSERT INTO test_db_1.domain_type_table (quantity, contact, label) VALUES (NULL, NULL, 'default');

-- test type names with special characters
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES ('a'::test_db_1."type:colon", 'x'::test_db_1."type,comma", ROW(1, 'hello')::test_db_1."type space", 10);
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES ('b'::test_db_1."type:colon", 'y'::test_db_1."type,comma", ROW(2, 'world')::test_db_1."type space", 0);
INSERT INTO test_db_1.special_type_name_table (col_colon, col_comma, col_space, col_domain_dash) VALUES (NULL, NULL, NULL, NULL);
