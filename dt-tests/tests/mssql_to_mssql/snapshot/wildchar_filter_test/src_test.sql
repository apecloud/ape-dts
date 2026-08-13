INSERT INTO test_db_1.one_pk_no_uk_1 VALUES (1, -123456789012.12345678, N'db1-table1 中文', 0x0001), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_1.one_pk_no_uk_2 VALUES (1, 123456789012.12345678, N'db1-table2 😀', 0x0100), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_2.one_pk_no_uk_1 VALUES (1, -22.22000000, N'db2-table1 日本語', 0x0002), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_2.one_pk_no_uk_2 VALUES (1, 22.22000000, N'db2-table2 대한민국', 0x0200), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_3.one_pk_no_uk_1 VALUES (1, -33.33000000, N'db3-table1 ignored schema', 0x0003), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_3.one_pk_no_uk_2 VALUES (1, 33.33000000, N'db3-table2 ignored schema', 0x0300), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_4.one_pk_no_uk_1 VALUES (1, -44.44000000, N'db4-table1 ignored table', 0x0004), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_4.one_pk_no_uk_2 VALUES (1, 44.44000000, N'db4-table2 ignored table', 0x0400), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_5.one_pk_no_uk_1 VALUES (1, -55.55000000, N'db5-table1 migrated', 0x0005), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO test_db_5.one_pk_no_uk_2 VALUES (1, 55.55000000, N'db5-table2 ignored', 0x0500), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO other_test_db_1.one_pk_no_uk_1 VALUES (1, -66.66000000, N'other-table1 migrated', 0x0006), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO other_test_db_1.one_pk_no_uk_2 VALUES (1, 66.66000000, N'other-table2 ignored', 0x0600), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
GO
