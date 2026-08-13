INSERT INTO [test_db_*.*].[one_pk_no_uk_1_*.*] VALUES (1, -32768, N'star one 中文', 0x00FF, -123456789012.3456), (2, NULL, NULL, NULL, NULL);
INSERT INTO [test_db_*.*].[one_pk_no_uk_2_*.*] VALUES (1, 32767, N'star two 😀', 0xFF00, 123456789012.3456), (2, 0, N'', 0x, 0);
INSERT INTO [test_db_&.&].[one_pk_no_uk_1_&.&] VALUES (1, -1, N'amp one 日本語', 0x0102, -1.2500), (2, NULL, NULL, NULL, NULL);
INSERT INTO [test_db_&.&].[one_pk_no_uk_2_&.&] VALUES (1, 1, N'amp two 대한민국', 0xCAFE, 1.2500), (2, 0, N'', 0x, 0);
INSERT INTO [test_db_^.^].[one_pk_no_uk_1_^.^] VALUES (1, -12, N'caret one 中文', 0x0011, -12.3400), (2, NULL, NULL, NULL, NULL);
INSERT INTO [test_db_^.^].[one_pk_no_uk_2_^.^] VALUES (1, 12, N'caret two 😀', 0x1100, 12.3400), (2, 0, N'', 0x, 0);
INSERT INTO [test_db_@.@].[one_pk_no_uk_1_@.@] VALUES (1, -34, N'at one 日本語', 0x0022, -34.5600), (2, NULL, NULL, NULL, NULL);
INSERT INTO [test_db_@.@].[one_pk_no_uk_2_@.@] VALUES (1, 34, N'at two 대한민국', 0x2200, 34.5600), (2, 0, N'', 0x, 0);
INSERT INTO [*.*_test_db].[one_pk_no_uk_1_*.*] VALUES (1, -56, N'leading star 中文', 0x0033, -56.7800), (2, NULL, NULL, NULL, NULL);
INSERT INTO [*.*_test_db].[one_pk_no_uk_2_*.*] VALUES (1, 56, N'leading star 😀', 0x3300, 56.7800), (2, 0, N'', 0x, 0);
INSERT INTO [&.&_test_db].[one_pk_no_uk_1_&.&] VALUES (1, -78, N'leading amp 日本語', 0x0044, -78.9000), (2, NULL, NULL, NULL, NULL);
INSERT INTO [&.&_test_db].[one_pk_no_uk_2_&.&] VALUES (1, 78, N'leading amp 대한민국', 0x4400, 78.9000), (2, 0, N'', 0x, 0);
INSERT INTO [^.^_test_db].[one_pk_no_uk_1_^.^] VALUES (1, -90, N'leading caret 中文', 0x0055, -90.1200), (2, NULL, NULL, NULL, NULL);
INSERT INTO [^.^_test_db].[one_pk_no_uk_2_^.^] VALUES (1, 90, N'leading caret 😀', 0x5500, 90.1200), (2, 0, N'', 0x, 0);
INSERT INTO [@.@_test_db].[one_pk_no_uk_1_@.@] VALUES (1, -123, N'leading at 日本語', 0x0066, -123.4500), (2, NULL, NULL, NULL, NULL);
INSERT INTO [@.@_test_db].[one_pk_no_uk_2_@.@] VALUES (1, 123, N'leading at 대한민국', 0x6600, 123.4500), (2, 0, N'', 0x, 0);
GO
