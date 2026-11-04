INSERT INTO [ape_dts].test_db_1.one_pk_no_uk_1 VALUES (1, -123456789012.12345678, N'db1-table1 中文', 0x0001), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_1.one_pk_no_uk_2 VALUES (1, 123456789012.12345678, N'db1-table2 😀', 0x0100), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_2.one_pk_no_uk_1 VALUES (1, -22.22000000, N'db2-table1 日本語', 0x0002), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_2.one_pk_no_uk_2 VALUES (1, 22.22000000, N'db2-table2 대한민국', 0x0200), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_3.one_pk_no_uk_1 VALUES (1, -33.33000000, N'db3-table1 ignored schema', 0x0003), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_3.one_pk_no_uk_2 VALUES (1, 33.33000000, N'db3-table2 ignored schema', 0x0300), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_4.one_pk_no_uk_1 VALUES (1, -44.44000000, N'db4-table1 ignored table', 0x0004), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_4.one_pk_no_uk_2 VALUES (1, 44.44000000, N'db4-table2 ignored table', 0x0400), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_5.one_pk_no_uk_1 VALUES (1, -55.55000000, N'db5-table1 migrated', 0x0005), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].test_db_5.one_pk_no_uk_2 VALUES (1, 55.55000000, N'db5-table2 ignored', 0x0500), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].other_test_db_1.one_pk_no_uk_1 VALUES (1, -66.66000000, N'other-table1 migrated', 0x0006), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].other_test_db_1.one_pk_no_uk_2 VALUES (1, 66.66000000, N'other-table2 ignored', 0x0600), (2, 0, N'', 0x), (3, NULL, NULL, NULL);
INSERT INTO [filter_db_keep].dbo.db_level_kept VALUES (1, N'database filter kept 中文'), (2, NULL);
INSERT INTO [filter_db_skip].dbo.db_level_filtered VALUES (1, N'database filter ignored');
INSERT INTO [ape_dts].filter_schema_keep.schema_level_kept VALUES (1, N'schema filter kept 日本語'), (2, NULL);
INSERT INTO [ape_dts].filter_schema_skip.schema_level_filtered VALUES (1, N'schema filter ignored');
GO

-- Keep a final row so unexpected replication cannot pass as empty data.
INSERT INTO [ape_dts].[wildchar_filter].[star] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[star_many] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[prestar] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[question] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[question1] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[question12] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[regex_1] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[regex_12] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[regex_x] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[regex_12tail] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[exact_name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[exact_name_tail] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[plus+name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[plusname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[paren(name)] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[parenname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[bracket[12]]] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[bracket1] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[brace{2}] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[bracee] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[caret^name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[caretname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[dollar$name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[dollarname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[pipe|name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[pipe_tail] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[quoted*name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[quotedXname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[quoted?name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[quotedYname] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_star] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_star_many] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skipXstar] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_question] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_question1] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_question12] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_regex_1] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip_regex_x] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skip+name] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[skipname] VALUES (1, 10);
GO

INSERT INTO [ape_dts].[wildchar_filter].[plus+star] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[plus+star_many] VALUES (1, 10);
INSERT INTO [ape_dts].[wildchar_filter].[plusstar] VALUES (1, 10);
GO
