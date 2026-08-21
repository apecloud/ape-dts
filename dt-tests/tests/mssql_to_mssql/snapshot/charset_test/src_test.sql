INSERT INTO [ape_dts].charset_test.ansi_rows VALUES
    (1, 'abc'), (2, 'Espana'), (3, 'francais'), (4, ''), (5, NULL);
INSERT INTO [ape_dts].charset_test.utf8_rows VALUES
    (1, 'abc'), (2, N'中文'), (3, N'わたし'), (4, N'대한민국'),
    (5, N'😀'), (6, N'quote '' and "'), (7, ''), (8, NULL);
INSERT INTO [ape_dts].charset_test.unicode_rows VALUES
    (1, N'abc'), (2, N'简体中文'), (3, N'繁體中文'), (4, N'日本語'),
    (5, N'대한민국'), (6, N'emoji 😀 🚀'), (7, N''), (8, NULL);
INSERT INTO [ape_dts].charset_test.fixed_utf8_rows VALUES
    (1, 'fixed'), (2, N'中文'), (3, N'日本'), (4, N'한국'), (5, N'😀'), (6, NULL);
INSERT INTO [ape_dts].charset_test.fixed_unicode_rows VALUES
    (1, N'fixed'), (2, N'中文'), (3, N'日本'), (4, N'한국'), (5, N'😀'), (6, NULL);
INSERT INTO [ape_dts].charset_test.case_sensitive_rows VALUES
    (1, N'Case'), (2, N'case'), (3, N'CASE'), (4, N'Accent'),
    (5, N'accent'), (6, N'中文'), (7, NULL);
INSERT INTO [ape_dts].charset_test.binary_collation_rows VALUES
    (1, N'A'), (2, N'a'), (3, N'Á'), (4, N'á'),
    (5, N'中'), (6, N'😀'), (7, N''), (8, NULL);
INSERT INTO [ape_dts].charset_test.max_text_rows VALUES
    (1, REPLICATE(CONVERT(nvarchar(max), N'中文-日本語-대한민국-😀-'), 1024)),
    (2, N'line 1' + NCHAR(10) + N'line 2' + NCHAR(9) + N'tab'),
    (3, N''), (4, NULL);
INSERT INTO [ape_dts].charset_test.composite_text_rows VALUES
    ('en-US', N'Hello', N'English'), ('en-US', N'hello', N'case distinct'),
    ('zh-CN', N'中文', N'简体'), ('zh-TW', N'中文', N'繁體'),
    ('ja-JP', N'日本語', N'Japanese'), ('ko-KR', N'한국어', N'Korean'),
    ('emoji', N'😀', NULL);
GO
