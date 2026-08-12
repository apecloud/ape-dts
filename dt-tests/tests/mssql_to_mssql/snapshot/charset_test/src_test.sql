INSERT INTO charset_test.unicode_rows VALUES
    (1, 'ASCII', N'ASCII', 'fixed', N'fixed'),
    (2, N'简体中文', N'简体中文', N'中文', N'中文'),
    (3, N'繁體中文', N'繁體中文', N'繁體', N'繁體'),
    (4, N'日本語', N'日本語', N'日本', N'日本'),
    (5, N'대한민국', N'대한민국', N'한국', N'한국'),
    (6, N'emoji 😀 🚀', N'emoji 😀 🚀', N'😀', N'😀'),
    (7, N'quote '' and "', N'quote '' and "', N'quote', N'quote'),
    (8, '', N'', '', N''),
    (9, NULL, NULL, NULL, NULL),
    (10, 'trailing spaces   ', N'trailing spaces   ', 'tail   ', N'tail   ');
GO
