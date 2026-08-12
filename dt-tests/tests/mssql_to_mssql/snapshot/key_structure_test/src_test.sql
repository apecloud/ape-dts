INSERT INTO key_structure.no_key_rows VALUES
    (1, N'one'), (NULL, N'null-key'), (2, NULL), (NULL, NULL), (1, N'one');
INSERT INTO key_structure.composite_primary_rows VALUES
    (1, 1, N'a'), (1, 2, NULL), (2, 1, N'b'), (2, 2, N'中文');
INSERT INTO key_structure.unique_rows VALUES
    (NULL, '11111111-1111-1111-1111-111111111111', N'alt-1', 10),
    (2, '22222222-2222-2222-2222-222222222222', N'alt-2', NULL),
    (3, '33333333-3333-3333-3333-333333333333', N'alt-3', -30);
INSERT INTO key_structure.default_rows (id, nullable_value) VALUES
    (1, NULL), (2, N'explicit nullable');
INSERT INTO key_structure.default_rows VALUES
    (3, N'explicit text', 0, N'value');
GO
