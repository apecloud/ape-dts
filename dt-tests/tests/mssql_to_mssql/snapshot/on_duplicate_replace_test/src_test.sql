INSERT INTO on_duplicate_replace.conflict_rows VALUES
    (1, N'source-conflict'), (2, N'source-new');
SET IDENTITY_INSERT on_duplicate_replace.unique_rows ON;
INSERT INTO on_duplicate_replace.unique_rows (id, code, value) VALUES
    (1, N'code-1', N'source-conflict'), (2, N'code-2', N'source-new');
SET IDENTITY_INSERT on_duplicate_replace.unique_rows OFF;
INSERT INTO on_duplicate_replace.nullable_unique_rows VALUES
    (1, NULL, N'source-null'), (2, N'code-2', N'source-new');
INSERT INTO on_duplicate_replace.key_only_rows VALUES (1), (2);
GO
