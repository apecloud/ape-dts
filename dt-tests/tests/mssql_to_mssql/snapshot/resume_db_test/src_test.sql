INSERT INTO [ape_dts].resume_test.resume_rows VALUES
    (1, N'one'), (2, N'two'), (3, N'three'), (4, N'中文'), (5, NULL);
INSERT INTO [ape_dts].resume_test.composite_rows VALUES
    (1, 1, N'one-one'), (1, 2, N'one-two'), (2, 1, N'two-one'), (2, 2, NULL);
INSERT INTO [ape_dts].resume_test.binary_key_rows VALUES
    (0x00FF, N'binary-one'), (0x0102, N'binary-two'),
    (0xE4BDA0E5A5BD, N'UTF-8 bytes'), (0xFFFFFFFF, NULL);
INSERT INTO [ape_dts].resume_test.[resume table.*] VALUES
    (1, N'special-one'), (2, N'特殊値'), (3, NULL);
INSERT INTO [ape_dts].resume_test.nullable_composite_unique_rows VALUES
    (1, 1, N'a', N'one-a'), (2, 1, N'b', N'one-b'),
    (3, NULL, N'a', N'null-a'), (4, NULL, N'a', N'null-a-duplicate'),
    (5, 2, NULL, N'two-null'), (6, NULL, NULL, NULL);
INSERT INTO [ape_dts].resume_test.string_key_rows VALUES
    (N'a', 0x01), (N'b', 0x02), (N'c', 0x03),
    (N'd', 0xE4BDA0E5A5BD), (N'z', NULL);
INSERT INTO [ape_dts].resume_test.fresh_rows VALUES
    (-2147483648, N'fresh-min'), (0, N''), (2147483647, NULL);
INSERT INTO [ape_dts].resume_test.finished_rows VALUES (1, N'skipped-one'), (2, N'skipped-two');
INSERT INTO [ape_dts].resume_test.finished_rows_2 VALUES (1, N'skipped-three'), (2, N'skipped-four');
GO
