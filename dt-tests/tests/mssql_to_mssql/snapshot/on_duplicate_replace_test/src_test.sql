USE [ape_dts];
GO
INSERT INTO [ape_dts].on_duplicate_replace.conflict_rows VALUES
    (1, N'source-conflict'), (2, N'source-new'), (3, N''),
    (4, N'source-primary-conflict'), (5, N'中文'), (6, N'symbols !@#');
-- SET IDENTITY_INSERT [ape_dts].on_duplicate_replace.unique_rows ON;
INSERT INTO [ape_dts].on_duplicate_replace.unique_rows (id, code, value) VALUES
    (1, N'code-1', N'source-unique-conflict'),
    (2, N'code-2', N'source-new'),
    (3, N'code-3', N''),
    (4, N'code-4', N'id-is-not-a-key'),
    (5, N'中文-code', N'中文'),
    (6, N'symbols-!@#', N'symbols');
-- SET IDENTITY_INSERT [ape_dts].on_duplicate_replace.unique_rows OFF;
INSERT INTO [ape_dts].on_duplicate_replace.nullable_unique_rows VALUES
    (1, NULL, N'source-null-conflict'),
    (2, N'code-2', N'source-new'),
    (3, N'code-3', N''),
    (4, N'code-4', N'id-is-not-a-key'),
    (5, N'中文-code', N'中文'),
    (6, N'symbols-!@#', N'symbols');
INSERT INTO [ape_dts].on_duplicate_replace.key_only_rows VALUES (1), (2), (3), (4), (5), (6);
INSERT INTO [ape_dts].on_duplicate_replace.primary_and_unique_rows VALUES
    (1, N'code-1', N'source-unique-conflict'),
    (2, N'code-2', N'source-new'),
    (3, N'code-3', NULL),
    (4, N'code-4', N'source-primary-conflict'),
    (5, N'中文-code', N'中文'),
    (6, N'symbols-!@#', N'symbols');
INSERT INTO [ape_dts].on_duplicate_replace.udt_rows VALUES
(
    1,
    geometry::STGeomFromText(N'POINT (1.25 -2.5 3.5 4.5)', 4326),
    geography::STGeomFromText(N'POINT (-122.360 47.656 3.5 4.5)', 4326),
    hierarchyid::Parse(N'/1/2/'),
    dbo.BigVariantFromString(N'source-conflict 中文')
),
(
    2,
    geometry::STGeomFromText(N'LINESTRING (0 0, 3 4, -5 6)', 0),
    geography::STGeomFromText(N'LINESTRING (-122.360 47.656, -122.343 47.656)', 4326),
    hierarchyid::Parse(N'/1/2.5/3/'),
    dbo.BigVariantFromBinary(0x000102030405FEFF)
),
(3, NULL, NULL, NULL, NULL);
GO
