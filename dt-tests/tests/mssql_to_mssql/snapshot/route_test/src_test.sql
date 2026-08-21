INSERT INTO [ape_dts].route_src_schema.schema_orders VALUES
    (1, N'schema-one 中文', -123456789012.3456, N'not copied'),
    (2, N'', 0, NULL), (3, NULL, NULL, N'also ignored');
INSERT INTO [ape_dts].route_src_schema.schema_payloads VALUES
    (1, 0x000102FF, '0001-01-01T00:00:00'),
    (2, 0x, '9999-12-31T23:59:59.9999999'), (3, NULL, NULL);
INSERT INTO [ape_dts].route_table_schema.source_orders VALUES
    (-9223372036854775808, N'table-min 日本語', 0x00),
    (0, N'', 0x), (9223372036854775807, NULL, NULL);
INSERT INTO [ape_dts].route_table_schema.source_metrics VALUES
    ('00000000-0000-0000-0000-000000000000', -1.5E100, '2026-08-12T12:34:56.1234567+08:00'),
    ('550e8400-e29b-41d4-a716-446655440000', 0, '2026-08-12T04:34:56.1234567+00:00'),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', NULL, NULL);
INSERT INTO [ape_dts].route_col_schema.source_users VALUES
    (1, N'user-one 대한민국', '11111111-1111-1111-1111-111111111111', 0xCAFE),
    (2, N'', '00000000-0000-0000-0000-000000000000', 0x), (3, NULL, NULL, NULL);
INSERT INTO [ape_dts].route_col_schema.source_events VALUES
    (-9223372036854775808, N'event-min 😀', '0001-01-01T00:00:00', -9999999999999999.9999),
    (0, N'', '2024-02-29T23:59:59.9999999', 0),
    (9223372036854775807, NULL, NULL, NULL);
INSERT INTO [route_src_db].dbo.database_orders VALUES
    (1, N'database route 中文'), (2, NULL);
INSERT INTO [route.source.*].[schema.with.*].[database orders.*] VALUES
    (1, N'special database route 日本語'), (2, N''), (3, NULL);
GO
