INSERT INTO dbo.basic_test (
    enabled, amount, name, note, payload, event_date, event_time, external_id
) VALUES
    (1, 12.3400, N'basic', NULL, 0x000102FEFF, '2024-01-02', '2024-01-02T03:04:05.123456', '11111111-1111-1111-1111-111111111111'),
    (0, -98.7654, N'Unicode 测试', N'quote '' value', 0xCAFE, '2024-02-29', '2024-02-29T23:59:59.999999', '22222222-2222-2222-2222-222222222222'),
    (1, 0.0000, N'empty optionals', NULL, NULL, '2000-01-01', '2000-01-01T00:00:00.000000', '33333333-3333-3333-3333-333333333333');
GO
