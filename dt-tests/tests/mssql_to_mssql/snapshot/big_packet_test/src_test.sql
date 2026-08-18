INSERT INTO big_packet_test.large_values VALUES
    (1, REPLICATE(CONVERT(nvarchar(max), N'x'), 5 * 1024 * 1024),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'a'), 10 * 1024 * 1024))),
    (2, REPLICATE(CONVERT(nvarchar(max), N'中'), 5 * 1024 * 1024),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'b'), 10 * 1024 * 1024))),
    (3, REPLICATE(CONVERT(nvarchar(max), N'わ'), 5 * 1024 * 1024),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'c'), 10 * 1024 * 1024))),
    (4, REPLICATE(CONVERT(nvarchar(max), N'한'), 5 * 1024 * 1024),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'd'), 10 * 1024 * 1024))),
    (5, REPLICATE(CONVERT(nvarchar(max), N'😀'), 5 * 1024 * 1024),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'e'), 10 * 1024 * 1024)));
GO
