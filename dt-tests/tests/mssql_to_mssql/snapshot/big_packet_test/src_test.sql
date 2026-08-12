INSERT INTO big_packet_test.large_values VALUES
    (1, REPLICATE(CONVERT(nvarchar(max), N'中文-data-'), 32768),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), '0123456789ABCDEF'), 32768))),
    (2, REPLICATE(CONVERT(nvarchar(max), N'x'), 524288),
        CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'z'), 524288)));
GO
