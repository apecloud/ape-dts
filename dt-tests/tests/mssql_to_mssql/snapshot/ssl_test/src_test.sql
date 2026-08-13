INSERT INTO ssl_test.ssl_test_1 VALUES
    (1, -32768), (2, -1), (3, 0), (4, 1), (5, 32767), (6, 123), (7, NULL);
INSERT INTO ssl_test.ssl_test_2 VALUES
    (1, -2147483648), (2, -1), (3, 0), (4, 1), (5, 2147483647), (6, 123456), (7, NULL);
INSERT INTO ssl_test.ssl_test_3 VALUES
    (1, -9223372036854775808), (2, -1), (3, 0), (4, 1),
    (5, 9223372036854775807), (6, 1234567890123), (7, NULL);
INSERT INTO ssl_test.ssl_test_4 VALUES
    (1, -9999999999999999.9999), (2, -1.2500), (3, 0), (4, 1.2500),
    (5, 9999999999999999.9999), (6, 123456.7890), (7, NULL);
INSERT INTO ssl_test.ssl_test_5 VALUES
    (1, -123.25), (2, -1), (3, 0), (4, 1), (5, 123.25), (6, 3.14), (7, NULL);
INSERT INTO ssl_test.ssl_test_6 VALUES
    (1, -1.5E100), (2, -1), (3, 0), (4, 1), (5, 1.5E100), (6, 4.44), (7, NULL);
INSERT INTO ssl_test.ssl_test_7 VALUES
    (1, 0), (2, 1), (3, 0), (4, 1), (5, 0), (6, 1), (7, NULL);
INSERT INTO ssl_test.ssl_test_8 VALUES
    (1, N'encrypted', 0x0102), (2, N'加密连接', 0xCAFE),
    (3, N'日本語', 0x00FF), (4, N'대한민국', 0xFFFFFFFF),
    (5, N'emoji 😀', 0xE4BDA0E5A5BD), (6, N'', 0x), (7, NULL, NULL);
GO
