INSERT INTO [special.* schema].[orders.* detail]
    ([order.id], [select], [value with space]) VALUES
    (1, N'first', 1.250),
    (2, N'第二', NULL),
    (3, N'quote '' value', -2.500);
GO
