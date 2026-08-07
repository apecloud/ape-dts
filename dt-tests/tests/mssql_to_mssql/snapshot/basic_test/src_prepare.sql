IF OBJECT_ID(N'dbo.basic_test', N'U') IS NOT NULL
    DROP TABLE dbo.basic_test;
GO
CREATE TABLE dbo.basic_test (
    id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    enabled BIT NOT NULL,
    amount DECIMAL(18, 4) NOT NULL,
    name NVARCHAR(100) NOT NULL,
    note NVARCHAR(100) NULL,
    payload VARBINARY(32) NULL,
    event_date DATE NOT NULL,
    event_time DATETIME2(6) NOT NULL,
    external_id UNIQUEIDENTIFIER NOT NULL
);
GO
