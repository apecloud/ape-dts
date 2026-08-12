DROP TABLE IF EXISTS charset_test.unicode_rows;
IF SCHEMA_ID(N'charset_test') IS NULL EXEC(N'CREATE SCHEMA charset_test');
CREATE TABLE charset_test.unicode_rows (
    id int NOT NULL PRIMARY KEY,
    utf8_value varchar(300) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NULL,
    unicode_value nvarchar(300) COLLATE Latin1_General_100_CI_AS_SC NULL,
    fixed_utf8 char(20) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NULL,
    fixed_unicode nchar(20) COLLATE Latin1_General_100_CI_AS_SC NULL
);
GO
