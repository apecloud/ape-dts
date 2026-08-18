DROP TABLE IF EXISTS charset_test.ansi_rows;
DROP TABLE IF EXISTS charset_test.utf8_rows;
DROP TABLE IF EXISTS charset_test.unicode_rows;
DROP TABLE IF EXISTS charset_test.fixed_utf8_rows;
DROP TABLE IF EXISTS charset_test.fixed_unicode_rows;
DROP TABLE IF EXISTS charset_test.case_sensitive_rows;
DROP TABLE IF EXISTS charset_test.binary_collation_rows;
DROP TABLE IF EXISTS charset_test.max_text_rows;
DROP TABLE IF EXISTS charset_test.composite_text_rows;
IF SCHEMA_ID(N'charset_test') IS NULL EXEC(N'CREATE SCHEMA charset_test');
CREATE TABLE charset_test.ansi_rows (
    id int NOT NULL PRIMARY KEY, value varchar(300) COLLATE Latin1_General_100_CI_AS NULL
);
CREATE TABLE charset_test.utf8_rows (
    id int NOT NULL PRIMARY KEY,
    value varchar(300) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NULL
);
CREATE TABLE charset_test.unicode_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(300) COLLATE Latin1_General_100_CI_AS_SC NULL
);
CREATE TABLE charset_test.fixed_utf8_rows (
    id int NOT NULL PRIMARY KEY,
    value char(40) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NULL
);
CREATE TABLE charset_test.fixed_unicode_rows (
    id int NOT NULL PRIMARY KEY,
    value nchar(20) COLLATE Latin1_General_100_CI_AS_SC NULL
);
CREATE TABLE charset_test.case_sensitive_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(300) COLLATE Latin1_General_100_CS_AS_SC NULL
);
CREATE TABLE charset_test.binary_collation_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(300) COLLATE Latin1_General_100_BIN2 NULL
);
CREATE TABLE charset_test.max_text_rows (
    id int NOT NULL PRIMARY KEY,
    value nvarchar(max) COLLATE Latin1_General_100_CI_AS_SC NULL
);
CREATE TABLE charset_test.composite_text_rows (
    locale varchar(16) COLLATE Latin1_General_100_CS_AS_SC_UTF8 NOT NULL,
    code nvarchar(30) COLLATE Latin1_General_100_CS_AS_SC NOT NULL,
    value nvarchar(300) NULL,
    PRIMARY KEY (locale, code)
);
GO
