
DROP TABLE IF EXISTS [order_key_dst].[dbo].[mixed];
CREATE TABLE [order_key_dst].[dbo].[mixed] (a int NOT NULL, b int NOT NULL, c int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_mixed ON [order_key_dst].[dbo].[mixed] (a ASC, b DESC, c ASC);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[descending];
CREATE TABLE [order_key_dst].[dbo].[descending] (id int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_descending ON [order_key_dst].[dbo].[descending] (id DESC);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[selected];
CREATE TABLE [order_key_dst].[dbo].[selected] (id varchar(40) NOT NULL PRIMARY KEY, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_selected ON [order_key_dst].[dbo].[selected] (a DESC);
CREATE UNIQUE INDEX z_selected ON [order_key_dst].[dbo].[selected] (a ASC);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[scored_key];
CREATE TABLE [order_key_dst].[dbo].[scored_key] (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_scored ON [order_key_dst].[dbo].[scored_key] (a DESC);
CREATE UNIQUE INDEX z_scored ON [order_key_dst].[dbo].[scored_key] (a ASC);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[nullable_key];
CREATE TABLE [order_key_dst].[dbo].[nullable_key] (a int NULL, b int NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_nullable ON [order_key_dst].[dbo].[nullable_key] (a DESC, b ASC);
CREATE UNIQUE INDEX uk_nullable_payload ON [order_key_dst].[dbo].[nullable_key] (payload);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[float_key];
CREATE TABLE [order_key_dst].[dbo].[float_key] (a float NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_float ON [order_key_dst].[dbo].[float_key] (a DESC);

DROP TABLE IF EXISTS [order_key_dst].[dbo].[catalog_key];
CREATE TABLE [order_key_dst].[dbo].[catalog_key] (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, b int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_catalog ON [order_key_dst].[dbo].[catalog_key] (a DESC, b ASC) INCLUDE (id);
CREATE UNIQUE INDEX partial_key ON [order_key_dst].[dbo].[catalog_key] (a) WHERE a > 99;
CREATE UNIQUE INDEX disabled_key ON [order_key_dst].[dbo].[catalog_key] (payload);
ALTER INDEX disabled_key ON [order_key_dst].[dbo].[catalog_key] DISABLE;

DROP TABLE IF EXISTS [order_key_dst].[dbo].cursor_ignored;
CREATE TABLE [order_key_dst].[dbo].cursor_ignored (a int NULL, payload varchar(40) NOT NULL);

-- Keep this catalog example in sync with the meta manager's parse_keys comment.
DROP TABLE IF EXISTS [order_key_dst].[dbo].parse_keys_example;
CREATE TABLE [order_key_dst].[dbo].parse_keys_example (
    id int NOT NULL, value int NOT NULL,
    CONSTRAINT some_pk_name PRIMARY KEY (id DESC, value ASC),
    CONSTRAINT some_uk_name UNIQUE (value DESC)
);
CREATE UNIQUE INDEX uk_example ON [order_key_dst].[dbo].parse_keys_example (value ASC, id DESC);
CREATE INDEX non_unique_key ON [order_key_dst].[dbo].parse_keys_example (id);
