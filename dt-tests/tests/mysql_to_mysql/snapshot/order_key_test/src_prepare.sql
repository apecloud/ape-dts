CREATE DATABASE IF NOT EXISTS order_key_src;

DROP TABLE IF EXISTS order_key_src.mixed;
CREATE TABLE order_key_src.mixed (a int NOT NULL, b int NOT NULL, c int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_mixed ON order_key_src.mixed (a ASC, b DESC, c ASC);

DROP TABLE IF EXISTS order_key_src.descending;
CREATE TABLE order_key_src.descending (id int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_descending ON order_key_src.descending (id DESC);

DROP TABLE IF EXISTS order_key_src.selected;
CREATE TABLE order_key_src.selected (id varchar(40) NOT NULL PRIMARY KEY, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_selected ON order_key_src.selected (a DESC);
CREATE UNIQUE INDEX z_selected ON order_key_src.selected (a ASC);

DROP TABLE IF EXISTS order_key_src.scored_key;
CREATE TABLE order_key_src.scored_key (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_scored ON order_key_src.scored_key (a DESC);
CREATE UNIQUE INDEX z_scored ON order_key_src.scored_key (a ASC);

DROP TABLE IF EXISTS order_key_src.nullable_key;
CREATE TABLE order_key_src.nullable_key (a int NULL, b int NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_nullable ON order_key_src.nullable_key (a DESC, b ASC);
CREATE UNIQUE INDEX uk_nullable_payload ON order_key_src.nullable_key (payload);

DROP TABLE IF EXISTS order_key_src.float_key;
CREATE TABLE order_key_src.float_key (a float NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_float ON order_key_src.float_key (a DESC);

DROP TABLE IF EXISTS order_key_src.catalog_key;
CREATE TABLE order_key_src.catalog_key (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, b int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_catalog ON order_key_src.catalog_key (a DESC, b ASC);
CREATE UNIQUE INDEX expression_key ON order_key_src.catalog_key (a, (b + 1));

DROP TABLE IF EXISTS order_key_src.cursor_ignored;
CREATE TABLE order_key_src.cursor_ignored (a int NOT NULL PRIMARY KEY, payload varchar(40) NOT NULL);

-- Keep this catalog example in sync with the meta manager's parse_keys comment.
DROP TABLE IF EXISTS order_key_src.parse_keys_example;
CREATE TABLE order_key_src.parse_keys_example (
    id int NOT NULL, value int NOT NULL,
    PRIMARY KEY some_pk_name (id DESC, value ASC),
    UNIQUE KEY some_uk_name (value DESC)
);
CREATE UNIQUE INDEX uk_example ON order_key_src.parse_keys_example (value ASC, id DESC);
CREATE INDEX non_unique_key ON order_key_src.parse_keys_example (id);
