CREATE DATABASE IF NOT EXISTS order_key_dst;

DROP TABLE IF EXISTS order_key_dst.mixed;
CREATE TABLE order_key_dst.mixed (a int NOT NULL, b int NOT NULL, c int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_mixed ON order_key_dst.mixed (a ASC, b DESC, c ASC);

DROP TABLE IF EXISTS order_key_dst.descending;
CREATE TABLE order_key_dst.descending (id int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_descending ON order_key_dst.descending (id DESC);

DROP TABLE IF EXISTS order_key_dst.selected;
CREATE TABLE order_key_dst.selected (id varchar(40) NOT NULL PRIMARY KEY, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_selected ON order_key_dst.selected (a DESC);
CREATE UNIQUE INDEX z_selected ON order_key_dst.selected (a ASC);

DROP TABLE IF EXISTS order_key_dst.scored_key;
CREATE TABLE order_key_dst.scored_key (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX a_scored ON order_key_dst.scored_key (a DESC);
CREATE UNIQUE INDEX z_scored ON order_key_dst.scored_key (a ASC);

DROP TABLE IF EXISTS order_key_dst.nullable_key;
CREATE TABLE order_key_dst.nullable_key (a int NULL, b int NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_nullable ON order_key_dst.nullable_key (a DESC, b ASC);
CREATE UNIQUE INDEX uk_nullable_payload ON order_key_dst.nullable_key (payload);

DROP TABLE IF EXISTS order_key_dst.float_key;
CREATE TABLE order_key_dst.float_key (a float NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_float ON order_key_dst.float_key (a DESC);

DROP TABLE IF EXISTS order_key_dst.catalog_key;
CREATE TABLE order_key_dst.catalog_key (id varchar(40) NOT NULL UNIQUE, a int NOT NULL, b int NOT NULL, payload varchar(40) NOT NULL);
CREATE UNIQUE INDEX uk_catalog ON order_key_dst.catalog_key (a DESC, b ASC);
CREATE UNIQUE INDEX expression_key ON order_key_dst.catalog_key (a, (b + 1));

DROP TABLE IF EXISTS order_key_dst.cursor_ignored;
CREATE TABLE order_key_dst.cursor_ignored (a int NULL, payload varchar(40) NOT NULL);

-- Keep this catalog example in sync with the meta manager's parse_keys comment.
DROP TABLE IF EXISTS order_key_dst.parse_keys_example;
CREATE TABLE order_key_dst.parse_keys_example (
    id int NOT NULL, value int NOT NULL,
    PRIMARY KEY some_pk_name (id DESC, value ASC),
    UNIQUE KEY some_uk_name (value DESC)
);
CREATE UNIQUE INDEX uk_example ON order_key_dst.parse_keys_example (value ASC, id DESC);
CREATE INDEX non_unique_key ON order_key_dst.parse_keys_example (id);
