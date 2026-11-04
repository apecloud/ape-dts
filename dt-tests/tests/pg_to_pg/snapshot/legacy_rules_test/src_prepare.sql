DROP SCHEMA IF EXISTS legacy_allowed CASCADE;
DROP SCHEMA IF EXISTS legacy_denied CASCADE;
DROP SCHEMA IF EXISTS legacy_selected CASCADE;
DROP SCHEMA IF EXISTS legacy_shard_a CASCADE;
DROP SCHEMA IF EXISTS legacy_shard_b CASCADE;

CREATE SCHEMA legacy_allowed;
CREATE SCHEMA legacy_denied;
CREATE SCHEMA legacy_selected;
CREATE SCHEMA legacy_shard_a;
CREATE SCHEMA legacy_shard_b;

CREATE TABLE legacy_allowed.kept (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_allowed.blocked (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_denied.rescued (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_selected.kept (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_selected.unselected (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_shard_a.merge_left (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_shard_b.merge_right (id INT PRIMARY KEY, note VARCHAR(80));
