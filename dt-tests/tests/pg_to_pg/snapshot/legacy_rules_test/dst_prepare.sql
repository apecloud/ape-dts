DROP SCHEMA IF EXISTS legacy_allowed CASCADE;
DROP SCHEMA IF EXISTS legacy_denied CASCADE;
DROP SCHEMA IF EXISTS legacy_selected CASCADE;
DROP SCHEMA IF EXISTS legacy_merged CASCADE;

CREATE SCHEMA legacy_allowed;
CREATE SCHEMA legacy_denied;
CREATE SCHEMA legacy_selected;
CREATE SCHEMA legacy_merged;

CREATE TABLE legacy_allowed.kept (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_allowed.blocked (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_denied.rescued (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_selected.kept (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_selected.unselected (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_merged.merge_left (id INT PRIMARY KEY, note VARCHAR(80));
CREATE TABLE legacy_merged.merge_right (id INT PRIMARY KEY, note VARCHAR(80));
