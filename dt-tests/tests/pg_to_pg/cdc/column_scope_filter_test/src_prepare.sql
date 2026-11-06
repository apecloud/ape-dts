DROP SCHEMA IF EXISTS column_scope CASCADE;
CREATE SCHEMA column_scope;
CREATE TABLE column_scope.selected (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
CREATE TABLE column_scope.blocked (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
CREATE TABLE column_scope.sibling (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
CREATE TABLE column_scope.a_empty (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
