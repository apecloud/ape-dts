DROP SCHEMA IF EXISTS "test_db_*.*" CASCADE;

CREATE SCHEMA "test_db_*.*";

DROP TABLE IF EXISTS resume_table_1;
CREATE TABLE resume_table_1(pk serial, val numeric(20,8), PRIMARY KEY(pk));

DROP TABLE IF EXISTS resume_table_2;
CREATE TABLE resume_table_2("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS resume_table_3;
CREATE TABLE resume_table_3(f_0 integer, f_1 integer, PRIMARY KEY(f_0, f_1));

DROP TABLE IF EXISTS "resume_table_*$4";
CREATE TABLE "resume_table_*$4"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

-- test nullable composite unique key
DROP TABLE IF EXISTS nullable_composite_unique_key_table;
CREATE TABLE nullable_composite_unique_key_table (uk1 int, uk2 varchar(10), val int, UNIQUE(uk1, uk2));

DROP TABLE IF EXISTS "test_db_*.*"."resume_table_*$5";
CREATE TABLE "test_db_*.*"."resume_table_*$5"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."finished_table_*$1";
CREATE TABLE "test_db_*.*"."finished_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."finished_table_*$2";
CREATE TABLE "test_db_*.*"."finished_table_*$2"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."in_finished_log_table_*$1";
CREATE TABLE "test_db_*.*"."in_finished_log_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."in_finished_log_table_*$2";
CREATE TABLE "test_db_*.*"."in_finished_log_table_*$2"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."in_position_log_table_*$1";
CREATE TABLE "test_db_*.*"."in_position_log_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));