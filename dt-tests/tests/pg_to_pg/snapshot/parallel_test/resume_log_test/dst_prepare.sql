DROP SCHEMA IF EXISTS test_db_1 CASCADE;

CREATE SCHEMA test_db_1;

DROP SCHEMA IF EXISTS "test_db_@" CASCADE;

CREATE SCHEMA "test_db_@";

CREATE TABLE test_db_1.no_pk_no_uk ( f_0 smallint DEFAULT NULL, f_1 smallint DEFAULT NULL, f_2 int DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 real DEFAULT NULL, f_7 double precision DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 timestamp(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 smallint DEFAULT NULL, f_13 timestamp(6) DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 bytea DEFAULT NULL, f_17 bytea DEFAULT NULL, f_18 text, f_19 text, f_20 text, f_21 text, f_22 bytea, f_23 bytea, f_24 bytea, f_25 bytea, f_26 varchar(20) DEFAULT NULL, f_27 varchar(20) DEFAULT NULL, f_28 jsonb DEFAULT NULL); 

CREATE TABLE test_db_1.one_pk_no_uk ( f_0 smallint, f_1 smallint DEFAULT NULL, f_2 int DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 real DEFAULT NULL, f_7 double precision DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 timestamp(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 smallint DEFAULT NULL, f_13 timestamp(6) DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 bytea DEFAULT NULL, f_17 bytea DEFAULT NULL, f_18 text, f_19 text, f_20 text, f_21 text, f_22 bytea, f_23 bytea, f_24 bytea, f_25 bytea, f_26 varchar(20) DEFAULT NULL, f_27 varchar(20) DEFAULT NULL, f_28 jsonb DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE test_db_1.no_pk_one_uk ( f_0 smallint DEFAULT NULL, f_1 smallint, f_2 int, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 real DEFAULT NULL, f_7 double precision DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 timestamp(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 smallint DEFAULT NULL, f_13 timestamp(6) DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 bytea DEFAULT NULL, f_17 bytea DEFAULT NULL, f_18 text, f_19 text, f_20 text, f_21 text, f_22 bytea, f_23 bytea, f_24 bytea, f_25 bytea, f_26 varchar(20) DEFAULT NULL, f_27 varchar(20) DEFAULT NULL, f_28 jsonb DEFAULT NULL, UNIQUE (f_1,f_2) ); 

CREATE TABLE test_db_1.no_pk_multi_uk ( f_0 smallint DEFAULT NULL, f_1 smallint, f_2 int, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 real, f_7 double precision, f_8 bit(64), f_9 timestamp(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 smallint DEFAULT NULL, f_13 timestamp(6) DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 bytea DEFAULT NULL, f_17 bytea DEFAULT NULL, f_18 text, f_19 text, f_20 text, f_21 text, f_22 bytea, f_23 bytea, f_24 bytea, f_25 bytea, f_26 varchar(20) DEFAULT NULL, f_27 varchar(20) DEFAULT NULL, f_28 jsonb DEFAULT NULL, UNIQUE (f_1,f_2), UNIQUE (f_3,f_4,f_5), UNIQUE (f_6,f_7,f_8) ); 

CREATE TABLE test_db_1.one_pk_multi_uk ( f_0 smallint, f_1 smallint, f_2 int, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 real, f_7 double precision, f_8 bit(64), f_9 timestamp(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 smallint DEFAULT NULL, f_13 timestamp(6) DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 bytea DEFAULT NULL, f_17 bytea DEFAULT NULL, f_18 text, f_19 text, f_20 text, f_21 text, f_22 bytea, f_23 bytea, f_24 bytea, f_25 bytea, f_26 varchar(20) DEFAULT NULL, f_27 varchar(20) DEFAULT NULL, f_28 jsonb DEFAULT NULL, PRIMARY KEY (f_0), UNIQUE (f_1,f_2), UNIQUE (f_3,f_4,f_5), UNIQUE (f_6,f_7,f_8) );

CREATE TABLE test_db_1.multi_pk(f_0 integer, f_1 integer, PRIMARY KEY(f_0, f_1));

-- test nullable composite unique key
CREATE TABLE test_db_1.nullable_composite_unique_key_table (uk1 int, uk2 varchar(10), val int, UNIQUE(uk1, uk2));

CREATE TABLE test_db_1.varchar_uk ( "id" varchar(64) DEFAULT NULL, "value" int DEFAULT NULL, UNIQUE ("id"));

CREATE TABLE "test_db_@"."resume_table_*$4"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

CREATE TABLE "test_db_@"."finished_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

CREATE TABLE "test_db_@"."in_finished_log_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

CREATE TABLE "test_db_@"."in_position_log_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));