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

DROP SCHEMA IF EXISTS apecloud_resumer_test CASCADE;
CREATE SCHEMA apecloud_resumer_test;

CREATE TABLE IF NOT EXISTS apecloud_resumer_test.ape_task_position (
    id bigserial PRIMARY KEY,
    task_id varchar(255) NOT NULL,
    resumer_type varchar(255) NOT NULL,
    position_key varchar(255) NOT NULL,
    position_data text,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (task_id, resumer_type, position_key)
);

INSERT INTO apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) VALUES
('resume_db_test_1', 'SnapshotFinished', 'test_db_@-finished_table_*$1', '{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$1"}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_no_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_no_uk","order_key":{"single":["f_0","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-one_pk_no_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_key":{"single":["f_0","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_one_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_one_uk","order_key":{"single":["f_1","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_multi_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_multi_uk","order_key":{"single":["f_1","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-one_pk_multi_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_multi_uk","order_key":{"single":["f_0","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-multi_pk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"multi_pk","order_key":{"single":["f_0","5"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-nullable_composite_unique_key_table', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"nullable_composite_unique_key_table","order_key":{"single":["uk1","6"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_1-varchar_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"varchar_uk","order_key":{"single":["id","6"]}}'),
('resume_db_test_1', 'SnapshotDoing', 'test_db_@-resume_table_*$4', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"resume_table_*$4","order_key":{"single":["p.k","5"]}}');