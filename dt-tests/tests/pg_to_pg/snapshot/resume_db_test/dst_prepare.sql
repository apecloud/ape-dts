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

DROP TABLE IF EXISTS "test_db_*.*"."resume_table_*$5";
CREATE TABLE "test_db_*.*"."resume_table_*$5"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."finished_table_*$1";
CREATE TABLE "test_db_*.*"."finished_table_*$1"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP TABLE IF EXISTS "test_db_*.*"."finished_table_*$2";
CREATE TABLE "test_db_*.*"."finished_table_*$2"("p.k" serial, val numeric(20,8), PRIMARY KEY("p.k"));

DROP SCHEMA IF EXISTS apecloud_resumer_test CASCADE;
CREATE SCHEMA apecloud_resumer_test;

CREATE TABLE IF NOT EXISTS apecloud_resumer_test.ape_task_position (
  id bigserial PRIMARY KEY,
  task_id varchar(255) NOT NULL,
  resumer_type varchar(100) NOT NULL,
  position_key varchar(255) NOT NULL,
  position_data text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_task_id_task_type_position_key UNIQUE (task_id, resumer_type, position_key)
);

insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotFinished', 'test_db_*.*-finished_table_*$1', '{"type":"RdbSnapshotFinished","db_type":"pg","schema":"test_db_*.*","tb":"finished_table_*$1"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotFinished', 'test_db_*.*-finished_table_*$2', '{"type":"RdbSnapshotFinished","db_type":"pg","schema":"test_db_*.*","tb":"finished_table_*$2"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'public-resume_table_1-pk', '{"type":"RdbSnapshot","db_type":"pg","schema":"public","tb":"resume_table_1","order_col":"pk","value":"1"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'public-resume_table_2-p.k', '{"type":"RdbSnapshot","db_type":"pg","schema":"public","tb":"resume_table_2","order_col":"p.k","value":"1"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'public-resume_table_3-f_0', '{"type":"RdbSnapshot","db_type":"pg","schema":"public","tb":"resume_table_3","order_col":"f_0","value":"1"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'public-resume_table_*$4-p.k', '{"type":"RdbSnapshot","db_type":"pg","schema":"public","tb":"resume_table_*$4","order_col":"p.k","value":"1"}');
insert into apecloud_resumer_test.ape_task_position (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_*.*-resume_table_*$5-p.k', '{"type":"RdbSnapshot","db_type":"pg","schema":"test_db_*.*","tb":"resume_table_*$5","order_col":"p.k","value":"1"}');
