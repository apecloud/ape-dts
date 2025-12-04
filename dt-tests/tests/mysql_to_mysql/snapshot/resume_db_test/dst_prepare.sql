DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

DROP DATABASE IF EXISTS `test_db_@`;

CREATE DATABASE `test_db_@`;

CREATE TABLE test_db_1.no_pk_no_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_no_uk ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_one_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_multi_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_multi_uk ( f_0 tinyint, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0), UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE test_db_1.multi_pk(f_0 integer, f_1 integer, PRIMARY KEY(f_0, f_1));

-- test nullable composite unique key
CREATE TABLE test_db_1.nullable_composite_unique_key_table (uk1 int, uk2 varchar(10), val int, UNIQUE(uk1, uk2));

CREATE TABLE `test_db_@`.`resume_table_*$4`(`p.k` serial, val numeric(20,8), PRIMARY KEY(`p.k`));

CREATE TABLE `test_db_@`.`finished_table_*$1`(`p.k` serial, val numeric(20,8), PRIMARY KEY(`p.k`));

CREATE TABLE `test_db_@`.`finished_table_*$2`(`p.k` serial, val numeric(20,8), PRIMARY KEY(`p.k`));

```
CREATE TABLE test_db_1.bytea_pk_gb2312_test (
    category_id VARCHAR(50),
    binary_id   BLOB,
    description TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- composite primary key
    CONSTRAINT pk_bytea_pk_test PRIMARY KEY (category_id, binary_id(100))
) ENGINE=InnoDB DEFAULT CHARSET=gb2312;
```

```
CREATE TABLE test_db_1.bytea_pk_utf8_test (
    category_id VARCHAR(50),
    binary_id   BLOB,
    description TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- composite primary key
    CONSTRAINT pk_bytea_pk_test PRIMARY KEY (category_id, binary_id(100))
);
```

DROP DATABASE IF EXISTS apecloud_resumer_test;
CREATE DATABASE apecloud_resumer_test;

```
CREATE TABLE IF NOT EXISTS `apecloud_resumer_test`.`ape_task_position` (
    id bigint AUTO_INCREMENT PRIMARY KEY,
    task_id varchar(255) NOT NULL,
    resumer_type varchar(255) NOT NULL,
    position_key varchar(255) NOT NULL,
    position_data text,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_task_id_task_type_position_key` (task_id, resumer_type, position_key)
);
```

insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotFinished', 'test_db_@-finished_table_*$1', '{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$1"}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotFinished', 'test_db_@-finished_table_*$2', '{"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db_@","tb":"finished_table_*$2"}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_no_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_no_uk","order_key":{"single":["f_0",null]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-one_pk_no_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_no_uk","order_key":{"single":["f_0","5"]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_one_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_one_uk","order_key":{"single":["f_0",null]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-no_pk_multi_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"no_pk_multi_uk","order_key":{"single":["f_0",null]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-one_pk_multi_uk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"one_pk_multi_uk","order_key":{"single":["f_0","5"]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-multi_pk', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"multi_pk","order_key":{"composite":[["f_0","1"],["f_1","30"]]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-nullable_composite_unique_key_table', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"nullable_composite_unique_key_table","order_key":{"composite":[["uk1","6"],["uk2","6"]]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-bytea_pk_gb2312_test', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"bytea_pk_gb2312_test","order_key":{"composite":[["category_id","cat1"],["binary_id","c4e3bac3cac0bde730"]]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_1-bytea_pk_utf8_test', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_1","tb":"bytea_pk_utf8_test","order_key":{"composite":[["category_id","cat1"],["binary_id","e4bda0e5a5bde4b896e7958c30"]]}}');
insert into `apecloud_resumer_test`.`ape_task_position` (task_id, resumer_type, position_key, position_data) values ('resume_db_test_1', 'SnapshotDoing', 'test_db_@-resume_table_*$4', '{"type":"RdbSnapshot","db_type":"mysql","schema":"test_db_@","tb":"resume_table_*$4","order_key":{"single":["p.k","1"]}}');






