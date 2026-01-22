# Migrate data from MySQL to StarRocks

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mysql_to_starrocks.md) and [common configs](/docs/en/config.md) for more details.

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Prepare StarRocks instance
- tested versions: 2.5.4 to 3.2.11

```
docker run -itd --name some-starrocks \
-p 9030:9030 \
-p 8030:8030 \
-p 8040:8040 \
"$STARROCKS_IMAGE"
```

# Migrate structures
## Prepare source data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
url=mysql://root:@127.0.0.1:9030
sink_type=struct
db_type=starrocks

[filter]
do_dbs=test_db

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SHOW CREATE TABLE test_db.tb_1;
```

```
CREATE TABLE `tb_1` (
  `id` int(11) NOT NULL COMMENT "",
  `value` int(11) NULL COMMENT "",
  `_ape_dts_is_deleted` boolean NULL COMMENT "",
  `_ape_dts_timestamp` bigint(20) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

# Migrate snapshot data
## Prepare source data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040
batch_size=5000

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```
+------+-------+---------------------+--------------------+
| id   | value | _ape_dts_is_deleted | _ape_dts_timestamp |
+------+-------+---------------------+--------------------+
|    1 |     1 |                NULL |    1731665154965   |
|    2 |     2 |                NULL |    1731665159858   |
|    3 |     3 |                NULL |    1731665159880   |
|    4 |     4 |                NULL |    1731665159880   |
+------+-------+---------------------+--------------------+
```

# Cdc task with hard delete (NOT recommended)

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert,update,delete
do_tbs=test_db.table1,test_db.table2

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040
hard_delete=true
batch_size=5000

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```
+------+---------+---------------------+--------------------+
| id   | value   | _ape_dts_is_deleted | _ape_dts_timestamp |
+------+---------+---------------------+--------------------+
|    2 | 2000000 |                NULL |    1731665679461   |
|    3 |       3 |                NULL |    1731665609225   |
|    4 |       4 |                NULL |    1731665609236   |
|    5 |       5 |                NULL |    1731665679572   |
+------+---------+---------------------+--------------------+
```

# Cdc task with soft delete (recommended)
## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040
batch_size=5000

[parallelizer]
parallel_type=table
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

DELETE FROM test_db.tb_1 WHERE id=3;
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "

SELECT * FROM test_db.tb_1;
```

```
+------+---------+---------------------+--------------------+
| id   | value   | _ape_dts_is_deleted | _ape_dts_timestamp |
+------+---------+---------------------+--------------------+
|    2 | 2000000 |                NULL |    1731665679461   |
|    3 |       3 |                   1 |    1731665747381   |
|    4 |       4 |                NULL |    1731665609236   |
|    5 |       5 |                NULL |    1731665679572   |
+------+---------+---------------------+--------------------+
```

# How it works

We use [Stream Load](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/) to import data from MySQL. You need to configure url (query metadata) and stream_load_url (specify Stream Load port and user info).

When importing data into StarRocks by Stream Load, you need to avoid frequent small-batch imports, as this may cause throttle errors in StarRocks. This can be resolved by configuring batch_sink_interval_secs, refer to [task templates](/docs/templates/mysql_to_starrocks.md). Usually, only CDC tasks need to configure batch_sink_interval_secs.

Stream Load allows importing up to 10GB of data in a single load. You can change the following configurations to adjust the batch data size.

```
[pipeline]
buffer_size=100000
buffer_memory_mb=200

[sinker]
batch_size=5000
```

Refer to [config](/docs/en/config.md) for other common configurations

# Data type mapping

| MySQL | StarRocks |
| :-------- | :-------- |
| tinyint | TINYINT |
| tinyint unsigned | SMALLINT |
| smallint | SMALLINT |
| smallint unsigned | INT |
| mediumint | INT |
| mediumint unsigned | BIGINT |
| int | INT |
| int unsigned | BIGINT |
| bigint | BIGINT |
| bigint unsigned | LARGEINT |
| decimal | DECIMAL |
| float | FLOAT |
| double | DOUBLE |
| bit | BIGINT |
| datetime | DATETIME |
| time | VARCHAR |
| date | DATE |
| year | INT |
| timestamp | DATETIME |
| char | CHAR |
| varchar | VARCHAR |
| binary | VARBINARY |
| varbinary | VARBINARY |
| tinytext | STRING |
| text | STRING |
| mediumtext | STRING |
| longtext | STRING |
| tinyblob | VARBINARY |
| blob | VARBINARY |
| mediumblob | VARBINARY |
| longblob | VARBINARY |
| enum | VARCHAR |
| set | VARCHAR |
| json | JSON/STRING |

## Example
- Create a table in MySQL

```
CREATE TABLE test_db.one_pk_no_uk ( 
    f_0 tinyint, 
    f_0_1 tinyint unsigned, 
    f_1 smallint, 
    f_1_1 smallint unsigned, 
    f_2 mediumint,
    f_2_1 mediumint unsigned, 
    f_3 int, 
    f_3_1 int unsigned, 
    f_4 bigint, 
    f_4_1 bigint unsigned, 
    f_5 decimal(10,4), 
    f_6 float(6,2), 
    f_7 double(8,3), 
    f_8 bit(64),
    f_9 datetime(6), 
    f_10 time(6), 
    f_11 date, 
    f_12 year, 
    f_13 timestamp(6) NULL, 
    f_14 char(255), 
    f_15 varchar(255), 
    f_16 binary(255), 
    f_17 varbinary(255), 
    f_18 tinytext, 
    f_19 text, 
    f_20 mediumtext, 
    f_21 longtext, 
    f_22 tinyblob, 
    f_23 blob, 
    f_24 mediumblob, 
    f_25 longblob, 
    f_26 enum('x-small','small','medium','large','x-large'), 
    f_27 set('a','b','c','d','e'), 
    f_28 json,
    PRIMARY KEY (f_0) );
```

- The generated sql to be executed in StarRocks when migrate structures by ape_dts:
```
CREATE TABLE IF NOT EXISTS `test_db`.`one_pk_no_uk` (
    `f_0` TINYINT NOT NULL,
    `f_0_1` SMALLINT,
    `f_1` SMALLINT,
    `f_1_1` INT,
    `f_2` INT,
    `f_2_1` BIGINT,
    `f_3` INT,
    `f_3_1` BIGINT,
    `f_4` BIGINT,
    `f_4_1` LARGEINT,
    `f_5` DECIMAL(10, 4),
    `f_6` FLOAT,
    `f_7` DOUBLE,
    `f_8` BIGINT,
    `f_9` DATETIME,
    `f_10` VARCHAR(255),
    `f_11` DATE,
    `f_12` INT,
    `f_13` DATETIME,
    `f_14` CHAR(255),
    `f_15` VARCHAR(255),
    `f_16` VARBINARY,
    `f_17` VARBINARY,
    `f_18` STRING,
    `f_19` STRING,
    `f_20` STRING,
    `f_21` STRING,
    `f_22` VARBINARY,
    `f_23` VARBINARY,
    `f_24` VARBINARY,
    `f_25` VARBINARY,
    `f_26` VARCHAR(255),
    `f_27` VARCHAR(255),
    `f_28` JSON,
    `_ape_dts_is_deleted` BOOLEAN,
    `_ape_dts_timestamp` BIGINT
) PRIMARY KEY (`f_0`) DISTRIBUTED BY HASH(`f_0`);
```

# Soft delete or Hard delete 
Due to the poor performance of StarRocks in handling delete operations, hard delete is always NOT recommended.

Soft delete prerequisites: 
- target table must have a `_ape_dts_is_deleted` column.

Hard delete prerequisites: 
- `[parallelizer] parallel_type` must be `rdb_merge`.

# Supported versions

We've tested on StarRocks 2.5.4 and 3.2.11, refer to [tests](/dt-tests/tests/mysql_to_starrocks/)

For 2.5.4, the stream_load_url should use be_http_port instead of fe_http_port.

# DDL during CDC is NOT supported yet
Currently, DDL events are ignored, we may support this in future.