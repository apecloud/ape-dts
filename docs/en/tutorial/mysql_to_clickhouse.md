# Migrate data from MySQL to Clickhouse

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mysql_to_clickhouse.md) and [common configs](/docs/en/config.md) for more details.

# Prepare MySQL instance
Refer to [mysql to mysql](./mysql_to_mysql.md)

# Prepare ClickHouse instance

```
docker run -d --name some-clickhouse-server \
--ulimit nofile=262144:262144 \
-p 9100:9000 \
-p 8123:8123 \
-e CLICKHOUSE_USER=admin -e CLICKHOUSE_PASSWORD=123456 \
"$CLICKHOUSE_IMAGE"
```

# Migrate structures
## Prepare source data
```
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
sink_type=struct
db_type=clickhouse
url=http://admin:123456@127.0.0.1:8123

[rules.db]
do=test_db

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
docker exec -it some-clickhouse-server clickhouse \
    client --user admin --password 123456

SHOW CREATE TABLE test_db.tb_1;
```

```
CREATE TABLE test_db.tb_1
(
    `id` Int32,
    `value` Nullable(Int32),
    `_ape_dts_is_deleted` Int8,
    `_ape_dts_timestamp` Int64
)
ENGINE = ReplacingMergeTree(_ape_dts_timestamp)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192
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
db_type=clickhouse
sink_type=write
url=http://admin:123456@127.0.0.1:8123
batch_size=5000

[rules.db]
do=test_db

[rules.event]
do=insert

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
docker exec -it some-clickhouse-server clickhouse \
    client --user admin --password 123456

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
   ┌─id─┬─value─┬─_ape_dts_is_deleted─┬─_ape_dts_timestamp─┐
1. │  1 │     1 │                   0 │    1731897789627   │
2. │  2 │     2 │                   0 │    1731897789627   │
3. │  3 │     3 │                   0 │    1731897789627   │
4. │  4 │     4 │                   0 │    1731897789627   │
   └────┴───────┴─────────────────────┴────────────────────┘
```

# Cdc task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[rules.db]
do=test_db

[rules.event]
do=insert,update,delete

[sinker]
db_type=clickhouse
sink_type=write
url=http://admin:123456@127.0.0.1:8123
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

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results
```
docker exec -it some-clickhouse-server clickhouse \
    client --user admin --password 123456

OPTIMIZE TABLE test_db.tb_1 FINAL;
SELECT * FROM test_db.tb_1;
```

```
   ┌─id─┬───value─┬─_ape_dts_is_deleted─┬─_ape_dts_timestamp─┐
1. │  1 │       1 │                   1 │    1731900431736   │
2. │  2 │ 2000000 │                   0 │    1731900431736   │
3. │  3 │       3 │                   0 │    1731900332526   │
4. │  4 │       4 │                   0 │    1731900332526   │
5. │  5 │       5 │                   0 │    1731900431736   │
   └────┴─────────┴─────────────────────┴────────────────────┘
```

# How it works

We convert source data into json and call http api to batch insert into ClickHouse, it is like:

curl -X POST -d @json_data 'http://localhost:8123/?query=INSERT%20INTO%test_db.tb_1%20FORMAT%20JSON' --user admin:123456

You can change the following configurations to adjust the batch data size.

```
[pipeline]
buffer_size=100000
buffer_memory_mb=200

[sinker]
batch_size=5000
```

Refer to [config](/docs/en/config.md) for other common configurations

# Column type mapping

| MySQL | ClickHouse |
| :-------- | :-------- |
| tinyint | Int8/UInt8 |
| smallint | Int16/UInt16 |
| mediumint | Int32/UInt32 |
| int | Int32/UInt32 |
| bigint | Int64/UInt64 |
| decimal | Decimal(P,S) |
| float | Float32 |
| double | Float64 |
| bit | UInt64 |
| datetime | DateTime64(6) |
| time | String |
| date | Date32 |
| year | Int32 |
| timestamp | DateTime64(6) |
| char | String |
| varchar | String |
| binary | String |
| varbinary | String |
| tinytext | String |
| text | String |
| mediumtext | String |
| longtext | String |
| tinyblob | String |
| blob | String |
| mediumblob | String |
| longblob | String |
| enum | String |
| set | String |
| json |String |

## Example
- Create a table with all supported types in MySQL

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

- The generated sql to be executed in ClickHouse when migrate structures by ape_dts:
```
CREATE TABLE IF NOT EXISTS `test_db`.`one_pk_no_uk` (
   `f_0` Int8, 
   `f_0_1` Nullable(UInt8), 
   `f_1` Nullable(Int16), 
   `f_1_1` Nullable(UInt16), 
   `f_2` Nullable(Int32), 
   `f_2_1` Nullable(UInt32), 
   `f_3` Nullable(Int32), 
   `f_3_1` Nullable(UInt32), 
   `f_4` Nullable(Int64), 
   `f_4_1` Nullable(UInt64), 
   `f_5` Nullable(Decimal(10, 4)), 
   `f_6` Nullable(Float32), 
   `f_7` Nullable(Float64), 
   `f_8` Nullable(UInt64), 
   `f_9` Nullable(DateTime64(6)), 
   `f_10` Nullable(String), 
   `f_11` Nullable(Date32), 
   `f_12` Nullable(Int32), 
   `f_13` Nullable(DateTime64(6)), 
   `f_14` Nullable(String), 
   `f_15` Nullable(String), 
   `f_16` Nullable(String), 
   `f_17` Nullable(String), 
   `f_18` Nullable(String), 
   `f_19` Nullable(String), 
   `f_20` Nullable(String), 
   `f_21` Nullable(String), 
   `f_22` Nullable(String), 
   `f_23` Nullable(String), 
   `f_24` Nullable(String), 
   `f_25` Nullable(String), 
   `f_26` Nullable(String), 
   `f_27` Nullable(String), 
   `f_28` Nullable(String), 
   `_ape_dts_is_deleted` Int8, 
   `_ape_dts_timestamp` Int64
   ) ENGINE = ReplacingMergeTree(`_ape_dts_timestamp`) PRIMARY KEY (`f_0`) 
   ORDER BY (`f_0`)
```

# DDL during CDC is NOT supported yet
Currently, DDL events are ignored, we may support this in future.