# Migrate data from Postgres to Doris

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/pg_to_doris.md) and [common configs](/docs/en/config.md) for more details.

# Prepare Postgres instance
Refer to [pg to pg](./pg_to_pg.md)

# Prepare Doris instance

```
docker run -itd --name some-doris \
-p 9030:9030 \
-p 8030:8030 \
-p 8040:8040 \
"$DORIS_IMAGE"
```

# Migrate structures
## Prepare source data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
url=mysql://root:@127.0.0.1:9030
sink_type=struct
db_type=doris

[rules.schema]
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
mysql -P 9030 -h 127.0.0.1 -u root --prompt="Doris > "

SHOW CREATE TABLE test_db.tb_1;
```

```
CREATE TABLE `tb_1` (
  `id` INT NOT NULL,
  `value` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
```

# Migrate snapshot data
## Prepare source data
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=doris
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040
batch_size=5000

[rules.schema]
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
mysql -P 9030 -h 127.0.0.1 -u root --prompt="Doris > "

SELECT * FROM test_db.tb_1;
```

```
+------+-------+
| id   | value |
+------+-------+
|    1 |     1 |
|    2 |     2 |
|    3 |     3 |
|    4 |     4 |
+------+-------+
```

# Cdc task

## Drop replication slot if exists
```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

[rules.schema]
do=test_db

[rules.event]
do=insert,update,delete

[sinker]
db_type=doris
sink_type=write
url=mysql://root:@127.0.0.1:9030
stream_load_url=mysql://root:@127.0.0.1:8040
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
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results
```
mysql -P 9030 -h 127.0.0.1 -u root --prompt="Doris > "

SELECT * FROM test_db.tb_1;
```

```
+------+---------+
| id   | value   |
+------+---------+
|    2 | 2000000 |
|    3 |       3 |
|    4 |       4 |
|    5 |       5 |
+------+---------+
```

# How it works

Refer to [mysql to doris](/docs/en/tutorial/mysql_to_doris.md)

# Data type mapping
- Create a table in Postgres

```
CREATE SCHEMA test_db;

CREATE TABLE test_db.full_column_type (
    id serial PRIMARY KEY, 

    -- char
    char_col char,
    char_col_2 char(255),
    character_col character,
    character_col_2 character(255),

    -- varchar
    varchar_col varchar, 
    varchar_col_2 varchar(255), 
    character_varying_col character varying,
    character_varying_col_2 character varying(255),

    bpchar_col bpchar,
    bpchar_col_2 bpchar(10),

    text_col text, 

    -- float
    real_col real, 
    float4_col float4,

    -- double
    double_precision_col double precision, 
    float8_col float8,

    -- decimal
    numeric_col numeric, 
    numeric_col_2 numeric(10, 2), 
    decimal_col decimal, 
    decimal_col_2 decimal(10, 2), 

    -- small int
    smallint_col smallint, 
    int2_col int2,
    smallserial_col smallserial,
    serial2_col smallserial,

    -- int
    integer_col integer,
    int_col int,
    int4_col int,
    serial_col serial,
    serial4_col serial4,

    -- bigint
    bigint_col bigint, 
    int8_col int8, 
    bigserial_col bigserial,
    serial8_col serial8,

    -- bit
    bit_col bit,
    bit_col_2 bit(10),
    bit_varying_col bit varying,
    bit_varying_col_2 bit varying(10),
    varbit_col varbit,
    varbit_col_2 varbit(10),

    -- time
    time_col time, 
    time_col_2 time(6),
    time_col_3 time without time zone,
    time_col_4 time(6) without time zone,

    -- timez
    timez_col timetz,
    timez_col_2 timetz(6),
    timez_col_3 time with time zone,
    timez_col_4 time(6) with time zone,

    -- timestamp
    timestamp_col timestamp, 
    timestamp_col_2 timestamp(6),
    timestamp_col_3 timestamp without time zone,
    timestamp_col_4 timestamp(6) without time zone,

    -- timestampz
    timestampz_col timestamptz,
    timestampz_col_2 timestamptz(6),
    timestampz_col_3 timestamp with time zone,
    timestampz_col_4 timestamp(6) with time zone,

    date_col date, 
    
    bytea_col bytea, 

    -- bool
    boolean_col boolean, 
    bool_col bool,

    -- json
    json_col json, 
    jsonb_col jsonb, 

    -- interval
    interval_col interval, 
    interval_col_2 interval(3), 

    -- array
    array_float4_col float4[],
    array_float8_col float8[],

    array_int2_col int2[],
    array_int4_col int4[],
    array_int8_col bigint[],
    array_int8_col_2 int8[],

    array_text_col text[],

    array_boolean_col boolean[],
    array_boolean_col_2 bool[],

    array_date_col date[],

    array_timestamp_col timestamp[],
    array_timestamp_col_2 timestamp(6)[],
    array_timestamptz_col timestamptz[],
    array_timestamptz_col_2 timestamptz(6)[],

    -- others
    box_col box, 
    cidr_col cidr,
    circle_col circle,
    inet_col inet,

    line_col line,
    lseg_col lseg, 
    macaddr_col macaddr,
    macaddr8_col macaddr8,
    money_col money,

    path_col path, 
    pg_lsn_col pg_lsn,
    pg_snapshot_col pg_snapshot,
    polygon_col polygon, 
    point_col point, 

    tsquery_col tsquery,
    tsvector_col tsvector,
    txid_snapshot_col txid_snapshot,

    uuid_col uuid, 
    xml_col xml
);
```

- The generated sql to be executed in Doris when migrate structures by ape_dts:
```
CREATE TABLE IF NOT EXISTS `test_db`.`full_column_type` (
  `id` INT NOT NULL, 
  `char_col` STRING, 
  `char_col_2` STRING, 
  `character_col` STRING, 
  `character_col_2` STRING, 
  `varchar_col` STRING, 
  `varchar_col_2` STRING, 
  `character_varying_col` STRING, 
  `character_varying_col_2` STRING, 
  `bpchar_col` STRING, 
  `bpchar_col_2` STRING, 
  `text_col` STRING, 
  `real_col` FLOAT, 
  `float4_col` FLOAT, 
  `double_precision_col` DOUBLE, 
  `float8_col` DOUBLE, 
  `numeric_col` DECIMAL(38, 9), 
  `numeric_col_2` DECIMAL(38, 9), 
  `decimal_col` DECIMAL(38, 9), 
  `decimal_col_2` DECIMAL(38, 9), 
  `smallint_col` SMALLINT, 
  `int2_col` SMALLINT, 
  `smallserial_col` SMALLINT, 
  `serial2_col` SMALLINT, 
  `integer_col` INT, 
  `int_col` INT, 
  `int4_col` INT, 
  `serial_col` INT, 
  `serial4_col` INT, 
  `bigint_col` BIGINT, 
  `int8_col` BIGINT, 
  `bigserial_col` BIGINT, 
  `serial8_col` BIGINT, 
  `bit_col` STRING, 
  `bit_col_2` STRING, 
  `bit_varying_col` STRING, 
  `bit_varying_col_2` STRING, 
  `varbit_col` STRING, 
  `varbit_col_2` STRING, 
  `time_col` VARCHAR(255), 
  `time_col_2` VARCHAR(255), 
  `time_col_3` VARCHAR(255), 
  `time_col_4` VARCHAR(255), 
  `timez_col` VARCHAR(255), 
  `timez_col_2` VARCHAR(255), 
  `timez_col_3` VARCHAR(255), 
  `timez_col_4` VARCHAR(255), 
  `timestamp_col` DATETIME(6), 
  `timestamp_col_2` DATETIME(6), 
  `timestamp_col_3` DATETIME(6), 
  `timestamp_col_4` DATETIME(6), 
  `timestampz_col` DATETIME(6), 
  `timestampz_col_2` DATETIME(6), 
  `timestampz_col_3` DATETIME(6), 
  `timestampz_col_4` DATETIME(6), 
  `date_col` DATE, 
  `bytea_col` STRING, 
  `boolean_col` BOOLEAN, 
  `bool_col` BOOLEAN, 
  `json_col` JSON, 
  `jsonb_col` JSON, 
  `interval_col` VARCHAR(255), 
  `interval_col_2` VARCHAR(255), 
  `array_float4_col` STRING, 
  `array_float8_col` STRING, 
  `array_int2_col` STRING, 
  `array_int4_col` STRING, 
  `array_int8_col` STRING, 
  `array_int8_col_2` STRING, 
  `array_text_col` STRING, 
  `array_boolean_col` STRING, 
  `array_boolean_col_2` STRING, 
  `array_date_col` STRING, 
  `array_timestamp_col` STRING, 
  `array_timestamp_col_2` STRING, 
  `array_timestamptz_col` STRING, 
  `array_timestamptz_col_2` STRING, 
  `box_col` STRING, 
  `cidr_col` STRING, 
  `circle_col` STRING, 
  `inet_col` STRING, 
  `line_col` STRING, 
  `lseg_col` STRING, 
  `macaddr_col` STRING, 
  `macaddr8_col` STRING, 
  `money_col` STRING, 
  `path_col` STRING, 
  `pg_lsn_col` STRING, 
  `pg_snapshot_col` STRING, 
  `polygon_col` STRING, 
  `point_col` STRING, 
  `tsquery_col` STRING, 
  `tsvector_col` STRING, 
  `txid_snapshot_col` STRING, 
  `uuid_col` STRING, 
  `xml_col` STRING
) UNIQUE KEY (`id`) DISTRIBUTED BY HASH(`id`) PROPERTIES ("replication_num" = "1")
```

# Supported versions

Refer to [mysql to doris](/docs/en/tutorial/mysql_to_doris.md)

# DDL during CDC is NOT supported yet
Currently, DDL events are ignored, we may support this in future.