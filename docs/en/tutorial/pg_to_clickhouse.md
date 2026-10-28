# Migrate data from Postgres to ClickHouse

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/pg_to_clickhouse.md) and [common configs](/docs/en/config.md) for more details.

# Prepare Postgres instance
Refer to [pg to pg](./pg_to_pg.md)

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
sink_type=struct
db_type=clickhouse
url=http://admin:123456@127.0.0.1:8123

[source.schema]
include=test_db

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
db_type=clickhouse
sink_type=write
url=http://admin:123456@127.0.0.1:8123
batch_size=5000

[source.schema]
include=test_db

[source.event]
include=insert

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

SELECT * FROM test_db.tb_1;
```

```
┌─id─┬─value─┬─_ape_dts_is_deleted─┬─_ape_dts_timestamp─┐
│  1 │     1 │                   0 │      1736500603659 │
│  2 │     2 │                   0 │      1736500603659 │
│  3 │     3 │                   0 │      1736500603659 │
│  4 │     4 │                   0 │      1736500603659 │
└────┴───────┴─────────────────────┴────────────────────┘
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

[source.schema]
include=test_db

[source.event]
include=insert,update,delete

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
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

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
│  1 │    ᴺᵁᴸᴸ │                   1 │      1736500859060 │
│  2 │ 2000000 │                   0 │      1736500859060 │
│  3 │       3 │                   0 │      1736500603659 │
│  4 │       4 │                   0 │      1736500603659 │
│  5 │       5 │                   0 │      1736500859060 │
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

# Data type mapping
- Create a table in Postgres

```
CREATE SCHEMA test_db;

CREATE TABLE test_db_1.full_column_type (
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

- The generated sql to be executed in ClickHouse when migrate structures by ape_dts:
```
CREATE TABLE test_db_1.full_column_type
(
    `id` Int32,
    `char_col` Nullable(String),
    `char_col_2` Nullable(String),
    `character_col` Nullable(String),
    `character_col_2` Nullable(String),
    `varchar_col` Nullable(String),
    `varchar_col_2` Nullable(String),
    `character_varying_col` Nullable(String),
    `character_varying_col_2` Nullable(String),
    `bpchar_col` Nullable(String),
    `bpchar_col_2` Nullable(String),
    `text_col` Nullable(String),
    `real_col` Nullable(Float32),
    `float4_col` Nullable(Float32),
    `double_precision_col` Nullable(Float64),
    `float8_col` Nullable(Float64),
    `numeric_col` Nullable(Decimal(38, 9)),
    `numeric_col_2` Nullable(Decimal(38, 9)),
    `decimal_col` Nullable(Decimal(38, 9)),
    `decimal_col_2` Nullable(Decimal(38, 9)),
    `smallint_col` Nullable(Int16),
    `int2_col` Nullable(Int16),
    `smallserial_col` Int16,
    `serial2_col` Int16,
    `integer_col` Nullable(Int32),
    `int_col` Nullable(Int32),
    `int4_col` Nullable(Int32),
    `serial_col` Int32,
    `serial4_col` Int32,
    `bigint_col` Nullable(Int64),
    `int8_col` Nullable(Int64),
    `bigserial_col` Int64,
    `serial8_col` Int64,
    `bit_col` Nullable(String),
    `bit_col_2` Nullable(String),
    `bit_varying_col` Nullable(String),
    `bit_varying_col_2` Nullable(String),
    `varbit_col` Nullable(String),
    `varbit_col_2` Nullable(String),
    `time_col` Nullable(String),
    `time_col_2` Nullable(String),
    `time_col_3` Nullable(String),
    `time_col_4` Nullable(String),
    `timez_col` Nullable(String),
    `timez_col_2` Nullable(String),
    `timez_col_3` Nullable(String),
    `timez_col_4` Nullable(String),
    `timestamp_col` Nullable(DateTime64(6)),
    `timestamp_col_2` Nullable(DateTime64(6)),
    `timestamp_col_3` Nullable(DateTime64(6)),
    `timestamp_col_4` Nullable(DateTime64(6)),
    `timestampz_col` Nullable(DateTime64(6)),
    `timestampz_col_2` Nullable(DateTime64(6)),
    `timestampz_col_3` Nullable(DateTime64(6)),
    `timestampz_col_4` Nullable(DateTime64(6)),
    `date_col` Nullable(Date32),
    `bytea_col` Nullable(String),
    `boolean_col` Nullable(Bool),
    `bool_col` Nullable(Bool),
    `json_col` Nullable(String),
    `jsonb_col` Nullable(String),
    `interval_col` Nullable(String),
    `interval_col_2` Nullable(String),
    `array_float4_col` Nullable(String),
    `array_float8_col` Nullable(String),
    `array_int2_col` Nullable(String),
    `array_int4_col` Nullable(String),
    `array_int8_col` Nullable(String),
    `array_int8_col_2` Nullable(String),
    `array_text_col` Nullable(String),
    `array_boolean_col` Nullable(String),
    `array_boolean_col_2` Nullable(String),
    `array_date_col` Nullable(String),
    `array_timestamp_col` Nullable(String),
    `array_timestamp_col_2` Nullable(String),
    `array_timestamptz_col` Nullable(String),
    `array_timestamptz_col_2` Nullable(String),
    `box_col` Nullable(String),
    `cidr_col` Nullable(String),
    `circle_col` Nullable(String),
    `inet_col` Nullable(String),
    `line_col` Nullable(String),
    `lseg_col` Nullable(String),
    `macaddr_col` Nullable(String),
    `macaddr8_col` Nullable(String),
    `money_col` Nullable(String),
    `path_col` Nullable(String),
    `pg_lsn_col` Nullable(String),
    `pg_snapshot_col` Nullable(String),
    `polygon_col` Nullable(String),
    `point_col` Nullable(String),
    `tsquery_col` Nullable(String),
    `tsvector_col` Nullable(String),
    `txid_snapshot_col` Nullable(String),
    `uuid_col` Nullable(UUID),
    `xml_col` Nullable(String),
    `_ape_dts_is_deleted` Int8,
    `_ape_dts_timestamp` Int64
)
ENGINE = ReplacingMergeTree(_ape_dts_timestamp)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192
```

# DDL during CDC is NOT supported yet
Currently, DDL events are ignored, we may support this in future.