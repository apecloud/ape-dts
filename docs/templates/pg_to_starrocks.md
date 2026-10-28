# Postgres -> StarRocks templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

The templates are almost same with [mysql to starrocks](/docs/templates/mysql_to_starrocks.md), the only differences are the [extractor] configs.

# Struct
```
[extractor]
extract_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
url=mysql://root:@127.0.0.1:9030
sink_type=struct
db_type=starrocks
conflict_policy=interrupt

[source.schema]
include=test_db

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs

[parallelizer]
parallel_type=serial

[pipeline]
checkpoint_interval_secs=10
buffer_size=100
```

# Snapshot
```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:123456@127.0.0.1:9030
stream_load_url=mysql://root:123456@127.0.0.1:8040
batch_size=5000

[source.table]
include=test_db.*

[source.event]
include=insert

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=100000
buffer_memory_mb=200
checkpoint_interval_secs=10
batch_sink_interval_secs=0

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- [sinker]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| url | the url of StarRocks FE, used for metadata query | - | - |
| stream_load_url | the url for Stream Load | - | - |
| batch_size | the max record count in one Stream Load | - | - |

# CDC

## Soft delete
```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test

[source.table]
include=test_db.*

[source.event]
include=insert,update,delete

[sinker]
db_type=starrocks
sink_type=write
url=mysql://root:123456@127.0.0.1:9030
stream_load_url=mysql://root:123456@127.0.0.1:8040
batch_size=5000

[parallelizer]
parallel_type=table
parallel_size=8

[pipeline]
buffer_size=100000
buffer_memory_mb=200
checkpoint_interval_secs=10
batch_sink_interval_secs=15

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- [pipeline]

Refer to [mysql to starrocks](/docs/templates/mysql_to_starrocks.md)