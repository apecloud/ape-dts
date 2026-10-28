# MySQL -> ClickHouse templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# Struct
```
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=clickhouse
url=http://admin:123456@127.0.0.1:8123
conflict_policy=interrupt

[source.db]
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
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
batch_size=10000

[sinker]
db_type=clickhouse
sink_type=write
url=http://admin:123456@127.0.0.1:8123
conflict_policy=interrupt
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

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# CDC
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=5299302
binlog_filename=mysql-bin.000035
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=clickhouse
sink_type=write
url=http://admin:123456@127.0.0.1:8123
conflict_policy=interrupt
batch_size=5000

[source.table]
include=test_db.*

[source.event]
include=insert,update,delete

[parallelizer]
parallel_type=table
parallel_size=8

[pipeline]
buffer_size=100000
buffer_memory_mb=200
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```