# MySQL -> Doris templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# Struct
```
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
url=mysql://root:@127.0.0.1:9030
sink_type=struct
db_type=doris
conflict_policy=interrupt

[rules.db]
do=test_db

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
db_type=doris
sink_type=write
url=mysql://root:123456@127.0.0.1:9030
stream_load_url=mysql://root:123456@127.0.0.1:8040
batch_size=5000

[rules.table]
do=test_db.*

[rules.event]
do=insert

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
| url | the url of Doris FE, used for metadata query | - | - |
| stream_load_url | the url for Stream Load | - | - |
| batch_size | the max record count in one Stream Load | - | - |

# CDC

```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=5299302
binlog_filename=mysql-bin.000035
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[rules.table]
do=test_db.*

[rules.event]
do=insert,update,delete

[sinker]
db_type=doris
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

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| batch_sink_interval_secs | when importing data into Doris by Stream Load, avoid frequent small-batch imports, as this may cause throttle errors in Doris. If the batch_sink_interval_secs is set, Stream Load will be triggered when either of the following conditions is met: 1) the pipeline's buffer is full, or 2) it has been more than batch_sink_interval_secs seconds since the last Stream Load. | 15 | 0 |