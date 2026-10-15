# MySQL/Postgres -> Kafka templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

`to_<kind>` defines a dispatch mapping; `to_topic` dispatches data to Kafka topics. In `[rules.table]`, the source kind is `table` and the receiving kind is `topic`; use source table names on the left. `route` changes names inside messages, while dispatch selects where messages are sent.

The configurations below cover the producer leg of a MySQL/PostgreSQL → Kafka topology. For a complete MySQL → Kafka → MySQL topology, including one producer, three consumers and final destination tables, see the [dispatch example](/docs/en/rules.md#topology-example-mysql--kafka--mysql).

# MySQL Snapshot
```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[rules.table]
do=test_db_1.*,test_db_2.*
to_topic=*.*:default_topic,test_db_2.*:topic2,test_db_2.tb_1:topic3

[rules.event]
do=insert

[sinker]
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- refer to [config details](/docs/en/config.md) for `[rules.table] to_topic`

- [sinker]

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| url | url of Kafka servers | 127.0.0.1:9093 | - |
| with_field_defs | when sending data to Kafka in avro format, include the definitions of data fields or not | true | true |

# MySQL CDC
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=0
binlog_filename=
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[rules.table]
do=*.*
to_topic=*.*:test

[rules.event]
do=insert,update,delete

[rules.ddl]
do=*

[sinker]
db_type=kafka
sink_type=write
batch_size=200
url=127.0.0.1:9093
with_field_defs=true

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

# Posgres Snapshot

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
batch_size=10000
```

# Postgres CDC

The only difference with MySQL is [extractor]

```
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
start_lsn=0/406DE430
slot_name=ape_test
```