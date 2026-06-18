# Mongo -> Mongo templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# Struct

```
[extractor]
db_type=mongo
extract_type=struct
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[sinker]
db_type=mongo
sink_type=struct
url=mongodb://ape_dts:123456@127.0.0.1:27018
conflict_policy=interrupt

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=
// do_structures=*
// do_structures=collection,shardkey
do_structures=collection

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=100
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- [filter]

| Config        | Description                                                                                                   | Example                   | Default             |
| :------------ | :------------------------------------------------------------------------------------------------------------ | :------------------------ | :------------------ |
| do_structures | one or multiple in [collection,shardkey]. `shardkey` is only meaningful when the source collection is sharded | collection,index,shardkey | \*, which means all |

Mongo struct migration currently supports databases implicitly through collection creation. It does
not use a separate Mongo `database` structure type. `collection` creates Mongo collections and
copies collection options. `shardkey` copies
the source sharding definition; when the target is a sharded cluster, DTS runs `enableSharding`
before `shardCollection` if needed. When the target is not `mongos`, shard key statements are
ignored and collection migration can still run.

System collections such as `system.*` are filtered out. Views and time-series collections are not
part of the current Mongo struct migration scope.

# Snapshot

```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# CDC, by op_log

```
[extractor]
db_type=mongo
extract_type=cdc
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
source=op_log
start_timestamp=1728525445

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mongo
sink_type=write
batch_size=200
url=mongodb://ape_dts:123456@127.0.0.1:27018

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- [extractor]

| Config          | Description                                                                              | Example    | Default                    |
| :-------------- | :--------------------------------------------------------------------------------------- | :--------- | :------------------------- |
| source          | op_log / change_stream, change_stream is recommended if the source mongo version is 6.0+ | op_log     | change_stream              |
| start_timestamp | the starting UTC timestamp to pull op logs from                                          | 1728525445 | 0, which means from newest |

## Mongo CDC source capability boundary

- `source=op_log` reads `local.oplog.rs` and replays DML from oplog entries. It supports insert,
  delete, legacy `$set` / `$unset` updates, and common MongoDB `$v:2 diff` updates including
  top-level `i` / `u` / `d` fields and nested document sub-diffs such as `sprofile.u.name`.
  Array sub-diffs (`a`) are not fully supported yet; if an array is written as a whole field value
  it can be replayed, but element-level array diffs may be skipped or unsupported. Oplog DDL replay
  is not the recommended path.
- `source=change_stream` watches MongoDB change streams with `fullDocument=UpdateLookup` and replays
  update/replace events from the full post-image document. It is the recommended CDC source for
  complex document fields such as arrays and nested documents. Change stream DDL replay depends on
  MongoDB 6.0+ `showExpandedEvents`; on older MongoDB versions, use it for DML only.
- For sharded targets, update/delete/upsert filters should contain the full target shard key. If the
  target is sharded by `_id`, Mongo CDC can usually build the required filter from the document key.
  For other shard keys, make sure the source event/full document carries the shard key fields or keep
  the default fail-fast shard-key validation enabled.

# CDC, by change_stream

```
[extractor]
db_type=mongo
extract_type=cdc
resume_token={"_data":"826707373B000000012B022C0100296E5A1004B4A9FD2BFD9C44609366CD4CD6A3D98E46645F696400646707373B22E3B8A398F7FB340004"}
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
source=change_stream

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mongo
sink_type=write
batch_size=200
url=mongodb://ape_dts:123456@127.0.0.1:27018

[router]
tb_map=
col_map=
db_map=

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[parallelizer]
parallel_type=mongo
parallel_size=8

[runtime]
log_dir=./logs
log_level=info
log4rs_file=./log4rs.yaml
```

- [extractor]

| Config       | Description                                 | Example | Default                        |
| :----------- | :------------------------------------------ | :------ | :----------------------------- |
| resume_token | the resume_token to pull change stream from | -       | empty, which means from newest |

# Standalone snapshot check

```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[checker]
enable=true
db_type=mongo
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=100

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/check/

# Inline snapshot check

```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[checker]
enable=true
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/check/
- `[checker]` intentionally omits `db_type` / `url` / `username` / `password`; inline snapshot
  check reuses the parsed `[sinker]` target.

# Inline cdc check

MongoDB CDC currently does not support inline cdc check. Use standalone snapshot check or inline
snapshot check instead.

# Data revise

```
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
check_log_dir=./check_task/logs/check
batch_size=200

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- [extractor]

| Config        | Description                          | Example                 | Default |
| :------------ | :----------------------------------- | :---------------------- | :------ |
| check_log_dir | the directory of check log, required | ./check_task/logs/check | -       |

# Data review

```
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
check_log_dir=./logs/origin_check_log
batch_size=200

[checker]
enable=true
db_type=mongo
url=mongodb://ape_dts:123456@127.0.0.1:27018
batch_size=100

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db_1.*,test_db_2.*
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=mongo
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/check/
