# Mongo -> Mongo templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

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

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| source | op_log / change_stream, change_stream is recommended if the source mongo version is 6.0+ | op_log | change_stream |
| start_timestamp | the starting UTC timestamp to pull op logs from | 1728525445 | 0, which means from newest |

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

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| resume_token | the resume_token to pull change stream from | - | empty, which means from newest |

# Data check
```
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0

[checker]
db_type=mongo
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
parallel_type=rdb_check
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

| Config | Description | Example | Default |
| :-------- | :-------- | :-------- | :-------- |
| check_log_dir | the directory of check log, required | ./check_task/logs/check | - |

# Data review
```
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0
check_log_dir=./logs/origin_check_log
batch_size=200

[checker]
db_type=mongo
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
parallel_type=rdb_check
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
