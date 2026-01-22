# MySQL -> MySQL templates

Refer to [config details](/docs/en/config.md) for explanations of common fields.

# Struct

```
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
conflict_policy=interrupt

[filter]
do_dbs=test_db
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=
do_structures=

[router]
db_map=
tb_map=
col_map=

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

- [filter]

| Config          | Description        | Example   | Default   |
| :-------------- | :----------------- | :-------- | :-------- |
| conflict_policy | interrupt / ignore | interrupt | interrupt |

- [sinker]

| Config        | Description                                                           | Example        | Default             |
| :------------ | :-------------------------------------------------------------------- | :------------- | :------------------ |
| do_structures | one or multiple in [database,table,constraint,sequence,comment,index] | database,table | \*, which means all |

# Snapshot

```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
batch_size=10000

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200
replace=true

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db.*
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

# CDC

```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=5299302
binlog_filename=mysql-bin.000035
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
replace=true

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=rdb_merge
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

| Config          | Description                                                                                                          | Example          | Default |
| :-------------- | :------------------------------------------------------------------------------------------------------------------- | :--------------- | :------ |
| binlog_filename | the starting binlog file, if empty, will pull binlog from the last position of the last file                         | mysql-bin.000035 | empty   |
| binlog_position | the starting position in binlog_file to pull binlog, only works when binlog_filename is NOT empty                    | 5299302          | 0       |
| server_id       | the identifier of current ape-dts instance in the MySQL replication group, with a value from 1 to 2^32 - 1, required | 2000             | -       |

# CDC with gtid

```
[extractor]
db_type=mysql
extract_type=cdc
gtid_enabled=true
gtid_set=6d3960f6-4b36-11ef-8614-0242ac110002:1-10,787d08c4-4b36-11ef-8614-0242ac110006:1-5
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
ignore_dbs=
do_dbs=
do_tbs=test_db.*
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
replace=true

[router]
tb_map=
col_map=
db_map=

[parallelizer]
parallel_type=rdb_merge
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

| Config       | Description                               | Example                                   | Default                                              |
| :----------- | :---------------------------------------- | :---------------------------------------- | :--------------------------------------------------- |
| gtid_enabled | use Gtid_Set to pull binlog               | true                                      | false                                                |
| gtid_set     | the starting Gtid_Set to pull binlog from | 6d3960f6-4b36-11ef-8614-0242ac110002:1-10 | empty, which means from the latest Executed_Gtid_Set |

# CDC with ddl capture

- The differences with CDC task config:

```
[filter]
do_ddls=create_database,drop_database,alter_database,create_table,alter_table,drop_table,create_index,drop_index,truncate_table,rename_table
```

- [filter]

| Config  | Description                                                                                                                                                                                                                 | Example                             | Default                            |
| :------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------- | :--------------------------------- |
| do_ddls | the ddl types to capture and sync to target, it should be one or more among "create_database, drop_database, alter_database, create_table, alter_table, drop_table, create_index, drop_index, truncate_table, rename_table" | create_table,alter_table,drop_table | empty, which means ignore all ddls |

# Struct check

```
[extractor]
db_type=mysql
extract_type=struct
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
replace=true

[filter]
do_dbs=test_db
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/check/

# Data check

```
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
batch_size=10000

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=test_db.*
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
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./check_task/logs/check
batch_size=200

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=*

[router]
db_map=
tb_map=
col_map=

[parallelizer]
parallel_type=rdb_merge
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
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./logs/origin_check_log
batch_size=200

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=200

[filter]
do_dbs=
ignore_dbs=
do_tbs=
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

# CDC to sqls

```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=5299750
binlog_filename=mysql-bin.000035
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/sql.log

# CDC to reverse sqls

```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=5299750
binlog_filename=mysql-bin.000035
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
ignore_dbs=
do_tbs=
ignore_tbs=
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql
reverse=true

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

- the output will be in {log_dir}/sql.log
