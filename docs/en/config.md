# Config details

Different tasks may require extra configs, refer to [task templates](/docs/templates/) and [tutorial](/docs/en/tutorial/)

# Example: MySQL -> MySQL

# [extractor]

| Config          | Description                                                                  | Example                                                        | Default                                                 |
| :-------------- | :--------------------------------------------------------------------------- | :------------------------------------------------------------- | :------------------------------------------------------ |
| db_type         | source database type                                                         | mysql                                                          | -                                                       |
| extract_type    | snapshot, cdc                                                                | snapshot                                                       | -                                                       |
| url             | database URL. You can specify the username and password directly in the URL. | mysql://127.0.0.1:3307 or mysql://root:password@127.0.0.1:3307 |
| username        | database connection username                                                 | root                                                           |
| password        | database connection password                                                 | password                                                       | -                                                       |
| max_connections | max connections for source database                                          | 10                                                             | currently 10, may be dynamically adjusted in the future |
| batch_size      | number of extracted records in a batch                                       | 10000                                                          | same as [pipeline] buffer_size                          |

## URL escaping

- If the username/password contains special characters, the corresponding parts need to be percent-encoded, for example:

```
create user user1@'%' identified by 'abc%$#?@';
The url should be:
url=mysql://user1:abc%25%24%23%3F%40@127.0.0.1:3307?ssl-mode=disabled
```

# [sinker]

| Config          | Description                                                                                                                          | Example                                                        | Default                                                 |
| :-------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------- | :------------------------------------------------------ |
| db_type         | target database type                                                                                                                 | mysql                                                          | -                                                       |
| sink_type       | write, check                                                                                                                         | write                                                          | write                                                   |
| url             | database URL. You can specify the username and password directly in the URL.                                                         | mysql://127.0.0.1:3307 or mysql://root:password@127.0.0.1:3307 |
| username        | database connection username                                                                                                         | root                                                           |
| password        | database connection password                                                                                                         | password                                                       | -                                                       |
| max_connections | max connections for source database                                                                                                  | 10                                                             | currently 10, may be dynamically adjusted in the future |
| batch_size      | number of records written in a batch, 1 for serial                                                                                   | 200                                                            | 200                                                     |
| replace         | when inserting data, whether to force replacement if data already exists in target database, used in snapshot/cdc tasks for MySQL/PG | false                                                          | true                                                    |

# [filter]

| Config           | Description                                                          | Example                                                                                                                              | Default |
| :--------------- | :------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :------ |
| do_dbs           | databases to be synced, takes union with do_tbs                      | db_1,db_2*,\`db*&#\`                                                                                                                 | -       |
| ignore_dbs       | databases to be filtered, takes union with ignore_tbs                | db_1,db_2*,\`db*&#\`                                                                                                                 | -       |
| do_tbs           | tables to be synced, takes union with do_dbs                         | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -       |
| ignore_tbs       | tables to be filtered, takes union with ignore_dbs                   | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -       |
| ignore_cols      | table columns to be filtered                                         | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -       |
| do_events        | events to be synced                                                  | insert,update,delete                                                                                                                 | -       |
| do_ddls          | ddls to be synced, for mysql cdc tasks                               | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -       |
| do_structures    | structures to be migrated, for mysql/pg structure migration tasks    | database,table,constraint,sequence,comment,index                                                                                     | \*      |
| ignore_cmds      | commands to be filtered, for redis cdc tasks                         | flushall,flushdb                                                                                                                     | -       |
| where_conditions | where conditions for the source SELECT SQL during snapshot migration | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -       |

## Values

- All configurations support multiple items, which are separated by ",". Example: do_dbs=db_1,db_2.
- Set to \* to match all. Example: do_dbs=\*.
- Keep empty to match nothing. Example: ignore_dbs=.
- ignore_cols and where_conditions are in JSON format, it should starts with "json:".
- do_events takes one or more values from **insert**, **update**, and **delete**.

## Priority

- ignore_tbs + ignore_tbs > do_tbs + do_dbs.
- If a table matches both **ignore** configs and **do** configs, the table will be filtered.
- If both do_tbs and do_dbs are configured, **the filter is the union of both**. If both ignore_tbs and ignore_dbs are configured, **the filter is the union of both**.

## Wildcard

| Wildcard | Description                 |
| :------- | :-------------------------- |
| \*       | Matches multiple characters |
| ?        | Matches 0 or 1 characters   |

Used in: do_dbs, ignore_dbs, do_tbs, and ignore_tbs.

## Escapes

| Database | Before      | After               |
| :------- | :---------- | :------------------ |
| mysql    | db\*&#      | \`db\*&#\`          |
| mysql    | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg       | db\*&#      | "db\*&#"            |
| pg       | db*&#.tb*$# | "db*&#"."tb*$#"     |

Names should be enclosed in escape characters if there are special characters.

Used in: do_dbs, ignore_dbs, do_tbs and ignore_tbs.

# [router]

| Config    | Description                                                         | Example                                                                      | Default |
| :-------- | :------------------------------------------------------------------ | :--------------------------------------------------------------------------- | :------ |
| db_map    | database mapping                                                    | db_1:dst_db_1,db_2:dst_db_2                                                  | -       |
| tb_map    | table mapping                                                       | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2                      | -       |
| col_map   | column mapping                                                      | json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}] | -       |
| topic_map | table -> kafka topic mapping, for mysql/pg -> kafka tasks. required | \*.\*:default_topic,test_db_2.\*:topic2,test_db_2.tb_1:topic3                | -       |

## Values

- A mapping rule consists of the source and target, which are separated by ":".
- All configurations support multiple items, which are separated by ",". Example: db_map=db_1:dst_db_1,db_2:dst_db_2.
- col_map value is in JSON format, it should starts with "json:".
- If not set, data will be routed to the same databases/tables/columns with the source database.

## Priority

- tb_map > db_map.
- col_map only works for column mapping. If a table needs database + table + column mapping, tb_map/db_map must be set.
- topic_map: test_db_2.tb_1:topic3 > test_db_2.\*:topic2 > \*.\*:default_topic.

## Wildcard

Not supported.

## Escapes

Same with [filter].

# [pipeline]

| Config                   | Description                                                                                                                     | Example | Default                                       |
| :----------------------- | :------------------------------------------------------------------------------------------------------------------------------ | :------ | :-------------------------------------------- |
| buffer_size              | max cached records in memory                                                                                                    | 16000   | 16000                                         |
| buffer_memory_mb         | [optional] memory limit for buffer, if reached, new records will be blocked even if buffer_size is not reached, 0 means not set | 200     | 0                                             |
| checkpoint_interval_secs | interval to flush logs/statistics/position                                                                                      | 10      | 10                                            |
| max_rps                  | [optional] max synced records in a second                                                                                       | 1000    | -                                             |
| counter_time_window_secs | time window for monitor counters                                                                                                | 10      | same with [pipeline] checkpoint_interval_secs |

# [parallelizer]

| Config        | Description                  | Example  | Default |
| :------------ | :--------------------------- | :------- | :------ |
| parallel_type | parallel type                | snapshot | serial  |
| parallel_size | threads for parallel syncing | 8        | 1       |

## parallel_type

| Type      | Strategy                                                                                                                                                                                      | Usage                             | Advantages | Disadvantages        |
| :-------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------- | :--------- | :------------------- |
| snapshot  | Records in cache are divided into [parallel_size] partitions, and each partition will be synced in batches in a separate thread.                                                              | snapshot tasks for mysql/pg/mongo | fast       |                      |
| serial    | Single thread, one by one.                                                                                                                                                                    | all                               |            | slow                 |
| rdb_merge | Merge CDC records(insert, update, delete) in cache into insert + delete records，and then divide them into [parallel_size] partitions, each partition synced in batches in a separate thread. | CDC tasks for mysql/pg            | fast       | eventual consistency |
| mongo     | Mongo version of rdb_merge.                                                                                                                                                                   | CDC tasks for mongo               |
| rdb_check | Similar to snapshot. But if the source table does not have primary/unique keys, records will be synced in serial.                                                                             | check tasks for mysql/pg/mongo    |
| redis     | Single thread, batch/serial writing(determined by [sinker] batch_size)                                                                                                                        | snapshot/CDC tasks for redis      |

# [runtime]

| Config      | Description        | Example                     | Default       |
| :---------- | :----------------- | :-------------------------- | :------------ |
| log_level   | level              | info/warn/error/debug/trace | info          |
| log4rs_file | log4rs config file | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir     | output dir         | ./logs                      | ./logs        |

Note that the log files contain progress information for the task, which can be used for task [resuming at breakpoint](/docs/en/snapshot/resume.md). Therefore, if you have multiple tasks, **please set up separate log directories for each task**.

# [global]

| Config  | Description            | Example    | Default |
| :------ | :--------------------- | :--------- | :------ |
| task_id | Unique task identifier | cdc_task_1 |         |

In some scenarios, task_id is used to distinguish task uniqueness, such as when using resumer from database. By default, it will be automatically generated based on key configuration information.

# [resumer]

| Config          | Description                                                                | Example                                     | Default                                |
| :-------------- | :------------------------------------------------------------------------- | :------------------------------------------ | :------------------------------------- |
| resume_type     | Type: [from_log;from_target;from_db]                                       | from_target                                 |                                        |
| log_dir         | Valid when resume_type is from_log, the log directory location             | ./logs                                      |                                        |
| url             | Valid when resume_type is from_db, database connection URL                 | mysql://xxx:xxx@127.0.0.1:3306              |                                        |
| db_type         | Valid when resume_type is from_db, database type                           | mysql                                       |                                        |
| table_full_name | Valid when resume_type is from_db or from_target, table name for recording | apecloud_metadata_test.apedts_task_position | apecloud_metadata.apedts_task_position |
| max_connections | Maximum connections for the resumer connection pool                        | 1                                           | 1                                      |

For details, please refer to the resumer documentation: [resuming at breakpoint](/docs/en/snapshot/resume.md).
