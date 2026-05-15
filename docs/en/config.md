# Config details

Different tasks may require extra configs, refer to [task templates](/docs/templates/) and [tutorial](/docs/en/tutorial/)

# Example: MySQL -> MySQL

# [extractor]

| Config          | Description                                                                                 | Example                                                                                              | Default                                                 |
| :-------------- | :------------------------------------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------- | :------------------------------------------------------ |
| db_type         | source database type                                                                        | mysql                                                                                                | -                                                       |
| extract_type    | snapshot, cdc                                                                               | snapshot                                                                                             | -                                                       |
| url             | database URL. You can specify the username and password directly in the URL.                | mysql://127.0.0.1:3307 or mysql://root:password@127.0.0.1:3307                                       |
| username        | database connection username                                                                | root                                                                                                 |
| password        | database connection password                                                                | password                                                                                             | -                                                       |
| max_connections | max connections for source database                                                         | 10                                                                                                   | currently 10, may be dynamically adjusted in the future |
| batch_size      | number of extracted records in a batch                                                      | 10000                                                                                                | same as [pipeline] buffer_size                          |
| parallel_size   | number of workers for extracting a table                                                    | 4                                                                                                    | 1                                                       |
| partition_cols  | partition column for data splitting during snapshot migration, only single column supported | json:[{"db":"db_1","tb":"tb_1","partition_col":"id"},{"db":"db_2","tb":"tb_2","partition_col":"id"}] | -                                                       |

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
| sink_type       | write, dummy                                                                                                                         | write                                                          | write                                                   |
| url             | database URL. You can specify the username and password directly in the URL.                                                         | mysql://127.0.0.1:3307 or mysql://root:password@127.0.0.1:3307 |
| username        | database connection username                                                                                                         | root                                                           |
| password        | database connection password                                                                                                         | password                                                       | -                                                       |
| max_connections | max connections for target database                                                                                                  | 10                                                             | currently 10, may be dynamically adjusted in the future |
| batch_size      | number of records written in a batch, 1 for serial                                                                                   | 200                                                            | 200                                                     |
| replace         | when inserting data, whether to force replacement if data already exists in target database, used in snapshot/cdc tasks for MySQL/PG | false                                                          | true                                                    |

# [checker]

The `[checker]` section is used by three documented data check flows:
- Standalone snapshot check: run a snapshot check task only (no data write). Set
  `sink_type=dummy` or omit `[sinker]`, and configure the checker target explicitly in
  `[checker]`. Standalone snapshot checker targets support MySQL, PostgreSQL, and MongoDB. This
  flow is data-only and does not run structure check automatically.
- Inline snapshot check: for snapshot tasks with `sink_type=write`, the checker runs after sink
  and reuses the parsed `[sinker]` target directly.
- Inline cdc check: for CDC tasks with `extract_type=cdc` and `sink_type=write`, the checker
  validates applied changes after write, reuses the parsed `[sinker]` target, and requires
  resumer state persistence.

Struct check is supported only for standalone MySQL/PostgreSQL checker targets.

| Config                      | Description                                                            | Example     | Default                           |
| :-------------------------- | :--------------------------------------------------------------------- | :---------- | :-------------------------------- |
| enable                      | whether to enable the checker when `[checker]` section is present      | true        | required                          |
| queue_size                  | checker queue capacity, counted in pending batches/messages            | 200         | 200                               |
| max_connections             | max connections for checker pool                                       | 8           | 8                                 |
| batch_size                  | checker chunk size; also used for checker chunking in inline cdc check | 200         | 200                               |
| sample_rate                 | percentage sample rate for snapshot and CDC checks                     | 25          | empty (check all rows/changes)    |
| output_full_row             | output full row in diff log                                            | false       | false                             |
| output_revise_sql           | write generated revise SQL to `sql.log`                                | false       | false                             |
| revise_match_full_row       | match full row when building revise SQL                                | false       | false                             |
| retry_interval_secs         | retry interval in seconds (forced to 0 in inline cdc check)            | 0           | 0                                 |
| max_retries                 | retry count (forced to 0 in inline cdc check)                          | 0           | 0                                 |
| check_log_dir               | check log dir                                                          | /tmp/check  | empty (use runtime.log_dir/check) |
| check_log_file_size         | local per-log file size limit (`diff.log` / `miss.log` / `sql.log`)    | 100mb       | 100mb                             |
| check_log_max_rows          | CDC check snapshot max rows (`diff.log` / `miss.log`)                  | 1000        | 1000                              |
| db_type                     | checker target db type (standalone target only)                        | mysql       | -                                 |
| url                         | checker target URL (standalone target only)                            | mysql://... | -                                 |
| username                    | checker target username (standalone target only)                       | root        | empty                             |
| password                    | checker target password (standalone target only)                       | password    | empty                             |
| check_log_s3                | upload check logs to S3 for standalone snapshot or inline CDC check    | false       | false                             |
| cdc_check_log_interval_secs | interval (seconds) for periodic CDC check snapshot output              | 10          | 10                                |
| s3_bucket                   | S3 bucket for check log upload                                         | my-bucket   | -                                 |
| s3_access_key_id            | S3 access key id                                                       | AKIA...     | -                                 |
| s3_secret_access_key        | S3 secret access key                                                   | ****        | -                                 |
| s3_region                   | S3 region                                                              | us-east-1   | -                                 |
| s3_endpoint                 | S3 endpoint                                                            | https://... | -                                 |
| s3_key_prefix               | S3 key prefix for check logs                                           | task1/check | empty                             |

Notes:

**General behavior**
- Checker only supports `[pipeline] pipeline_type=basic`.
- `sample_rate` only supports snapshot check and inline CDC check. Valid values are `1..=100`; an
  empty value means all rows/changes are checked. Standalone MySQL/PostgreSQL/MongoDB snapshot check
  applies it during extraction by row position inside each extractor stream, so later checker work
  receives fewer rows. MySQL/PostgreSQL parallel chunks are sampled independently, and the nullable
  order-column NULL pass has its own counter; MongoDB uses the collection cursor position. For
  example, `sample_rate=25` keeps positions 1-25 in every 100-position window of each stream. After
  resume, standalone snapshot check restarts those position counters from the resumed stream, so the
  sampled row set can differ from an uninterrupted run if the previous run stopped between sampled
  rows. Inline
  snapshot check and inline CDC check write all rows/changes first, then apply deterministic
  checker-side key-hash sampling before target fetch, so rows/changes with the same key are sampled
  consistently.
- `queue_size` counts queued checker DML batches, not rows. Control signals such as checkpoint and
  `refresh_meta` bypass this queue.
- In inline write-after-check flows, if the checker DML queue is full, the oldest pending batch is
  dropped with a warning log instead of blocking the write path.
- Checker runtime errors (batch check failure, checkpoint failure, output failure) are logged but do
  not affect the main CDC write path. Checkpoint and meta refresh delivery remain best-effort.

**Flow selection and target rules**
- For inline write-after-check flows, one queued batch is usually close to the effective sink batch
  size. In practice this is often about `[sinker].batch_size` rows, but the final batch may be
  smaller and upstream partitioning can also change the actual count.
- For standalone / dummy-sinker check flows, queued batch size is decided by the upstream
  parallelizer. After dequeue, the checker processes non-CDC rows in chunks of `[checker].batch_size`.
- Struct tasks only support standalone MySQL/PostgreSQL checker targets. If `[checker]` is enabled
  for struct tasks, use `sink_type=dummy` or omit `[sinker]`. Run structure check explicitly when
  structure verification is needed; standalone snapshot check does not start it automatically.
- Inline snapshot check is supported only when `[extractor] extract_type=snapshot`,
  `[sinker] sink_type=write`, and `[sinker].db_type` is `mysql`, `pg`, or `mongo`.
- Inline cdc check is currently supported only when `[extractor] extract_type=cdc`,
  `[sinker] sink_type=write`, `[checker].enable=true`, `[parallelizer].parallel_type=rdb_merge`,
  and `[sinker].db_type` is `mysql` or `pg`.
- In inline cdc check, the checker uses `[checker].batch_size`. It does not fall back to
  `[sinker].batch_size`. For example, if `[checker].batch_size=100` and `queue_size=200`, the
  checker queue can hold about 200 pending batches, which is roughly 20,000 rows when batches are full.
- In inline snapshot check and inline cdc check, `[checker]` must not set `db_type`, `url`,
  `username`, or `password`; the checker always reuses the parsed `[sinker]` target.
- In inline cdc check, `[resumer] resume_type=from_target` or `from_db` is required to persist
  checker state.
- In inline cdc check, the following combinations fail fast with `ConfigError`: `[checker]`
  section present without `enable`; `[pipeline].pipeline_type != basic`; `[sinker].sink_type != write`;
  `[parallelizer].parallel_type != rdb_merge`; `[sinker].db_type` not in `mysql` / `pg`; or any
  target field (`db_type` / `url` / `username` / `password`) set under `[checker]`.

**Inline cdc check log / retry behavior**
- In inline cdc check, `[checker].max_retries` / `[checker].retry_interval_secs` are forced to `0`.
- When `check_log_dir` is empty, `runtime.log_dir/check` is used consistently for checker logs (including CDC check outputs).
- Standalone snapshot check writes check results locally first. If `check_log_s3=true`, the final
  local `summary.log` plus non-empty `miss.log`, `diff.log`, and `sql.log` are uploaded to S3
  after the check task finishes.
- In inline cdc check, periodic check snapshots are always written locally under `check_log_dir`;
  `check_log_s3` controls only S3 upload. Outside inline cdc check, S3 upload is supported only by
  standalone snapshot check.
- `check_log_file_size` limits local `diff.log` / `miss.log` / `sql.log`. `summary.log` is not
  size-limited.
- `check_log_max_rows` only applies to CDC check snapshots for `diff.log` / `miss.log`; when either
  threshold is hit, only the latest records are kept.

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
- `ignore_cols` and `where_conditions` are in JSON format, it should starts with "json:".
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

| Type      | Strategy                                                                                                                                                                                                                                                                      | Usage                               | Advantages | Disadvantages        |
| :-------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------- | :--------- | :------------------- |
| snapshot  | Records in cache are divided into [parallel_size] partitions, and each partition will be synced in batches in a separate thread.                                                                                                                                              | snapshot tasks for mysql/pg/mongo   | fast       |                      |
| serial    | Single thread, one by one.                                                                                                                                                                                                                                                    | all                                 |            | slow                 |
| rdb_merge | Merge row changes in cache into write-friendly insert + delete batches, then divide them into [parallel_size] partitions for parallel syncing. When `[checker].enable=true`, checker-enabled MySQL/PG flows reuse this parallelizer and switch to check sink mode internally. | mysql/pg CDC, check, review, revise | fast       | eventual consistency |
| mongo     | Mongo version of merge parallelization. When `[checker].enable=true`, checker-enabled Mongo flows reuse this parallelizer and switch to check sink mode internally.                                                                                                           | mongo CDC, check, review            |
| redis     | Single thread, batch/serial writing(determined by [sinker] batch_size)                                                                                                                                                                                                        | snapshot/CDC tasks for redis        |

# [runtime]

| Config      | Description        | Example                     | Default       |
| :---------- | :----------------- | :-------------------------- | :------------ |
| log_level   | level              | info/warn/error/debug/trace | info          |
| log4rs_file | log4rs config file | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir     | output dir         | ./logs                      | ./logs        |
| check_result_stdout_only | output only check result logs to stdout | true/false | false |

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
