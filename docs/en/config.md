# Config details

Different tasks may require extra configs, refer to [task templates](/docs/templates/) and [tutorial](/docs/en/tutorial/)

For configuration changes between releases, see [Config changelog](/docs/en/config_changelog.md).

# [extractor]

| Config               | Description                                                                                                            | Example                                                                                              | Default                                                                                                          |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| db_type              | source database type                                                                                                   | mysql                                                                                                | required                                                                                                         |
| extract_type         | extraction type; available values depend on `db_type`                                                                  | snapshot                                                                                             | required                                                                                                         |
| url                  | database URL; credentials may be included in the URL or configured separately                                          | `mysql://127.0.0.1:3307`                                                                             | empty                                                                                                            |
| username             | database connection username                                                                                           | root                                                                                                 | empty                                                                                                            |
| password             | database connection password                                                                                           | password                                                                                             | empty                                                                                                            |
| ssl_mode             | MySQL/PostgreSQL TLS mode: `disable`, `require`, `verify_ca`, or `verify_full`                                         | verify_full                                                                                          | not set                                                                                                          |
| ssl_ca_path          | CA certificate path used by TLS verification                                                                           | /etc/ssl/certs/ca.pem                                                                                | empty                                                                                                            |
| max_connections      | maximum source connection pool size                                                                                    | 10                                                                                                   | 10                                                                                                               |
| batch_size           | number of rows extracted per batch; if using chunk splitting, this is also the target chunk size for the source        | 10000                                                                                                | `[pipeline].buffer_size / effective snapshot parallel_size`. If set to 0, uses `[pipeline].buffer_size` directly |
| max_rps              | optional source-side rate limit in records per second; `0` disables the limit                                          | 1000                                                                                                 | 0                                                                                                                |
| max_mbps             | optional source-side rate limit in MiB per second; `0` disables the limit                                              | 100                                                                                                  | 0                                                                                                                |
| app_name             | connection application name, currently used by MongoDB                                                                 | APE_DTS                                                                                              | APE_DTS                                                                                                          |
| parallel_type        | snapshot extraction parallel strategy                                                                                  | table                                                                                                | table                                                                                                            |
| parallel_size        | source snapshot worker limit                                                                                           | 4                                                                                                    | 1; legacy fallback: `[runtime].tb_parallel_size`                                                                 |
| partition_cols       | partition column for data splitting during MySQL/PostgreSQL snapshot migration; only one column per table is supported | json:[{"db":"db_1","tb":"tb_1","partition_col":"id"},{"db":"db_2","tb":"tb_2","partition_col":"id"}] | empty                                                                                                            |
| is_direct_connection | MongoDB driver `directConnection` option                                                                               | true                                                                                                 | not set (driver default)                                                                                         |
| is_cluster           | Redis Cluster mode for snapshot/CDC/snapshot-and-CDC                                                                   | true                                                                                                 | not set or empty (detect from the connected Redis node)                                                          |

## URL escaping

- If the username/password contains special characters, the corresponding parts need to be percent-encoded, for example:

```
create user user1@'%' identified by 'abc%$#?@';
The url should be:
url=mysql://user1:abc%25%24%23%3F%40@127.0.0.1:3307?ssl-mode=disabled
```

Credentials configured through `username` and `password` are percent-encoded and merged into the URL
by DTS. If `ssl_mode` is set, `ssl_ca_path` is optional unless the selected verification mode and
server setup require a CA certificate.

## extractor.parallel_type

- `table`: allocate snapshot concurrency across tables. With `parallel_size=4`, up to 4 tables can be extracted at the same time.
- `chunk`: allocate snapshot concurrency within a single table by chunk splitting. With `parallel_size=4`, one table can run up to 4 chunk workers in parallel.
- When `parallel_type=chunk`, `[extractor].batch_size` is also the target chunk size. Chunk boundaries are data-dependent, so the actual row count may differ, but the extractor tries to make each chunk close to `batch_size`.
- `parallel_size` is the effective concurrency limit in both modes.
- MySQL and PostgreSQL snapshot extractors support both `table` and `chunk`.
- MongoDB snapshot extractors currently support only `table`; `chunk` is not supported.
- Deprecated compatibility: `[runtime] tb_parallel_size` is kept only as a legacy fallback when `[extractor] parallel_size` is not set.

## Redis source cluster mode

- `[extractor].url` can point to any reachable node in the source cluster. DTS discovers all source master nodes through `CLUSTER NODES` and starts one PSYNC extractor for each master.
- `[extractor].is_cluster` is optional. When omitted, DTS connects to the Redis node specified by `[extractor].url` and detects whether Redis Cluster mode should be used from the node's actual cluster state.
- Set `[extractor].is_cluster=true` to force Redis Cluster mode. DTS discovers and syncs the whole source cluster.
- Set `[extractor].is_cluster=false` to force single-node Redis mode. DTS runs PSYNC only against the node specified by `[extractor].url`. This can be used when the source is a Redis Cluster but only one cluster node should be synced.

## Mongo source connection mode

- `[extractor].is_direct_connection` maps to the MongoDB driver `directConnection` option.
- Omit it to let the driver infer the topology from the URL. This is the recommended default for
  replica sets and sharded clusters.
- Set it only when you intentionally want to connect directly to a specific MongoDB node. Do not set
  it to `true` when connecting through `mongos` for sharded-cluster CDC or snapshot tasks.

# [sinker]

| Config                         | Description                                                                                                                    | Example                  | Default                                                                 |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------ | ----------------------------------------------------------------------- |
| db_type                        | target database type                                                                                                           | mysql                    | required except for `sink_type=dummy`                                   |
| sink_type                      | target operation; supported values depend on `db_type`                                                                         | write                    | write when `[sinker]` exists; dummy when omitted for standalone checker |
| url                            | database URL; credentials may be included in the URL or configured separately                                                  | `mysql://127.0.0.1:3307` | empty                                                                   |
| username                       | database connection username                                                                                                   | root                     | empty                                                                   |
| password                       | database connection password                                                                                                   | password                 | empty                                                                   |
| ssl_mode                       | MySQL/PostgreSQL TLS mode: `disable`, `require`, `verify_ca`, or `verify_full`                                                 | verify_full              | not set                                                                 |
| ssl_ca_path                    | CA certificate path used by TLS verification                                                                                   | /etc/ssl/certs/ca.pem    | empty                                                                   |
| max_connections                | maximum target connection pool size                                                                                            | 10                       | 10                                                                      |
| batch_size                     | records written per batch; must be greater than `0`                                                                            | 200                      | 200                                                                     |
| max_rps                        | optional target-side rate limit in records per second; `0` disables the limit                                                  | 1000                     | 0                                                                       |
| max_mbps                       | optional target-side rate limit in MiB per second; `0` disables the limit                                                      | 100                      | 0                                                                       |
| replace                        | replace an existing row on insert conflict; supports MySQL/PostgreSQL snapshot and CDC tasks, and SQL Server snapshot tasks    | false                    | true                                                                    |
| disable_foreign_key_checks     | disable foreign-key checks while writing MySQL/PostgreSQL                                                                      | true                     | true                                                                    |
| transaction_isolation          | MySQL/TiDB target transaction isolation: `default`, `read_uncommitted`, `read_committed`, `repeatable_read`, or `serializable` | read_committed           | default                                                                 |
| conflict_policy                | structure migration conflict policy: `interrupt` or `ignore`                                                                   | interrupt                | interrupt                                                               |
| app_name                       | connection application name, currently used by MongoDB                                                                         | APE_DTS                  | APE_DTS                                                                 |
| is_direct_connection           | MongoDB driver `directConnection` option                                                                                       | true                     | not set (driver default)                                                |
| is_cluster                     | Redis Cluster mode                                                                                                             | true                     | not set or empty (detect from the connected Redis node)                 |
| mongo_require_shard_key_filter | fail fast when a MongoDB update/delete/upsert filter cannot contain the complete target shard key                              | true                     | true                                                                    |

## Redis target cluster mode

- `[sinker].url` can point to any reachable node in the target cluster. DTS discovers all target master nodes through `CLUSTER NODES` and routes Redis commands to the owning node by key slot.
- In Redis target cluster mode, DTS creates sinkers according to the target master nodes, instead of limiting the sinker count by `[parallelizer].parallel_size`.
- `[sinker].is_cluster` is optional. When omitted, DTS connects to the Redis node specified by `[sinker].url` and detects whether Redis Cluster mode should be used from the node's actual cluster state.
- Set `[sinker].is_cluster=true` to force Redis Cluster mode when writing to the target cluster.
- Set `[sinker].is_cluster=false` to force single-node Redis mode and write only to the node specified by `[sinker].url`.

## Mongo target connection and shard-key mode

- `[sinker].is_direct_connection` maps to the MongoDB driver `directConnection` option. Omit it to
  let the driver infer the topology from the URL. For sharded targets, connect through `mongos` and
  do not set it to `true`.
- `[sinker].mongo_require_shard_key_filter=true` is the default. When the target collection is
  sharded, DTS checks whether update/delete/upsert filters contain the full target shard key and
  fails fast if required shard key fields are missing.
- Keep `mongo_require_shard_key_filter=true` for normal migrations. Set it to `false` only when you
  explicitly accept MongoDB server-side routing behavior, such as a controlled best-effort migration
  on a compatible MongoDB version.

# [checker]

Common row/structure comparison settings. The section is used in these modes:

- Standalone snapshot/struct/check-log: set `[sinker].sink_type=check`. The target connection,
  authentication, TLS, connection limits, and database-specific options are all loaded from
  `[sinker]`.
- Inline snapshot: use `extract_type=snapshot`, `[sinker].sink_type=write`, and add a
  `[checker]` section. Checking runs synchronously after each successful sink operation.
- Inline CDC: use `extract_type=cdc`, `[sinker].sink_type=write`, and enable
  `[checker_cdc].is_enabled=true`. Checking runs asynchronously after sink through the CDC
  checker queue.

| Config                | Description                                      | Example | Default                        |
| --------------------- | ------------------------------------------------ | ------- | ------------------------------ |
| batch_size            | maximum rows processed by one checker query      | 200     | 200                            |
| sample_percent        | percentage sampled for snapshot/CDC checks       | 25      | empty (check every row/change) |
| recheck_count         | number of retries for a temporary inconsistency  | 4       | 0                              |
| recheck_interval_secs | interval between retries, in seconds             | 5       | 0                              |
| recheck_queue_size    | maximum pending rows in the retry buffer         | 10000   | 10000                          |
| recheck_queue_memory_mb | retry-buffer memory limit in MiB               | 256     | 256                            |

Notes:

- Checker tasks support only `[pipeline].pipeline_type=basic`.
- `sample_percent` accepts `1..=100` and applies only to snapshot checks and inline CDC checks.
  Standalone snapshot applies sampling during extraction. Inline snapshot/CDC writes every
  row/change and applies deterministic key-hash sampling before target fetch.
- Standalone snapshot check supports MySQL, PostgreSQL, and MongoDB targets. Standalone struct check
  supports MySQL and PostgreSQL.
- Inline snapshot check supports MySQL, PostgreSQL, and MongoDB write targets.
- `recheck_count` and `recheck_interval_secs` are not used by inline CDC reconciliation.
- When either retry-buffer limit is reached, the checker does not drop the result; newly found
  inconsistencies skip retry and are finalized immediately.

## Standalone target example

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://source-host:3306

[sinker]
db_type=mysql
sink_type=check
url=mysql://target-host:3306
username=root
password=target-password
max_connections=8

[checker]
batch_size=200
sample_percent=25
recheck_count=4
recheck_interval_secs=5
recheck_queue_size=10000
recheck_queue_memory_mb=256
```

# [checker_output]

Check-result output configuration. If this section is omitted, results are written as local logs
under `runtime.log_dir/check`.

| Config               | Description                                                         | Example       | Default |
| -------------------- | ------------------------------------------------------------------- | ------------- | ------- |
| output_type          | result destination: `logs` or `s3`                              | logs          | logs    |
| output_full_row      | include complete source/target rows in difference logs              | false         | false   |
| output_revise_sql    | generate repair statements in `sql.log`                           | true          | false   |
| revise_match_full_row| use the complete row in generated repair predicates                 | false         | false   |
| check_log_dir        | local check-log directory                                           | /tmp/check    | empty (use `runtime.log_dir/check`) |
| check_log_file_size  | per-file size limit for `diff.log`, `miss.log`, and `sql.log` | 100mb         | 100mb   |
| check_log_max_rows   | maximum rows in `diff.log`/`miss.log`                            | 1000          | 1000    |
| s3_bucket            | S3 bucket; required for `output_type=s3`                           | my-bucket     | -       |
| s3_access_key_id     | S3 access key                                                       | AKIA...       | empty   |
| s3_secret_access_key | S3 secret key                                                       | ****          | empty   |
| s3_region            | S3 region                                                           | us-east-1     | empty   |
| s3_endpoint          | custom S3 endpoint                                                   | https://...   | empty   |
| s3_root_dir          | local/mounted root used by the S3 helper                            | /tmp/check    | empty   |
| s3_root_url          | root URL used by the S3 helper                                      | s3://bucket   | empty   |
| s3_key_prefix        | key prefix for uploaded check logs                                  | task1/check   | empty   |

`output_type=s3` is supported for standalone snapshot check and inline CDC check. S3 output still
uses the configured local rolling-log directory and limits before upload. Structure check, check-log
review, and inline snapshot check support `output_type=logs` only.

# [checker_cdc]

CDC-only asynchronous checker settings.

| Config                  | Description                                          | Example | Default |
| ----------------------- | ---------------------------------------------------- | ------- | ------- |
| is_enabled              | enable inline CDC check                              | true    | false   |
| queue_size              | pending CDC checker batches                          | 200     | 200     |
| check_log_interval_secs | periodic CDC check-result output interval in seconds | 30      | 30      |

Inline CDC check additionally requires:

- `[extractor].extract_type=cdc`
- `[sinker].sink_type=write` with a MySQL/PostgreSQL target
- `[parallelizer].parallel_type=rdb_merge`
- `[resumer].resume_type=from_target` or `from_db`

The CDC checker queue is deliberately decoupled from the migration pipeline. When full, it evicts
the oldest pending batch instead of blocking writes. Checker processing/output failures are logged
without failing the main CDC write path. Snapshot and structure checks do not use this queue.

# [filter]

| Config           | Description                                                          | Example                                                                                                                              | Default |
| ---------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| do_dbs           | databases to be synced, takes union with do_tbs                      | db_1,db_2*,db*&#                                                                                                                     | -       |
| ignore_dbs       | databases to be filtered, takes union with ignore_tbs                | db_1,db_2*,db*&#                                                                                                                     | -       |
| do_tbs           | tables to be synced, takes union with do_dbs                         | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_tbs       | tables to be filtered, takes union with ignore_dbs                   | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_cols      | table columns to be filtered                                         | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -       |
| do_events        | events to be synced                                                  | insert,update,delete                                                                                                                 | \*      |
| do_ddls          | ddls to be synced, for mysql cdc tasks                               | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -       |
| do_dcls          | DCL statements to be synced, for supported structure tasks           | create_user,grant                                                                                                                    | -       |
| do_structures    | structures to be migrated in structure migration tasks               | mysql/pg: database,table,constraint,sequence,comment,index; mongo: collection,shardkey                                               | \*      |
| ignore_cmds      | commands to be filtered, for redis cdc tasks                         | flushall,flushdb                                                                                                                     | -       |
| where_conditions | where conditions for the source SELECT SQL during snapshot migration | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -       |

## Values

- All configurations support multiple items, which are separated by ",". Example: do_dbs=db_1,db_2.
- Set to `*` to match all. Example: `do_dbs=*`.
- Keep empty to match nothing. Example: ignore_dbs=.
- `ignore_cols` and `where_conditions` are in JSON format and must start with `json:`.
- do_events takes one or more values from **insert**, **update**, and **delete**.
- do_dcls takes one or more values from **create_user**, **alter_user**, **create_role**,
  **drop_user**, **drop_role**, **grant**, **revoke**, and **set_role**.
- `do_structures` takes structure object types. For MySQL/PostgreSQL, common values include
  **database**, **table**, **constraint**, **sequence**, **comment**, and **index**. For MongoDB,
  supported values are **collection**, **shardkey**. MongoDB does not use a separate
  **database** structure type; databases are created implicitly by creating collections. **shardkey**
  copies source sharding definitions for sharded collections and runs only when the target is
  connected through `mongos`.

## Priority

- ignore_tbs + ignore_dbs > do_tbs + do_dbs.
- If a table matches both **ignore** configs and **do** configs, the table will be filtered.
- If both do_tbs and do_dbs are configured, **the filter is the union of both**. If both ignore_tbs and ignore_dbs are configured, **the filter is the union of both**.

## Wildcard

| Wildcard | Description                 |
| -------- | --------------------------- |
| \*       | Matches multiple characters |
| ?        | Matches 0 or 1 characters   |

Used in: do_dbs, ignore_dbs, do_tbs, and ignore_tbs.

## Escapes

| Database | Before      | After               |
| -------- | ----------- | ------------------- |
| mysql    | db\*&#      | \`db\*&#\`          |
| mysql    | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg       | db\*&#      | "db\*&#"            |
| pg       | db*&#.tb*$# | "db\*&#"."tb\*$#"   |

Names should be enclosed in escape characters if there are special characters.

Used in: do_dbs, ignore_dbs, do_tbs and ignore_tbs.

# [router]

| Config    | Description                                                         | Example                                                                      | Default |
| --------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------- |
| db_map    | database mapping                                                    | db_1:dst_db_1,db_2:dst_db_2                                                  | -       |
| tb_map    | table mapping                                                       | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2                      | -       |
| col_map   | column mapping                                                      | json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}] | -       |
| topic_map | table -> kafka topic mapping, for mysql/pg -> kafka tasks. required | \*.\*:default_topic,test_db_2.\*:topic2,test_db_2.tb_1:topic3                | -       |

## Values

- A mapping rule consists of the source and target, which are separated by ":".
- All configurations support multiple items, which are separated by ",". Example: db_map=db_1:dst_db_1,db_2:dst_db_2.
- col_map value is in JSON format and must start with `json:`.
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
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ------- | --------------------------------------------- |
| buffer_size              | max cached records in memory                                                                                                    | 16000   | 16000                                         |
| buffer_memory_mb         | [optional] memory limit for buffer, if reached, new records will be blocked even if buffer_size is not reached, 0 means not set | 200     | 0                                             |
| checkpoint_interval_secs | interval to flush logs/statistics/position                                                                                      | 10      | 10                                            |
| batch_sink_interval_secs | maximum interval before flushing a non-empty sink batch                                                                         | 1       | 0                                             |
| counter_time_window_secs | time window for monitor counters                                                                                                | 10      | same with [pipeline] checkpoint_interval_secs |
| counter_max_sub_count    | maximum number of sub-counters                                                                                                  | 1000    | 1000                                          |
| pipeline_type            | pipeline implementation; only `basic` is supported                                                                              | basic   | basic                                         |

# [parallelizer]

| Config                              | Description                                               | Example  | Default             |
| ----------------------------------- | --------------------------------------------------------- | -------- | ------------------- |
| parallel_type                       | parallel type                                             | snapshot | serial              |
| parallel_size                       | threads for parallel syncing                              | 8        | 1                   |
| rebalance_strategy                  | snapshot chunk rebalance strategy used during sink writes | none     | none                |
| rebalance_cost                      | cost metric used to measure partition size                | rows     | rows                |
| rebalance_max_partitions_per_sinker | max split partitions per effective sinker                 | 2        | 2                   |
| rebalance_min_partition_rows        | minimum rows kept in each split snapshot insert partition | 200      | [sinker].batch_size |
| rebalance_split_skew_ratio          | skew threshold used by the auto_split strategy            | 1.0      | 1.0                 |

## parallel_type

| Type      | Strategy                                                                                                                                                                                                                                                                      | Usage                               | Advantages | Disadvantages        |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | ---------- | -------------------- |
| snapshot  | Records in cache are divided into [parallel_size] partitions, and each partition will be synced in batches in a separate thread.                                                                                                                                              | snapshot tasks for mysql/pg/mongo   | fast       |                      |
| serial    | Single thread, one by one.                                                                                                                                                                                                                                                    | all                                 |            | slow                 |
| rdb_merge | Merge row changes in cache into write-friendly insert + delete batches, then divide them into [parallel_size] partitions for parallel syncing. It is used by MySQL/PG CDC, check, review, and revise flows. | mysql/pg CDC, check, review, revise | fast       | eventual consistency |
| mongo     | Mongo version of merge parallelization, also used by standalone MongoDB check and review flows.                                                                                                           | mongo CDC, check, review            |            |                      |
| redis     | Single thread, batch/serial writing(determined by [sinker] batch_size)                                                                                                                                                                                                        | snapshot/CDC tasks for redis        |            |                      |

## snapshot chunk rebalance

When `[parallelizer].parallel_type=snapshot`, snapshot parallelizer uses chunk partitioner to rebalance the downstream write queue. It is mainly for snapshot write tasks and reduces sink-side long tails. It does not change source-side extractor concurrency and does not rewrite checkpoint chunk ids.

Default behavior:

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
rebalance_cost=rows
```

The default `rebalance_strategy=none` keeps logical chunk order after grouping and does not add sink-side sorting or splitting. If sink-side long tails are obvious, use `rebalance_strategy=auto_split`. Use `table_min_rows` or `table_even` for rows-only table-level partitioning. Use the default `rebalance_cost=rows` when row width is similar. If rows contain large JSON, LOB, or wide strings, use `rebalance_cost=bytes`. If the target has high request overhead, or you do not want to split logical chunks, use `rebalance_strategy=chunk_largest_first`.

For scenario-based tuning, see [Snapshot Chunk Partitioner Rebalance](/docs/en/snapshot/chunk_partitioner_rebalance.md).

# [runtime]

| Config                   | Description                             | Example                     | Default       |
| ------------------------ | --------------------------------------- | --------------------------- | ------------- |
| log_level                | level                                   | info/warn/error/debug/trace | info          |
| log4rs_file              | log4rs config file                      | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir                  | output dir                              | ./logs                      | ./logs        |
| check_result_stdout_only | output only check result logs to stdout | true/false                  | false         |

Note that the log files contain progress information for the task, which can be used for task [resuming at breakpoint](/docs/en/snapshot/resume.md). Therefore, if you have multiple tasks, **please set up separate log directories for each task**.

# [global]

| Config  | Description            | Example    | Default |
| ------- | ---------------------- | ---------- | ------- |
| task_id | Unique task identifier | cdc_task_1 |         |

In some scenarios, task_id is used to distinguish task uniqueness, such as when using resumer from database. By default, it will be automatically generated based on key configuration information.

# [resumer]

| Config               | Description                                                            | Example                                | Default                |
| -------------------- | ---------------------------------------------------------------------- | -------------------------------------- | ---------------------- |
| resume_type          | `dummy`, `from_log`, `from_target`, or `from_db`                       | from_target                            | dummy                  |
| log_dir              | log directory used by `from_log`                                       | ./logs                                 | `[runtime].log_dir`    |
| config_file          | optional resume config file used by `from_log`                         | ./resume.config                        | empty                  |
| url                  | database URL used by `from_db`                                         | `mysql://127.0.0.1:3306`               | required for `from_db` |
| db_type              | database type used by `from_db`                                        | mysql                                  | required for `from_db` |
| username             | database username used by `from_db`                                    | root                                   | empty                  |
| password             | database password used by `from_db`                                    | password                               | empty                  |
| ssl_mode             | MySQL/PostgreSQL TLS mode used by `from_db`                            | verify_full                            | not set                |
| ssl_ca_path          | CA certificate path used by `from_db`                                  | /etc/ssl/certs/ca.pem                  | empty                  |
| is_direct_connection | MongoDB driver `directConnection` option used by `from_db`             | true                                   | not set                |
| table_full_name      | target table used to store resume state for `from_db` or `from_target` | apecloud_metadata.apedts_task_position | empty                  |
| max_connections      | maximum resumer connection pool size                                   | 5                                      | 5                      |

For details, please refer to the resumer documentation: [resuming at breakpoint](/docs/en/snapshot/resume.md).

`resume_type=from_target` reuses the parsed sinker target. For a standalone checker with a dummy or
omitted sinker, it reuses the checker target. The legacy keys `resume_from_log`, `resume_log_dir`,
and `resume_config_file` are rejected; migrate them to `resume_type=from_log`, `log_dir`, and
`config_file`.

# [tracing]

| Config            | Description                                | Example | Default |
| ----------------- | ------------------------------------------ | ------- | ------- |
| task_summary_mode | trace aggregation mode: `task` or `marker` | marker  | marker  |
| output_format     | trace output format: `plain` or `json`     | json    | plain   |

The runtime trace summary is dumped periodically (every `pipeline.checkpoint_interval_secs`)
to the runtime trace log, including a final dump on task shutdown, so long-running CDC tasks
get continuous diagnostics. When both `metrics` and `tracing` features are enabled, per-marker task
counters and globally aggregated per-wait-point counters (`runtime_trace_*`) are also exposed on the
Prometheus `/metrics` endpoint.
In `task` summary mode, completed task details are emitted once on the next dump and then released;
marker summaries and Prometheus counters remain cumulative.

# [metacenter]

This optional section is used by the MySQL `dbengine` metadata-center mode.

| Config              | Description                                               | Example                  | Default   |
| ------------------- | --------------------------------------------------------- | ------------------------ | --------- |
| type                | metadata-center type: `basic` or `dbengine`               | dbengine                 | basic     |
| url                 | metadata database URL; required for MySQL `dbengine` mode | `mysql://127.0.0.1:3306` | required  |
| username            | metadata database username                                | root                     | empty     |
| password            | metadata database password                                | password                 | empty     |
| ssl_mode            | MySQL TLS mode                                            | verify_full              | not set   |
| ssl_ca_path         | CA certificate path                                       | /etc/ssl/certs/ca.pem    | empty     |
| ddl_conflict_policy | DDL conflict policy: `interrupt` or `ignore`              | interrupt                | interrupt |

The metadata-center URL must differ from both the extractor URL and the effective destination URL.

# [data_marker]

If this section is present, the required topology marker configuration is loaded.

| Config       | Description        | Default  |
| ------------ | ------------------ | -------- |
| topo_name    | topology name      | required |
| topo_nodes   | topology node list | empty    |
| src_node     | source node        | required |
| dst_node     | destination node   | required |
| do_nodes     | included nodes     | required |
| ignore_nodes | excluded nodes     | empty    |
| marker       | marker value       | required |

# [processor]

| Config        | Description                             | Default |
| ------------- | --------------------------------------- | ------- |
| lua_code_file | Lua processor source file loaded by DTS | empty   |

# [metrics]

This section is available only when DTS is built with the `metrics` feature.

| Config    | Description                               | Example       | Default |
| --------- | ----------------------------------------- | ------------- | ------- |
| http_host | metrics HTTP bind address                 | 0.0.0.0       | 0.0.0.0 |
| http_port | metrics HTTP port                         | 9090          | 9090    |
| workers   | metrics HTTP worker count                 | 2             | 2       |
| labels    | comma-separated `key=value` metric labels | env=prod,az=a | empty   |
