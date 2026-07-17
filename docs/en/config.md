# Config details

Different tasks may require extra configs, refer to [task templates](/docs/templates/) and [tutorial](/docs/en/tutorial/)

For configuration changes between releases, see [Config changelog](/docs/en/config_changelog.md).

# [extractor]

| Config               | Description                                                                                                                                                                    | Example                                                                                              | Default                                                        |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| db_type              | source database type                                                                                                                                                           | mysql                                                                                                | required                                                       |
| extract_type         | extraction type; available values depend on `db_type`                                                                                                                          | snapshot                                                                                             | required                                                       |
| url                  | database URL; credentials may be included in the URL or configured separately                                                                                                  | `mysql://127.0.0.1:3307`                                                                             | empty                                                          |
| username             | database connection username                                                                                                                                                   | root                                                                                                 | empty                                                          |
| password             | database connection password                                                                                                                                                   | password                                                                                             | empty                                                          |
| ssl_mode             | MySQL/PostgreSQL TLS mode: `disable`, `require`, `verify_ca`, or `verify_full`                                                                                                  | verify_full                                                                                          | not set                                                        |
| ssl_ca_path          | CA certificate path used by TLS verification                                                                                                                                  | /etc/ssl/certs/ca.pem                                                                                | empty                                                          |
| max_connections      | maximum source connection pool size                                                                                                                                            | 10                                                                                                   | 10                                                             |
| batch_size           | extracted records per batch; with chunk splitting, also the target source chunk size                                                                                           | 10000                                                                                                | `[pipeline].buffer_size / effective snapshot parallel_size`    |
| max_rps              | optional source-side rate limit in records per second; `0` disables the limit                                                                                                  | 1000                                                                                                 | 0                                                              |
| max_mbps             | optional source-side rate limit in MiB per second; `0` disables the limit                                                                                                      | 100                                                                                                  | 0                                                              |
| app_name             | connection application name, currently used by MongoDB                                                                                                                        | APE_DTS                                                                                              | APE_DTS                                                        |
| parallel_type        | snapshot extraction parallel strategy                                                                                                                                          | table                                                                                                | table                                                          |
| parallel_size        | source snapshot worker limit                                                                                                                                                   | 4                                                                                                    | 1; legacy fallback: `[runtime].tb_parallel_size`               |
| partition_cols       | partition column for data splitting during MySQL/PostgreSQL snapshot migration; only one column per table is supported                                                        | json:[{"db":"db_1","tb":"tb_1","partition_col":"id"},{"db":"db_2","tb":"tb_2","partition_col":"id"}] | empty                                                          |
| is_direct_connection | MongoDB driver `directConnection` option                                                                                                                                       | true                                                                                                 | not set (driver default)                                       |
| is_cluster           | Redis Cluster mode for snapshot/CDC/snapshot-and-CDC                                                                                                                           | true                                                                                                 | not set or empty (detect from the connected Redis node)        |

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

| Config                         | Description                                                                                                                                | Example                      | Default                                                 |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- | ------------------------------------------------------- |
| db_type                        | target database type                                                                                                                       | mysql                        | required except for `sink_type=dummy`                   |
| sink_type                      | target operation; supported values depend on `db_type`                                                                                     | write                        | write when `[sinker]` exists; dummy when omitted for standalone checker |
| url                            | database URL; credentials may be included in the URL or configured separately                                                             | `mysql://127.0.0.1:3307`     | empty                                                   |
| username                       | database connection username                                                                                                               | root                         | empty                                                   |
| password                       | database connection password                                                                                                               | password                     | empty                                                   |
| ssl_mode                       | MySQL/PostgreSQL TLS mode: `disable`, `require`, `verify_ca`, or `verify_full`                                                             | verify_full                  | not set                                                 |
| ssl_ca_path                    | CA certificate path used by TLS verification                                                                                              | /etc/ssl/certs/ca.pem        | empty                                                   |
| max_connections                | maximum target connection pool size                                                                                                        | 10                           | 10                                                      |
| batch_size                     | records written per batch; must be greater than `0`                                                                                        | 200                          | 200                                                     |
| max_rps                        | optional target-side rate limit in records per second; `0` disables the limit                                                             | 1000                         | 0                                                       |
| max_mbps                       | optional target-side rate limit in MiB per second; `0` disables the limit                                                                 | 100                          | 0                                                       |
| replace                        | replace an existing row on insert conflict, for MySQL/PostgreSQL snapshot and CDC tasks                                                    | false                        | true                                                    |
| disable_foreign_key_checks     | disable foreign-key checks while writing MySQL/PostgreSQL                                                                                  | true                         | true                                                    |
| transaction_isolation          | MySQL/TiDB target transaction isolation: `default`, `read_uncommitted`, `read_committed`, `repeatable_read`, or `serializable`             | read_committed               | default                                                 |
| conflict_policy                | structure migration conflict policy: `interrupt` or `ignore`                                                                              | interrupt                    | interrupt                                               |
| app_name                       | connection application name, currently used by MongoDB                                                                                    | APE_DTS                      | APE_DTS                                                 |
| is_direct_connection           | MongoDB driver `directConnection` option                                                                                                   | true                         | not set (driver default)                                |
| is_cluster                     | Redis Cluster mode                                                                                                                         | true                         | not set or empty (detect from the connected Redis node) |
| mongo_require_shard_key_filter | fail fast when a MongoDB update/delete/upsert filter cannot contain the complete target shard key                                          | true                         | true                                                    |

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
| --------------------------- | ---------------------------------------------------------------------- | ----------- | --------------------------------- |
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
| ssl_mode                    | checker target TLS mode (standalone target only)                       | verify_full | not set                           |
| ssl_ca_path                 | checker target CA certificate path (standalone target only)            | /ca.pem     | empty                             |
| check_log_s3                | upload check logs to S3 for standalone snapshot or inline CDC check    | false       | false                             |
| cdc_check_log_interval_secs | interval (seconds) for periodic CDC check snapshot output              | 30          | 30                                |
| s3_bucket                   | S3 bucket for check log upload                                         | my-bucket   | -                                 |
| s3_access_key_id            | S3 access key id                                                       | AKIA...     | -                                 |
| s3_secret_access_key        | S3 secret access key                                                   | \*\*\*\*    | -                                 |
| s3_region                   | S3 region                                                              | us-east-1   | -                                 |
| s3_endpoint                 | S3 endpoint                                                            | https://... | -                                 |
| s3_root_dir                 | local or mounted root directory used by the S3 helper                  | /tmp/check  | empty                             |
| s3_root_url                 | root URL used by the S3 helper                                         | s3://bucket | empty                             |
| s3_key_prefix               | S3 key prefix for check logs                                           | task1/check | empty                             |

Notes:

**General behavior**

- Checker only supports `[pipeline] pipeline_type=basic`.
- `sample_rate` only supports snapshot check and inline CDC check. Valid values are `1..=100`; an
  empty value means all rows/changes are checked. Standalone MySQL/PostgreSQL/MongoDB snapshot check
  applies it during extraction, so later checker work receives fewer rows. When row estimates are
  available, the extractor limits source reads to roughly `row_count * sample_rate / 100`.
  `row_count` is estimated from the table, or from the table's configured `where_conditions` when
  present. If no useful estimate is available, extraction reads the
  full source stream. This sampling is source-side Top-N limiting, not key-hash or random sampling.
  Inline snapshot check and inline CDC check write all rows/changes first, then apply deterministic
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
| ---------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| do_dbs           | databases to be synced, takes union with do_tbs                      | db_1,db_2*,db*&#                                                                                                                     | -       |
| ignore_dbs       | databases to be filtered, takes union with ignore_tbs                | db_1,db_2*,db*&#                                                                                                                     | -       |
| do_tbs           | tables to be synced, takes union with do_dbs                         | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_tbs       | tables to be filtered, takes union with ignore_dbs                   | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_cols      | table columns to be filtered                                         | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -       |
| do_events        | events to be synced                                                  | insert,update,delete                                                                                                                 | \*      |
| do_ddls          | ddls to be synced, for mysql cdc tasks                               | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -       |
| do_dcls          | DCL statements to be synced, for supported structure tasks          | create_user,grant                                                                                                                     | -       |
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

| Database | Before      | After           |
| -------- | ----------- | --------------- |
| mysql    | db\*&#      | \`db\*&#\`          |
| mysql    | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg       | db\*&#      | "db\*&#"            |
| pg       | db*&#.tb*$# | "db\*&#"."tb*$#"    |

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
| pipeline_type            | pipeline implementation; only `basic` is supported                                                                              | basic   | basic                                          |

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
| rdb_merge | Merge row changes in cache into write-friendly insert + delete batches, then divide them into [parallel_size] partitions for parallel syncing. When `[checker].enable=true`, checker-enabled MySQL/PG flows reuse this parallelizer and switch to check sink mode internally. | mysql/pg CDC, check, review, revise | fast       | eventual consistency |
| mongo     | Mongo version of merge parallelization. When `[checker].enable=true`, checker-enabled Mongo flows reuse this parallelizer and switch to check sink mode internally.                                                                                                           | mongo CDC, check, review            |            |                      |
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

| Config               | Description                                                                  | Example                                     | Default                  |
| -------------------- | ---------------------------------------------------------------------------- | ------------------------------------------- | ------------------------ |
| resume_type          | `dummy`, `from_log`, `from_target`, or `from_db`                             | from_target                                 | dummy                    |
| log_dir              | log directory used by `from_log`                                              | ./logs                                      | `[runtime].log_dir`      |
| config_file          | optional resume config file used by `from_log`                               | ./resume.config                             | empty                    |
| url                  | database URL used by `from_db`                                                | `mysql://127.0.0.1:3306`                    | required for `from_db`   |
| db_type              | database type used by `from_db`                                               | mysql                                       | required for `from_db`   |
| username             | database username used by `from_db`                                           | root                                        | empty                    |
| password             | database password used by `from_db`                                           | password                                    | empty                    |
| ssl_mode             | MySQL/PostgreSQL TLS mode used by `from_db`                                  | verify_full                                 | not set                  |
| ssl_ca_path          | CA certificate path used by `from_db`                                         | /etc/ssl/certs/ca.pem                       | empty                    |
| is_direct_connection | MongoDB driver `directConnection` option used by `from_db`                   | true                                        | not set                  |
| table_full_name      | target table used to store resume state for `from_db` or `from_target`       | apecloud_metadata.apedts_task_position      | empty                    |
| max_connections      | maximum resumer connection pool size                                          | 5                                           | 5                        |

For details, please refer to the resumer documentation: [resuming at breakpoint](/docs/en/snapshot/resume.md).

`resume_type=from_target` reuses the parsed sinker target. For a standalone checker with a dummy or
omitted sinker, it reuses the checker target. The legacy keys `resume_from_log`, `resume_log_dir`,
and `resume_config_file` are rejected; migrate them to `resume_type=from_log`, `log_dir`, and
`config_file`.

# [tracing]

| Config            | Description                                      | Example | Default |
| ----------------- | ------------------------------------------------ | ------- | ------- |
| task_summary_mode | trace aggregation mode: `task` or `marker`       | marker  | marker  |
| output_format     | trace output format: `plain` or `json`           | json    | plain   |

# [metacenter]

This optional section is used by the MySQL `dbengine` metadata-center mode.

| Config              | Description                                                    | Example                    | Default   |
| ------------------- | -------------------------------------------------------------- | -------------------------- | --------- |
| type                | metadata-center type: `basic` or `dbengine`                    | dbengine                   | basic     |
| url                 | metadata database URL; required for MySQL `dbengine` mode      | `mysql://127.0.0.1:3306`   | required  |
| username            | metadata database username                                     | root                       | empty     |
| password            | metadata database password                                     | password                   | empty     |
| ssl_mode            | MySQL TLS mode                                                 | verify_full                | not set   |
| ssl_ca_path         | CA certificate path                                            | /etc/ssl/certs/ca.pem      | empty     |
| ddl_conflict_policy | DDL conflict policy: `interrupt` or `ignore`                   | interrupt                  | interrupt |

The metadata-center URL must differ from both the extractor URL and the effective destination URL.

# [data_marker]

If this section is present, the required topology marker configuration is loaded.

| Config       | Description                     | Default  |
| ------------ | ------------------------------- | -------- |
| topo_name    | topology name                   | required |
| topo_nodes   | topology node list              | empty    |
| src_node     | source node                     | required |
| dst_node     | destination node                | required |
| do_nodes     | included nodes                  | required |
| ignore_nodes | excluded nodes                  | empty    |
| marker       | marker value                    | required |

# [processor]

| Config        | Description                                | Default |
| ------------- | ------------------------------------------ | ------- |
| lua_code_file | Lua processor source file loaded by DTS    | empty   |

# [metrics]

This section is available only when DTS is built with the `metrics` feature.

| Config    | Description                                      | Example       | Default |
| --------- | ------------------------------------------------ | ------------- | ------- |
| http_host | metrics HTTP bind address                        | 0.0.0.0       | 0.0.0.0 |
| http_port | metrics HTTP port                                | 9090          | 9090    |
| workers   | metrics HTTP worker count                        | 2             | 2       |
| labels    | comma-separated `key=value` metric labels        | env=prod,az=a | empty   |
