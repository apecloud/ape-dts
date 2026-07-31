# Config changelog

Current reference: [Config details](/docs/en/config.md).

## 2.0.26 compared with 2.0.25

### Removed configurations

| Removed in 2.0.26 | Replacement |
| ----------------- | ----------- |
| `[checker].enable` | Standalone check: use `[sinker].sink_type=check`. Inline snapshot: add `[checker]`. Inline CDC: use `[checker_cdc].is_enabled=true`. |
| `[parallelizer].parallel_type=rdb_check` | Use `rdb_merge` for RDB check/review/revise and inline CDC; use the normal write parallelizer for inline snapshot. |
| `[extractor].sample_interval`, `[checker].sample_rate` | Use `[checker].sample_percent=1..100`; empty means full check. |
| Checker target fields under `[checker]` | Use normal database fields under `[sinker]`; standalone check sets `sink_type=check`. |
| Checker result fields under `[checker]`: `output_full_row`, `output_revise_sql`, `revise_match_full_row`, `check_log_dir`, `check_log_file_size`, `check_log_max_rows` | Move them to `[checker_output]`. Checker results are written to local logs. |
| `[checker].check_log_s3` and checker `s3_*` fields | Removed without replacement. Checker S3 output is not supported. |
| `[checker].retry_interval_secs`, `[checker].max_retries` | Use `recheck_interval_secs`, `recheck_count`. |
| `[checker].queue_size`, `[checker].cdc_check_log_interval_secs` | Use `[checker_cdc].queue_size`, `check_log_interval_secs`. |
| `[pipeline].max_rps` | Use `[extractor].max_rps` and/or `[sinker].max_rps`. |
| `[resumer].resume_from_log`, `resume_log_dir`, `resume_config_file` | Use `resume_type=from_log`, `log_dir`, `config_file`. Old keys return `ConfigError`. |
| `[pipeline].pipeline_type=http_server`, `http_host`, `http_port`, pipeline `with_field_defs` | Removed. Only `pipeline_type=basic` remains. Kafka `[sinker].with_field_defs` is unchanged. |
| `db_type=foxlake`, `extract_type=foxlake_s3`, and Foxlake-only fields | Foxlake tasks are no longer supported. |

### Added configurations

| Section | New configuration | Default | Purpose |
| ------- | ----------------- | ------- | ------- |
| Database connection sections | `username`, `password` | Empty | Credentials outside URL. Applies to extractor, sinker, standalone checker, `resumer=from_db`, metacenter. |
| Same sections | `ssl_mode`, `ssl_ca_path` | Not set, empty | MySQL/PostgreSQL TLS. Modes: `disable`, `require`, `verify_ca`, `verify_full`. |
| `[extractor]` | `max_rps`, `max_mbps` | `0`, `0` | Source rate limits; `0` disables. |
| `[sinker]` | `max_rps`, `max_mbps` | `0`, `0` | Target rate limits; `0` disables. |
| `[extractor]` | `parallel_type`, `partition_cols` | `table`, empty | MySQL/PostgreSQL snapshot parallel strategy and split column. |
| MongoDB sections | `is_direct_connection` | Not set | Driver `directConnection`; supported by extractor, sinker, resumer. |
| Redis extractor/sinker | `is_cluster` | Auto-detect | `true`: cluster; `false`: single node; empty: auto. |
| MongoDB sinker | `mongo_require_shard_key_filter` | `true` | Require complete target shard key in write filters. |
| MongoDB struct task | `extract_type=struct`, `sink_type=struct` | — | Migrate collections and shard keys. |
| `[checker]` | `batch_size`, `sample_percent`, `recheck_count`, `recheck_interval_secs`, `recheck_queue_size`, `recheck_queue_memory_mb` | `200`, empty, `0`, `0`, `10000`, `256` | Common check behavior and bounded retry buffering. |
| `[checker_output]` | `output_full_row`, `output_revise_sql`, `revise_match_full_row` | All `false` | Control check-result content. |
| `[checker_output]` | `check_log_dir`, `check_log_file_size`, `check_log_max_rows` | Empty, `100mb`, `1000` | Local rolling-log directory and limits. Empty `check_log_dir` uses `runtime.log_dir/check`. |
| `[checker_cdc]` | `is_enabled`, `queue_size`, `check_log_interval_secs` | `false`, `200`, `30` | Enable and configure asynchronous inline CDC check. |
| Snapshot `[parallelizer]` | `rebalance_strategy`, `rebalance_cost`, `rebalance_max_partitions_per_sinker`, `rebalance_min_partition_rows`, `rebalance_split_skew_ratio` | `none`, `rows`, `2`, sinker batch size, `1.0` | Sink partition rebalance. |
| `[runtime]` | `check_result_stdout_only` | `false` | Print only checker result logs to stdout. |
| `[tracing]` | `task_summary_mode`, `output_format` | `marker`, `plain` | Trace aggregation and format. |

### Configuration logic changes

| Configuration | 2.0.25 | 2.0.26 |
| ------------- | ------ | ------ |
| Extractor `batch_size` omitted | `[pipeline].buffer_size` | `[pipeline].buffer_size / effective snapshot parallel_size` |
| Source snapshot concurrency | MySQL used `[extractor].parallel_size`; `[runtime].tb_parallel_size` was runtime config. | `[extractor].parallel_size` controls MySQL/PostgreSQL/MongoDB. `tb_parallel_size` is fallback only. |
| `[filter].do_events` empty | Empty value | `*` (all supported events) |
| Redis `is_cluster` empty | Target treated as `false`; no source cluster mode | Auto-detect; set `false` to force single node |
| MongoDB CDC `source` empty | Loaded empty, interpreted later | Defaults to `change_stream`; invalid value fails during config loading |
| MongoDB snapshot `batch_size` | Not used by snapshot config | Used as cursor batch size; must fit `u32` |
| `[sinker].batch_size=0` | Accepted during config loading | Rejected for non-dummy sinkers |
| MongoDB shard-key filter | No config-level requirement | Required by default; set `mongo_require_shard_key_filter=false` to disable |
| Standalone check | Checker target configured under `[checker]`; sinker was dummy or omitted | Configure the target under `[sinker]` and set `sink_type=check` |
| Inline snapshot check | Enabled by `[checker].enable=true`; target reused `[sinker]` | Keep `sink_type=write`; adding `[checker]` enables synchronous post-sink check |
| Inline CDC check | Enabled by `[checker].enable=true`; queue and output interval were under `[checker]` | MySQL/PostgreSQL only; enable with `[checker_cdc]`, requires `rdb_merge` and resumer `from_target`/`from_db` |
| Checker result destination | Local logs or checker-managed S3 upload | Local logs only; configure formatting and log limits under `[checker_output]` |

### Checker migration examples

#### Standalone snapshot check

2.0.25 configured the checker target and behavior together under `[checker]` and used a dummy
sinker:

```ini
[sinker]
sink_type=dummy

[checker]
enable=true
db_type=mysql
url=mysql://target-host:3306
sample_rate=25
retry_interval_secs=5
max_retries=4
output_full_row=true
check_log_dir=./logs/check
```

2.0.26 uses the regular sinker database configuration for the target and separates behavior from
output:

```ini
[sinker]
db_type=mysql
sink_type=check
url=mysql://target-host:3306

[checker]
sample_percent=25
recheck_interval_secs=5
recheck_count=4

[checker_output]
output_full_row=true
check_log_dir=./logs/check
```

The same `sink_type=check` selection is used by standalone structure checks. Standalone snapshot
supports MySQL, PostgreSQL, and MongoDB targets; standalone structure check supports MySQL and
PostgreSQL.

#### Inline CDC check

2.0.25 enabled and tuned CDC checking in `[checker]`:

```ini
[checker]
enable=true
batch_size=200
queue_size=200
cdc_check_log_interval_secs=30
```

2.0.26 keeps common comparison settings in `[checker]` and moves CDC activation and queue settings
to `[checker_cdc]`:

```ini
[checker]
batch_size=200

[checker_cdc]
is_enabled=true
queue_size=200
check_log_interval_secs=30
```

Inline CDC check keeps `[sinker].sink_type=write`, supports MySQL/PostgreSQL, requires
`[parallelizer].parallel_type=rdb_merge`, and requires a persistent `from_target` or `from_db`
resumer. Inline snapshot check also keeps `sink_type=write`; the presence of `[checker]` enables its
synchronous post-sink check.
