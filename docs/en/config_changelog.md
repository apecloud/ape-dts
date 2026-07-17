# Config changelog

Current reference: [Config details](/docs/en/config.md).

## 2.0.26 compared with 2.0.25

### Removed configurations

| Removed in 2.0.26 | Replacement |
| ----------------- | ----------- |
| `[sinker].sink_type=check` | Add `[checker] enable=true`. Standalone check: put target fields in `[checker]`. Inline check: keep `sink_type=write`. |
| `[parallelizer].parallel_type=rdb_check` | Use `rdb_merge` for RDB check/review/revise and inline CDC; use the normal write parallelizer for inline snapshot. |
| `[extractor].sample_interval` | Use `[checker].sample_rate=1..100`; empty means full check. |
| Check target fields under `[sinker]`, including `check_log_dir` | Move them to `[checker]` for standalone check. Inline check reuses `[sinker]`. |
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
| `[checker]` | `enable`, `queue_size`, `max_connections`, `batch_size`, `sample_rate` | Required, `200`, `8`, `200`, empty | Enable and size checker. |
| `[checker]` | `output_full_row`, `output_revise_sql`, `revise_match_full_row` | All `false` | Control diff and revise SQL output. |
| `[checker]` | `retry_interval_secs`, `max_retries` | `0`, `0` | Checker retry policy. |
| `[checker]` | `check_log_dir`, `check_log_file_size`, `check_log_max_rows` | Empty, `100mb`, `1000` | Local check logs and limits. |
| `[checker]` | `check_log_s3`, `s3_*`, `cdc_check_log_interval_secs` | `false`, empty, `30` | S3 upload and CDC check-log interval. |
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
| Standalone check | Target configured as check sinker | `[sinker]` may be omitted; target configured in `[checker]` |
| Inline check | Check-specific sinker/parallelizer | Keep `sink_type=write`; checker reuses sinker target |
| Inline CDC check | Not supported | MySQL/PostgreSQL only; requires `rdb_merge` and resumer `from_target`/`from_db` |
| `resumer=from_target` with dummy sinker | Uses sinker target | Uses standalone checker target |
