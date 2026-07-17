# 配置变更记录

当前配置参考：[配置详情](/docs/zh/config.md)。

## 2.0.26 对比 2.0.25

### 移除的配置

| 2.0.26 已移除 | 替代方式 |
| ------------- | -------- |
| `[sinker].sink_type=check` | 增加 `[checker] enable=true`。Standalone check：目标配置放入 `[checker]`；inline check：保留 `sink_type=write`。 |
| `[parallelizer].parallel_type=rdb_check` | RDB check/review/revise、inline CDC 使用 `rdb_merge`；inline snapshot 使用正常写入 parallelizer。 |
| `[extractor].sample_interval` | 使用 `[checker].sample_rate=1..100`；空值表示全量校验。 |
| `[sinker]` 下的 check 目标配置，包括 `check_log_dir` | Standalone check 移到 `[checker]`；inline check 复用 `[sinker]`。 |
| `[pipeline].max_rps` | 使用 `[extractor].max_rps` 和/或 `[sinker].max_rps`。 |
| `[resumer].resume_from_log`、`resume_log_dir`、`resume_config_file` | 使用 `resume_type=from_log`、`log_dir`、`config_file`；旧配置返回 `ConfigError`。 |
| `[pipeline].pipeline_type=http_server`、`http_host`、`http_port`、pipeline `with_field_defs` | 已删除，仅保留 `pipeline_type=basic`。Kafka `[sinker].with_field_defs` 不受影响。 |
| `db_type=foxlake`、`extract_type=foxlake_s3` 及 Foxlake 专用字段 | 不再支持 Foxlake 任务。 |

### 新增的配置

| Section | 新增配置 | 默认值 | 用途 |
| ------- | -------- | ------ | ---- |
| 数据库连接 section | `username`、`password` | 空 | URL 外配置认证信息；适用于 extractor、sinker、standalone checker、`resumer=from_db`、metacenter。 |
| 同上 | `ssl_mode`、`ssl_ca_path` | 不设置、空 | MySQL/PostgreSQL TLS；模式：`disable`、`require`、`verify_ca`、`verify_full`。 |
| `[extractor]` | `max_rps`、`max_mbps` | `0`、`0` | 源端限流；`0` 表示关闭。 |
| `[sinker]` | `max_rps`、`max_mbps` | `0`、`0` | 目标端限流；`0` 表示关闭。 |
| `[extractor]` | `parallel_type`、`partition_cols` | `table`、空 | MySQL/PostgreSQL snapshot 并发策略和切分列。 |
| MongoDB section | `is_direct_connection` | 不设置 | Driver `directConnection`；支持 extractor、sinker、resumer。 |
| Redis extractor/sinker | `is_cluster` | 自动探测 | `true`：集群；`false`：单节点；空：自动。 |
| MongoDB sinker | `mongo_require_shard_key_filter` | `true` | 写入 filter 必须包含完整目标 shard key。 |
| MongoDB struct 任务 | `extract_type=struct`、`sink_type=struct` | — | 迁移 collection 和 shard key。 |
| `[checker]` | `enable`、`queue_size`、`max_connections`、`batch_size`、`sample_rate` | 必填、`200`、`8`、`200`、空 | 启用 checker 及容量设置。 |
| `[checker]` | `output_full_row`、`output_revise_sql`、`revise_match_full_row` | 全部 `false` | 控制 diff 和修复 SQL 输出。 |
| `[checker]` | `retry_interval_secs`、`max_retries` | `0`、`0` | Checker 重试策略。 |
| `[checker]` | `check_log_dir`、`check_log_file_size`、`check_log_max_rows` | 空、`100mb`、`1000` | 本地校验日志及限制。 |
| `[checker]` | `check_log_s3`、`s3_*`、`cdc_check_log_interval_secs` | `false`、空、`30` | S3 上传和 CDC 校验日志间隔。 |
| Snapshot `[parallelizer]` | `rebalance_strategy`、`rebalance_cost`、`rebalance_max_partitions_per_sinker`、`rebalance_min_partition_rows`、`rebalance_split_skew_ratio` | `none`、`rows`、`2`、sinker batch size、`1.0` | 目标端 partition rebalance。 |
| `[runtime]` | `check_result_stdout_only` | `false` | stdout 只输出校验结果。 |
| `[tracing]` | `task_summary_mode`、`output_format` | `marker`、`plain` | Trace 聚合和输出格式。 |

### 配置逻辑变化

| 配置 | 2.0.25 | 2.0.26 |
| ---- | ------ | ------ |
| 省略 extractor `batch_size` | `[pipeline].buffer_size` | `[pipeline].buffer_size / 有效 snapshot 并发数` |
| 源端 snapshot 并发 | MySQL 使用 `[extractor].parallel_size`；`[runtime].tb_parallel_size` 是 runtime 配置。 | `[extractor].parallel_size` 控制 MySQL/PostgreSQL/MongoDB；`tb_parallel_size` 仅 fallback。 |
| `[filter].do_events` 为空 | 空值 | `*`，全部支持事件 |
| Redis `is_cluster` 为空 | 目标端按 `false`；源端不支持集群 | 自动探测；设为 `false` 强制单节点 |
| MongoDB CDC `source` 为空 | 加载为空，后续解释 | 默认 `change_stream`；非法值在配置加载时失败 |
| MongoDB snapshot `batch_size` | Snapshot 配置不使用 | 作为 cursor batch size，且必须能用 `u32` 表示 |
| `[sinker].batch_size=0` | 配置加载允许 | 非 dummy sinker 拒绝 |
| MongoDB shard-key filter | 配置层不强制 | 默认强制；`mongo_require_shard_key_filter=false` 可关闭 |
| Standalone check | 目标端配置为 check sinker | 可省略 `[sinker]`；目标配置放入 `[checker]` |
| Inline check | 使用 check 专用 sinker/parallelizer | 保留 `sink_type=write`；checker 复用 sinker 目标 |
| Inline CDC check | 不支持 | 仅 MySQL/PostgreSQL；要求 `rdb_merge` 和 resumer `from_target`/`from_db` |
| Dummy sinker 下的 `resumer=from_target` | 使用 sinker 目标 | 使用 standalone checker 目标 |
