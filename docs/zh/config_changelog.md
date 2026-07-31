# 配置变更记录

当前配置参考：[配置详情](/docs/zh/config.md)。

## 2.0.26 对比 2.0.25

### 移除的配置

| 2.0.26 已移除 | 替代方式 |
| ------------- | -------- |
| `[checker].enable` | Standalone check 使用 `[sinker].sink_type=check`；inline snapshot 增加 `[checker]`；inline CDC 使用 `[checker_cdc].is_enabled=true`。 |
| `[parallelizer].parallel_type=rdb_check` | RDB check/review/revise、inline CDC 使用 `rdb_merge`；inline snapshot 使用正常写入 parallelizer。 |
| `[extractor].sample_interval`、`[checker].sample_rate` | 使用 `[checker].sample_percent=1..100`；空值表示全量校验。 |
| `[checker]` 下的 checker 目标字段 | 使用 `[sinker]` 的普通数据库字段；standalone check 设置 `sink_type=check`。 |
| `[checker]` 下的结果字段：`output_full_row`、`output_revise_sql`、`revise_match_full_row`、`check_log_dir`、`check_log_file_size`、`check_log_max_rows` | 移到 `[checker_output]`；校验结果写入本地日志。 |
| `[checker].check_log_s3` 和 checker 的 `s3_*` 字段 | 删除且无替代配置；checker 不支持 S3 输出。 |
| `[checker].retry_interval_secs`、`[checker].max_retries` | 使用 `recheck_interval_secs`、`recheck_count`。 |
| `[checker].queue_size`、`[checker].cdc_check_log_interval_secs` | 使用 `[checker_cdc].queue_size`、`check_log_interval_secs`。 |
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
| `[checker]` | `batch_size`、`sample_percent`、`recheck_count`、`recheck_interval_secs`、`recheck_queue_size`、`recheck_queue_memory_mb` | `200`、空、`0`、`0`、`10000`、`256` | 通用校验行为和有界重试缓冲。 |
| `[checker_output]` | `output_full_row`、`output_revise_sql`、`revise_match_full_row` | 全部 `false` | 控制校验结果内容。 |
| `[checker_output]` | `check_log_dir`、`check_log_file_size`、`check_log_max_rows` | 空、`100mb`、`1000` | 本地滚动日志目录和限制；`check_log_dir` 为空时使用 `runtime.log_dir/check`。 |
| `[checker_cdc]` | `is_enabled`、`queue_size`、`check_log_interval_secs` | `false`、`200`、`30` | 启用和配置异步 inline CDC check。 |
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
| Standalone check | checker 目标配置在 `[checker]`；sinker 使用 dummy 或省略 | 将目标配置到 `[sinker]` 并设置 `sink_type=check` |
| Inline snapshot check | 通过 `[checker].enable=true` 启用；目标沿用 `[sinker]` | 保留 `sink_type=write`；增加 `[checker]` 后同步执行写后校验 |
| Inline CDC check | 通过 `[checker].enable=true` 启用；队列和输出周期配置在 `[checker]` | 仅 MySQL/PostgreSQL；使用 `[checker_cdc]` 启用，要求 `rdb_merge` 和 resumer `from_target`/`from_db` |
| Checker 结果介质 | 本地日志或 checker 管理的 S3 上传 | 仅本地日志；结果格式和日志限制配置在 `[checker_output]` |

### Checker 配置迁移示例

#### Standalone snapshot check

2.0.25 将 checker 目标和校验参数都配置在 `[checker]`，并使用 dummy sinker：

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

2.0.26 使用普通 sinker 数据库配置描述目标，并将校验行为与结果输出拆开：

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

Standalone struct check 同样通过 `sink_type=check` 选择。Standalone snapshot 支持 MySQL、
PostgreSQL、MongoDB 目标；standalone struct check 支持 MySQL、PostgreSQL。

#### Inline CDC check

2.0.25 在 `[checker]` 中启用并配置 CDC checker：

```ini
[checker]
enable=true
batch_size=200
queue_size=200
cdc_check_log_interval_secs=30
```

2.0.26 将通用比较参数保留在 `[checker]`，CDC 的启用和队列参数移到 `[checker_cdc]`：

```ini
[checker]
batch_size=200

[checker_cdc]
is_enabled=true
queue_size=200
check_log_interval_secs=30
```

Inline CDC check 保持 `[sinker].sink_type=write`，仅支持 MySQL/PostgreSQL，要求
`[parallelizer].parallel_type=rdb_merge`，并要求使用持久化的 `from_target` 或 `from_db`
resumer。Inline snapshot check 同样保持 `sink_type=write`；增加 `[checker]` section 即启用同步的
写后校验。
