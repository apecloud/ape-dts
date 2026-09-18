# Task metrics 指标说明

Task metrics 在任务级汇总 extractor、pipeline、sinker 和 checker 的运行状态，
并通过以下两种方式输出：

- `task.log`：`TaskMonitor` 周期性写入 JSON 对象。JSON 字段使用
  `snake_case` 命名，对应下表的“Task 日志字段”。无论是否启用 `metrics`
  crate feature，该日志都会输出。
- Prometheus：启用 `metrics` feature 后，当前指标值会以 Gauge 类型通过
  `GET /metrics` 暴露；`GET /healthz` 用于检查指标 HTTP 服务是否正常。

指标 HTTP 服务的地址、worker 数量和静态标签通过 `[metrics]` 配置。

并行利用率、batch 耗时、partitioner 耗时和独立的 `pipeline_sink_operations_total` 还会写入
`monitor.log` 的 `pipeline | <pipeline_id>`，随该 ID 的原有数据一起输出。
Snapshot 任务同时输出 `pipeline | global` 汇总；task JSON / Prometheus 保持任务级汇总。
格式及无样本行为见 [pipeline 监控日志](monitor.md#pipeline)。

## 采集和聚合规则

- Task metrics 在 `TaskMonitor` flush 时刷新。正常 pipeline 流程下，刷新周期由
  `[pipeline] checkpoint_interval_secs` 控制。
- 利用率、batch 耗时和数量、partitioner 耗时累计本次任务运行以来的数据，不受窗口配置影响。
  pipeline monitor 保留到最后一次 flush 完成后才注销。
- 吞吐量和响应时间指标使用 `[pipeline] counter_time_window_secs` 配置的滚动窗口。
- 时间窗口 counter 最多保留 `[pipeline] counter_max_sub_count` 个样本；事件频率较高时，
  指标统计当前窗口内最近保留的这些样本。
- 吞吐量和响应时间指标后缀 `max`、`min` 和 `avg` 分别表示当前窗口内有采样数据的
  单秒值的最大值、最小值和算术平均值；没有采样的秒不会参与平均值计算。
- 原有指标使用整数，除法舍弃小数部分；新增利用率、batch 和 partitioner 秒数保留小数。
- Sinker RPS/BPS 和单秒累计 RT 先按同一采样时刻对齐所有参与统计的 sinker monitor
  的秒桶，逐秒求和后再计算 min/max/avg。RPS/BPS 分别使用 `RecordCount`/`DataBytes`。
  Task 仍过滤已完成表的 monitor；其他组件的 task 级多 monitor 聚合沿用原有逐次合并规则。
  RT 批量提交时使用指标提交时刻，可能与数据库请求的实际完成时刻不同。
- `monitor.log` 中的 global 窗口统计合并各 monitor 的保留样本：按次数均值使用
  总 value / 总 count，极值取所有有效样本的极值；速率先对齐秒桶再逐秒求和。
  已完成表的未过期样本仍参与 global 统计，因此表完成阶段可能与 task 指标不同。
- 对应 counter 尚未产生数据时，字段不会出现在 task 日志中；已注册的 Prometheus
  Gauge 在首次发布数值前为 `0`。旧 Gauge 在字段缺失时仍可能保留上次值。

## Extractor 指标

`extractor_*` 表示从源端提取的流量。当前源端记录数和字节数统计不一定包含数据库
协议传输的全部字节。`extractor_pushed_*` 表示经过处理和过滤后，实际以 `DtData`
形式推送到 pipeline 的流量。

| Task 日志字段 | Prometheus 指标 | 单位 | 含义 |
| --- | --- | --- | --- |
| `extractor_rps_max` | `extractor_rps_max` | records/s | 窗口内单个采样秒的源端提取记录速率最大值。 |
| `extractor_rps_min` | `extractor_rps_min` | records/s | 窗口内单个采样秒的源端提取记录速率最小值。 |
| `extractor_rps_avg` | `extractor_rps_avg` | records/s | 窗口内各采样秒的源端提取记录速率平均值。 |
| `extractor_bps_max` | `extractor_bps_max` | bytes/s | 窗口内单个采样秒的源端提取字节速率最大值。 |
| `extractor_bps_min` | `extractor_bps_min` | bytes/s | 窗口内单个采样秒的源端提取字节速率最小值。 |
| `extractor_bps_avg` | `extractor_bps_avg` | bytes/s | 窗口内各采样秒的源端提取字节速率平均值。 |
| `extractor_pushed_rps_max` | `extractor_pushed_rps_max` | records/s | 处理和过滤后推送到 pipeline 的单秒记录速率最大值。 |
| `extractor_pushed_rps_min` | `extractor_pushed_rps_min` | records/s | 处理和过滤后推送到 pipeline 的单秒记录速率最小值。 |
| `extractor_pushed_rps_avg` | `extractor_pushed_rps_avg` | records/s | 处理和过滤后推送到 pipeline 的记录速率平均值。 |
| `extractor_pushed_bps_max` | `extractor_pushed_bps_max` | bytes/s | 处理和过滤后推送到 pipeline 的单秒字节速率最大值。 |
| `extractor_pushed_bps_min` | `extractor_pushed_bps_min` | bytes/s | 处理和过滤后推送到 pipeline 的单秒字节速率最小值。 |
| `extractor_pushed_bps_avg` | `extractor_pushed_bps_avg` | bytes/s | 处理和过滤后推送到 pipeline 的字节速率平均值。 |
| `extractor_plan_records` | `extractor_plan_records` | records | Snapshot 提取计划估算的源端记录数，仅 Snapshot 任务提供。 |

## Pipeline 指标

| Task 日志字段 | Prometheus 指标 | 单位 | 含义 |
| --- | --- | --- | --- |
| `pipeline_queue_size` | `pipeline_queue_size` | records | pipeline queue 当前缓存的记录数。 |
| `pipeline_queue_bytes` | `pipeline_queue_bytes` | bytes | pipeline queue 当前缓存的估算字节数。 |
| `timestamp` | `timestamp` | Unix 毫秒 | pipeline 已观察到的最大源端位点时间戳，仅 CDC 任务提供。位点没有可解析时间时为 `0`。 |

## Partitioner 指标

当前仅测量 `SnapshotParallelizer` 中的 `ChunkPartitioner::partition_dml` 调用。
一次样本 P 覆盖 chunk 分组、rebalance 和 partition 结果构造的完整墙钟耗时，
不包含前置输入大小统计、sink 派发、mutex 等待、写入及 checkpoint。

| Task 日志 / Prometheus 字段 | 单位 | 含义 |
| --- | --- | --- |
| `partitioner_duration_seconds_sum` | seconds | 本次运行以来各次 partition 调用的总耗时，`sum(P)`。 |
| `partitioner_duration_seconds_min` | seconds | 本次运行以来单次 partition 调用的最小耗时。 |
| `partitioner_duration_seconds_max` | seconds | 本次运行以来单次 partition 调用的最大耗时。 |
| `partitioner_duration_seconds_avg` | seconds | 本次运行以来单次调用的算术平均耗时，`sum(P) / partition_call_count`。 |

四个字段均为秒数浮点 Gauge，累计本次任务运行以来的所有调用，
不受 `counter_time_window_secs` 和 `counter_max_sub_count` 影响。
pipeline 只保存累计 sum/count 和极值，task 在最后一次 flush 时仍读取该 monitor 的累计值。
新任务实例重新统计，checkpoint 恢复不会恢复指标值。内部调用数独立于 `pipeline_sink_operations_total`，
不另外导出 count。空输入、错误返回和 panic unwind 的调用同样记录已花费的时间；
后续 sink 失败不会撤销已完成的 partition 样本。零耗时样本也参与平均值与 min。

测量启用后无样本时四个字段均为 0；已有统计不会随时间或表完成而归零。
未启用时不产生对应 Task JSON 字段，
Snapshot 的已注册 Prometheus Gauge 保持 0。当前不测量 raw、其他 partitioner 或 CDC 路径。

## Sinker 指标

| Task 日志字段 | Prometheus 指标 | 单位 | 含义 |
| --- | --- | --- | --- |
| `sinker_rps_max` | `sinker_rps_max` | records/s | 窗口内单个采样秒的目标端写入记录速率最大值。 |
| `sinker_rps_min` | `sinker_rps_min` | records/s | 窗口内单个采样秒的目标端写入记录速率最小值。 |
| `sinker_rps_avg` | `sinker_rps_avg` | records/s | 窗口内各采样秒的目标端写入记录速率平均值。 |
| `sinker_bps_max` | `sinker_bps_max` | bytes/s | 窗口内单个采样秒的目标端写入字节速率最大值。 |
| `sinker_bps_min` | `sinker_bps_min` | bytes/s | 窗口内单个采样秒的目标端写入字节速率最小值。 |
| `sinker_bps_avg` | `sinker_bps_avg` | bytes/s | 窗口内各采样秒的目标端写入字节速率平均值。 |
| `sinker_rt_max` | `sinker_rt_max` | 毫秒 | 窗口内单秒累计 sinker 操作响应时间的最大值，不是单次请求延迟分位数。 |
| `sinker_rt_min` | `sinker_rt_min` | 毫秒 | 窗口内单秒累计 sinker 操作响应时间的最小值。 |
| `sinker_rt_avg` | `sinker_rt_avg` | 毫秒 | 窗口内各采样秒累计 sinker 操作响应时间的平均值。 |
| `sinker_workers_configured` | `sinker_workers_configured` | workers | 当前任务注册的 sinker 实例数量。 |
| `sinker_workers_busy` | `sinker_workers_busy` | workers | 刷新时正在执行受监控 sinker 操作的已注册 sinker 数量。受监控操作包括数据写入、metadata 刷新，以及表完成处理等 control item 操作；不包含 `close`。这是一个瞬时采样值。 |
| `sinker_workers_per_drain_max` | `sinker_workers_per_drain_max` | workers/drain | 当前窗口内，单次 pipeline drain 将非空业务数据分发到的 distinct sinker 数量最大值。 |
| `sinker_workers_per_drain_avg` | `sinker_workers_per_drain_avg` | workers/drain | 当前窗口内，每次 pipeline drain 将非空业务数据分发到的 distinct sinker 数量平均值。 |
| `sinker_sinked_records` | `sinker_sinked_records` | records | 已成功写入目标端的累计记录数。 |
| `sinker_sinked_bytes` | `sinker_sinked_bytes` | bytes | 已成功写入目标端的累计估算字节数。 |
| `sinker_ddl_count` | `sinker_ddl_count` | operations | sink 端累计处理的 DDL 操作数，仅 CDC 任务提供。 |

### DML batch 利用率与耗时

这里的 batch 是一次 `BaseParallelizer::sink_dml` 调度的写入批次，包含该次调用的
所有 partition；一个 partition 可能执行多个 SQL 请求。计时从派发开始，不包含
此前由 `drain()` 完成的队列取数。既有 `sinker_workers_per_drain_*` 保留原名。

以下 8 个指标测量 `BaseParallelizer::sink_dml` 成功完成的非空 DML batch。
Snapshot、table、partition、serial 等使用该入口的路径统一记录，包含 Snapshot 与 CDC
任务；直接调用 sinker 的其他路径（例如 MergeParallelizer）暂不记录。
`K = min(parallel_size, sinkers.len())`，parallel_size 为本次 sink_dml 的并发上限
（serial 为 1）；内部 W 是取得 sinker mutex 后，
所有非空 `sink_dml` 调用的耗时之和，包含连接池、内部重试和 checker 等等待。
D 从派发前计到全部 partition 完成，包含 mutex 等待和调度时间，不含 partition/rebalance
计算与 checkpoint。每次 batch 的利用率 `U = W / (K * D)`。

| Task 日志 / Prometheus 字段 | 单位 | 含义 |
| --- | --- | --- |
| `pipeline_sink_parallel_utilization_avg` | 0..1 | 本次运行以来每次有效 pipeline sink operation 的 U 的算术平均，`sum(U) / N`。 |
| `pipeline_sink_parallel_utilization_min` | 0..1 | 本次运行以来最小 U。 |
| `pipeline_sink_parallel_utilization_max` | 0..1 | 本次运行以来最大 U。 |
| `pipeline_sink_duration_seconds_sum` | seconds | 本次运行以来各 pipeline sink operation 的墙钟耗时之和，`sum(D)`。 |
| `pipeline_sink_duration_seconds_min` | seconds | 本次运行以来单次 pipeline sink operation 的最小墙钟耗时。 |
| `pipeline_sink_duration_seconds_max` | seconds | 本次运行以来单次 pipeline sink operation 的最大墙钟耗时。 |
| `pipeline_sink_duration_seconds_avg` | seconds | 本次运行以来单次 pipeline sink operation 的平均墙钟耗时，`sum(D) / N`。 |
| `pipeline_sink_operations_total` | batches | 本次运行以来的有效 pipeline sink operation 数 N。 |

这些字段累计本次任务运行以来的全部有效 batch，沿用 Prometheus Gauge 类型。
利用率、batch 耗时、partitioner 耗时与累计批次数均使用通用无窗口 counter。
`pipeline_sink_operations_total` 每个有效 pipeline sink operation 增加 1；monitor.log 对应独立的 `pipeline_sink_operations_total | latest=N` 行。

新指标不受 `counter_time_window_secs` / `counter_max_sub_count` 限制。
pipeline 在 batch 完成时更新累计 sum/count 和极值，不保留历史样本队列。
任务只有一个 pipeline，其 monitor 保留到最后一次 flush 完成后才注销。
count 和 sum 在本次运行内持续累加，min/max/avg 基于全部有效样本计算。
新任务实例重新统计，checkpoint 恢复不会恢复指标值。
利用率 avg 按 batch 等权平均；W 仅供内部计算，没有导出指标。
`sum(D)` 是该 pipeline 各次成功 sink 操作的总墙钟耗时。

空 batch、错误、取消、非 DML 操作和无效测量不计入 N。测量启用后，N=0 时全部新字段为 0，
已有统计不会因空闲或表完成而归零；以 N 区分无样本与有效的零利用率。未启用测量时 Task JSON
不包含这些字段，Prometheus 已注册 Gauge 保持 0。
例如 K=4、各 partition 为 100/10/10/10 ms 时 U=0.325；只有两个 100 ms partition 时 U=0.5。
高利用率也可能来自连接池等待，应结合 D、吞吐和目标库延迟判断。

## Checker 指标

仅在任务运行数据 checker 时产生以下指标。

| Task 日志字段 | Prometheus 指标 | 单位 | 含义 |
| --- | --- | --- | --- |
| `checker_miss_count` | `checker_miss_total` | records | checker 发现的目标端缺失记录累计数。 |
| `checker_diff_count` | `checker_diff_total` | records | checker 发现的源端和目标端内容不一致记录累计数。 |
| `checker_pending` | `checker_queue_size` | records | checker 当前跟踪、尚未解决的记录数。 |
| `checker_rps_max` | `checker_rps_max` | records/s | 窗口内单个采样秒的校验记录速率最大值。 |
| `checker_rps_min` | `checker_rps_min` | records/s | 窗口内单个采样秒的校验记录速率最小值。 |
| `checker_rps_avg` | `checker_rps_avg` | records/s | 窗口内各采样秒的校验记录速率平均值。 |
| `checker_miss_rps_max` | `checker_miss_rps_max` | records/s | 窗口内单个采样秒的缺失记录速率最大值。 |
| `checker_miss_rps_min` | `checker_miss_rps_min` | records/s | 窗口内单个采样秒的缺失记录速率最小值。 |
| `checker_miss_rps_avg` | `checker_miss_rps_avg` | records/s | 窗口内各采样秒的缺失记录速率平均值。 |
| `checker_diff_rps_max` | `checker_diff_rps_max` | records/s | 窗口内单个采样秒的不一致记录速率最大值。 |
| `checker_diff_rps_min` | `checker_diff_rps_min` | records/s | 窗口内单个采样秒的不一致记录速率最小值。 |
| `checker_diff_rps_avg` | `checker_diff_rps_avg` | records/s | 窗口内各采样秒的不一致记录速率平均值。 |

## Snapshot 进度指标

| Task 日志字段 | Prometheus 指标 | 单位 | 含义 |
| --- | --- | --- | --- |
| `progress` | `progress` | percent | Snapshot 完成百分比，计算方式为 `finished_progress_count * 100 / total_progress_count`，最大为 `100`。 |
| `total_progress_count` | 不导出 | tables | Snapshot 进度计算使用的表总数。 |
| `finished_progress_count` | 不导出 | tables | Snapshot pipeline 已计为完成的表数量。 |

## 预留字段

`TaskMetricsType` 还定义了 `delay` 和 `pipeline_record_size_max`。当前
`TaskMonitor` 不会生成这两个字段，Prometheus exporter 也没有注册它们。在实现完成前，
不应将它们用于告警或监控面板。
