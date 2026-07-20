# Task metrics 指标说明

Task metrics 在任务级汇总 extractor、pipeline、sinker 和 checker 的运行状态，
并通过以下两种方式输出：

- `task.log`：`TaskMonitor` 周期性写入 JSON 对象。JSON 字段使用
  `snake_case` 命名，对应下表的“Task 日志字段”。无论是否启用 `metrics`
  crate feature，该日志都会输出。
- Prometheus：启用 `metrics` feature 后，当前指标值会以 Gauge 类型通过
  `GET /metrics` 暴露；`GET /healthz` 用于检查指标 HTTP 服务是否正常。

指标 HTTP 服务的地址、worker 数量和静态标签通过 `[metrics]` 配置。

## 采集和聚合规则

- Task metrics 在 `TaskMonitor` flush 时刷新。正常 pipeline 流程下，刷新周期由
  `[pipeline] checkpoint_interval_secs` 控制。
- 吞吐量和响应时间指标使用 `[pipeline] counter_time_window_secs` 配置的滚动窗口。
- 时间窗口 counter 最多保留 `[pipeline] counter_max_sub_count` 个样本；事件频率较高时，
  指标统计当前窗口内最近保留的这些样本。
- 指标后缀 `max`、`min` 和 `avg` 分别表示当前窗口内有采样数据的单秒值的最大值、
  最小值和算术平均值；没有采样的秒不会参与平均值计算。
- 指标内部使用整数，除法会舍弃小数部分。
- 当一个任务包含多个组件 monitor 时，`max` 和 `min` 取所有 monitor 的极值。
  当前 `avg` 会逐个合并各 monitor 的平均值，并不是全局加权平均值。
- 对应 counter 尚未产生数据时，字段不会出现在 task 日志中；已注册的 Prometheus
  Gauge 在首次发布数值前为 `0`。

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
