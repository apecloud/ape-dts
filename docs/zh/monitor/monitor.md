# 监控信息

counter 用于记录任务状态，并会按照 `[pipeline] checkpoint_interval_secs` 配置周期性写入 `monitor.log`。

# 时间窗口 counter

这类 counter 由一组 sub counter 组成。任务执行过程中，每当状态发生变化（例如成功写入一批数据到目标库），都会创建一个新的 sub counter 来记录对应的增量信息（如该次写入的数据条数）。

- counter 具有时间窗口（配置：[pipeline] counter_time_window_secs），过期数据会被丢弃，仅包含最近一个时间窗口的数据。
- counter 用于记录动态实时数据，如：最近一个时间窗口内，同步的数据条数。
- counter 上还会提供一系列聚合算法，如：最近一个时间窗口内，平均每秒同步的数据条数。

## 聚合算法

对于时间窗口类型的 counter，我们提供了该 counter 在窗口内的一系列聚合算法。


| 聚合算法   | 说明                                            | 示例                                      |
| :--------- | :---------------------------------------------- | :---------------------------------------- |
| sum        | 所有 sub counter 的总和                         | 最近 10s 内，同步的数据总条数             |
| avg        | 所有 sub counter 的总和 / sub counter 的个数    | 最近 10s 内，平均每次写入到目标库的耗时   |
| avg_by_sec | 所有 sub counter 的总和 / 时间窗口（单位：秒）  | 最近 10s 内，每秒平均写入目标库的数据条数 |
| max        | sub counter 中具有最大值的那一个                | 最近 10s 内，单次写入目标库的最大数据条数 |
| max_by_sec | 将 sub counter 按秒聚合后，选择总和最大的那一秒 | 最近 10s 内，单秒写入目标库的最大数据条数 |

# 无窗口 counter

该类 counter 记录本次任务运行的累计数据，不会过期，例如累计同步条数和已完成的 sink 操作数。

## 聚合算法

| 聚合算法 | 说明   | 示例                     |
| :------- | :----- | :----------------------- |
| latest   | 当前值 | 任务累计已同步的数据条数 |


# counter 详情

## 时间窗口配置

```
[pipeline]
counter_time_window_secs=60
```

## extractor
### monitor.log
```
2024-02-29 01:25:09.554271 | extractor | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554311 | extractor | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586
2024-02-29 01:25:09.554350 | extractor | extracted_record_count | avg_by_sec=20 | sum=20 | max_by_sec=20
2024-02-29 01:25:09.554391 | extractor | extracted_data_bytes | avg_by_sec=900 | sum=900 | max_by_sec=900
```

### Prometheus 指标
```
# 源端数据进入 ape-dts 服务器网卡的流量（当前统计可能尚不完全准确）
extractor_rps_avg 13 
extractor_rps_max 13
extractor_rps_min 13
extractor_bps_avg 586
extractor_bps_max 586
extractor_bps_min 586

# 数据处理后推送至 pipeline 的流量，已转换为 DtData
extractor_pushed_rps_avg 20
extractor_pushed_rps_max 20
extractor_pushed_rps_min 20
extractor_pushed_bps_avg 900 
extractor_pushed_bps_max 900 
extractor_pushed_bps_min 900 
```

### counters
| counter                                          | counter 类型 | 说明                                     |
| :----------------------------------------------- | :----------- | :--------------------------------------- |
| record_count                                     | 时间窗口     | 从源端拉取的数据条数                     |
| data_bytes                                       | 时间窗口     | 从源端拉取的数据 bytes                   |
| extracted_records（即 `extracted_record_count`） | 时间窗口     | 从源端提取并推送到 pipeline 的数据条数   |
| extracted_data_bytes                             | 时间窗口     | 从源端提取并推送到 pipeline 的数据 bytes |

任务指标会同时暴露两组吞吐指标，便于实时监控：

- `extractor_rps_*` / `extractor_bps_*`：源端数据进入 ape-dts 服务器网卡的流量（包含业务过滤后的数据，不包含时间过滤的数据）
  - `extractor_rps_avg`、`extractor_rps_max`、`extractor_rps_min`：每秒记录数
  - `extractor_bps_avg`、`extractor_bps_max`、`extractor_bps_min`：每秒 bytes 
  - 注意：当前统计可能尚不完全准确

- `extractor_pushed_rps_*` / `extractor_pushed_bps_*`：数据处理与过滤后推送到 pipeline 的流量（已转换为 DtData）
  - `extractor_pushed_rps_avg`、`extractor_pushed_rps_max`、`extractor_pushed_rps_min`：每秒记录数
  - `extractor_pushed_bps_avg`、`extractor_pushed_bps_max`、`extractor_pushed_bps_min`：每秒 bytes 

对比这两组指标，可以直观看到过滤规则的实际作用。

<br/>

- record_count

| 聚合算法   | 说明                                 |
| :--------- | :----------------------------------- |
| avg_by_sec | 窗口内，每秒平均从源端拉取的数据条数 |
| sum        | 窗口内，总计从源端拉取的数据条数     |
| max_by_sec | 窗口内，单秒从源端拉取的数据条数峰值 |

<br/>

- data_bytes

| 聚合算法   | 说明                                      |
| :--------- | :---------------------------------------- |
| avg_by_sec | 窗口内，每秒平均从源端拉取的数据 bytes    |
| sum        | 窗口内，总计从源端拉取的数据 bytes        |
| max_by_sec | 窗口内，单秒从源端拉取的数据 bytes 数峰值 |

<br/>

- extracted_record_count

| 聚合算法   | 说明                                                   |
| :--------- | :----------------------------------------------------- |
| avg_by_sec | 窗口内，每秒平均提取并推送到 pipeline 的数据条数 bytes |
| sum        | 窗口内，提取并推送到 pipeline 的数据条数总计 bytes     |
| max_by_sec | 窗口内，单秒提取并推送到 pipeline 的数据条数峰值 bytes |

<br/>

- extracted_data_bytes

| 聚合算法   | 说明                                                      |
| :--------- | :-------------------------------------------------------- |
| avg_by_sec | 窗口内，每秒平均提取并推送到 pipeline 的数据 bytes  bytes |
| sum        | 窗口内，提取并推送到 pipeline 的数据 bytes 总计 bytes     |
| max_by_sec | 窗口内，单秒提取并推送到 pipeline 的数据 bytes 峰值 bytes |

## sinker

### monitor.log

```
2024-02-29 01:25:09.554461 | sinker | rt_per_query | avg=3369 | sum=23585 | max=6408
2024-02-29 01:25:09.554503 | sinker | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554544 | sinker | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586
2024-02-29 01:25:09.554582 | sinker | records_per_query | avg=1 | sum=13 | max=2
```

### counter 说明

| counter           | 窗口类型 | 说明                     |
| :---------------- | :------- | :----------------------- |
| rt_per_query      | 时间窗口 | 单次写入耗时，单位：微秒 |
| records_per_query | 时间窗口 | 单次写入的记录条数       |
| record_count      | 时间窗口 | 写入目标端的记录条数     |
| data_bytes        | 时间窗口 | 写入目标端的 bytes       |

<br/>

- rt_per_query

| 聚合算法 | 说明                           |
| :------- | :----------------------------- |
| avg      | 窗口内，平均单次写入耗时       |
| sum      | 窗口内，写入目标的总耗时       |
| max      | 窗口内，单次写入目标的最大耗时 |

<br/>

- record_count

| 聚合算法   | 说明                         |
| :--------- | :--------------------------- |
| avg_by_sec | 窗口内，每秒平均写入数据条数 |
| sum        | 窗口内，总计写入数据条数     |
| max_by_sec | 窗口内，单秒写入数据条数峰值 |

<br/>

- data_bytes

| 聚合算法   | 说明                              |
| :--------- | :-------------------------------- |
| avg_by_sec | 窗口内，每秒平均写入的数据 bytes  |
| sum        | 窗口内，总计写入的数据 bytes      |
| max_by_sec | 窗口内，单秒写入的数据 bytes 峰值 |

<br/>

- records_per_query

| 聚合算法 | 说明                           |
| :------- | :----------------------------- |
| avg      | 窗口内，每次写入数据条数平均值 |
| sum      | 窗口内，总计写入数据条数       |
| max      | 窗口内，单次写入数据条数峰值   |


## pipeline
### monitor.log
```
2024-02-29 01:25:09.554348 | pipeline | record_size | avg=45
2024-02-29 01:25:09.554387 | pipeline | buffer_size | avg=3 | sum=13 | max=4
2024-02-29 01:25:09.554423 | pipeline | sinked_count | latest=13
```

三类新指标和独立的有效 pipeline sink operation 数量随所属 pipeline 一起 flush，使用相同的 ID：

```text
pipeline | f49dc9ee7d863b59 | buffer_size | sum=180 | avg=3 | max=4 | min=1
pipeline | f49dc9ee7d863b59 | sinker_workers_per_drain | sum=66 | avg=1 | max=2 | min=1
pipeline | f49dc9ee7d863b59 | sinked_records | latest=141
pipeline | f49dc9ee7d863b59 | pipeline_sink_parallel_utilization | latest=0.25 | avg=0.625 | min=0.25 | max=1
pipeline | f49dc9ee7d863b59 | pipeline_sink_duration_seconds | latest=0.4 | avg=0.25 | min=0.1 | max=0.4
pipeline | f49dc9ee7d863b59 | pipeline_sink_operations_total | latest=2
pipeline | f49dc9ee7d863b59 | partitioner_duration_seconds | latest=0.0009 | avg=0.0004 | min=0.0001 | max=0.0009
```

以上示例省略了时间戳。利用率单位为 `0..1`，两类耗时单位为秒，保留小数。
这三类指标使用时间窗口 counter，受 `counter_time_window_secs` 和
`counter_max_sub_count` 控制。`pipeline_sink_operations_total` 保留无窗口统计，
`latest` 表示本次任务运行以来的有效 sink 操作总数。

一个 batch 对应 parallelizer 的一次完整 sink 调度。所有 partition 成功完成后，
记录一次利用率、一次耗时，并将操作总数加一。partitioner 按每次 partition 调用独立采样。
latest 为窗口内最近一次有效样本，min/max 为窗口内单次有效样本的极值；平均值使用窗口内保留样本的 sum/count，不使用累计操作总数作分母；过期样本不再参与统计。

Snapshot 任务同时输出原有 `pipeline | global` 汇总。每个任务只有一个 pipeline，
其 monitor 保留到 final flush 完成后才注销。`task.log` 和 Prometheus 读取相同的
pipeline counter。CDC 仅输出各 pipeline ID 的日志。
已启用但无有效样本的 pipeline counter 输出零；global 窗口日志不输出没有有效样本的 counter。

新任务实例重新统计，不从 checkpoint 恢复指标值。batch 指标支持 Snapshot 和 CDC，
partitioner 指标目前仅采集 Snapshot 的 chunk partition。

### counter 说明

| counter      | 窗口类型 | 说明                      |
| :----------- | :------- | :------------------------ |
| record_size  | 时间窗口 | 单条记录大小，单位：字节  |
| buffer_size  | 时间窗口 | pipeline 中缓存的记录条数 |
| sinked_count | 无窗口   | 任务处理的记录总数        |
| pipeline_sink_operations_total | 无窗口 | 已完成的有效 pipeline sink operation 总数 |
| pipeline_sink_parallel_utilization | 时间窗口 | 每次 pipeline sink operation 利用率的 latest、avg、min、max |
| pipeline_sink_duration_seconds | 时间窗口 | pipeline sink operation 秒数的 latest、avg、min、max |
| partitioner_duration_seconds | 时间窗口 | partition 秒数的 latest、avg、min、max |

<br/>

- record_size

| 聚合算法 | 说明                       |
| :------- | :------------------------- |
| avg      | 窗口内，单条记录的平均大小 |

<br/>

- buffer_size

| 聚合算法 | 说明                       |
| :------- | :------------------------- |
| avg      | 窗口内，缓存记录的平均条数 |
| sum      | 窗口内，缓存记录的总条数   |
| max      | 窗口内，缓存记录的最大条数 |

<br/>

- sinked_count

| 聚合算法 | 说明                 |
| :------- | :------------------- |
| latest   | 该任务已同步数据条数 |
