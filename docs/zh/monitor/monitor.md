# 监控信息

在任务运行过程中，我们提供了一系列 counter 来记录当前状态。counter 会定时（配置：[pipeline] checkpoint_interval_secs）写入到 monitor.log。

# 时间窗口 counter

该类 counter 本身是一个数组容器。任务运行时，每当状态变化（如：成功写入一批数据到目标），就会往 counter 容器中推入一个新的 sub counter，用于记录该次变化产生的增量（如：该次写入的数据条数）。

- counter 具有时间窗口（配置：[pipeline] counter_time_window_secs），过期数据会被丢弃，仅包含最近一个时间窗口的数据。
- counter 用于记录动态实时数据，如：最近一个时间窗口内，同步的数据条数。
- counter 上还会提供一系列聚合算法，如：最近一个时间窗口内，平均每秒同步的数据条数。

## 聚合方式

对于时间窗口类型的 counter，我们提供了该 counter 在窗口内的一系列聚合算法。


| 聚合方式 | 说明 | 例子 |
| :-------- | :-------- | :-------- | 
| sum | 所有 sub counter 的总和 | 最近 10s 内，同步的数据总条数 |
| avg | 所有 sub counter 的总和 / sub counter 的个数 | 最近 10s 内，平均每次写入到目标库的耗时 |
| avg_by_sec | 所有 sub counter 的总和 / 时间窗口（单位：秒） | 最近 10s 内，每秒平均写入目标库的数据条数 |
| max | sub counter 中具有最大值的那一个 | 最近 10s 内，单次写入目标库的最大数据条数 |
| max_by_sec | 将 sub counter 按秒聚合后，选择总和最大的那一秒 | 最近 10s 内，单秒写入目标库的最大数据条数 |

# 无窗口 counter

该类 counter 就是一个简单的计数器，用于记录累计数据，如：当前任务总共已同步数据条数。

## 聚合方式

| 聚合方式 | 说明 | 例子 |
| :-------- | :-------- | :-------- |
| latest | 当前值 | 任务累计已同步的数据条数 |


# 实际使用

## 时间窗口配置

```
[pipeline]
counter_time_window_secs=60
```

## extractor
### monitor.log
```
2024-02-29 01:25:09.554350 | extractor | extracted_record_count | avg_by_sec=20 | sum=20 | max_by_sec=20
2024-02-29 01:25:09.554391 | extractor | extracted_data_bytes | avg_by_sec=900 | sum=900 | max_by_sec=900
2024-02-29 01:25:09.554271 | extractor | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554311 | extractor | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586

```

### Prometheus 监控指标
```
# 源端数据进到ape-dts运行机器网卡的流量（目前统计得还不太准确）
extractor_rps_avg 77
extractor_rps_max 77
extractor_rps_min 77
extractor_bps_avg 3438
extractor_bps_max 3438
extractor_bps_min 3438

# 数据经过处理后推送到pipeline的流量，已转为DtData
extractor_pushed_rps_avg 77
extractor_pushed_rps_max 77
extractor_pushed_rps_min 77
extractor_pushed_bps_avg 3438
extractor_pushed_bps_max 3438
extractor_pushed_bps_min 3438
```

### counter 说明
| counter | counter 类型 | 说明 |
| :-------- | :-------- | :-------- |
| extracted_record_count | 时间窗口 | 从源端提取的数据条数（包含被过滤的数据） |
| extracted_data_bytes | 时间窗口 | 从源端提取的数据 bytes（包含被过滤的数据） |
| record_count | 时间窗口 | 推送到队列的数据条数（过滤后） |
| data_bytes | 时间窗口 | 推送到队列的数据 bytes（过滤后） |

<br/>

**过滤与 Metrics 说明：**

对于 Extractor，存在两层过滤逻辑：

1. **时间过滤（Time Filter）**：当数据时间戳早于配置的 `start_time_utc` 时被过滤
   - 这类数据**不会**被记录到 metrics 中
   - 目的：避免无效数据解析，优化性能

2. **业务过滤（Business Filter）**：通过 `filter_event` / `filter_schema` 等配置规则过滤
   - 这类数据**会**被记录到 `extracted_*` metrics 中
   - 但**不会**被记录到 `record_count` / `data_bytes` metrics 中
   - 目的：了解过滤规则的实际影响

**Prometheus 监控指标：**

任务指标中同时会提供两组吞吐相关的指标用于实时监控：

- `extractor_rps_*` / `extractor_bps_*`：源端数据进到 ape-dts 运行机器网卡的流量（包含被业务规则过滤的数据，不包含时间过滤的数据）
  - `extractor_rps_avg`, `extractor_rps_max`, `extractor_rps_min`：每秒记录数
  - `extractor_bps_avg`, `extractor_bps_max`, `extractor_bps_min`：每秒字节数
  - 注意：目前统计得还不太准确

- `extractor_pushed_rps_*` / `extractor_pushed_bps_*`：数据经过处理后推送到 pipeline 的流量，已转为 DtData
  - `extractor_pushed_rps_avg`, `extractor_pushed_rps_max`, `extractor_pushed_rps_min`：每秒记录数
  - `extractor_pushed_bps_avg`, `extractor_pushed_bps_max`, `extractor_pushed_bps_min`：每秒字节数

通过对比这两组指标，可以观察到过滤规则的实际效果。

<br/>

- record_count

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，每秒平均推送数据条数 |
| sum | 窗口内，总计推送数据条数 |
| max_by_sec | 窗口内，单秒推送数据条数峰值 |

<br/>

- data_bytes

| 聚合方式   | 说明                           |
| :--------- | :----------------------------- |
| avg_by_sec | 窗口内，每秒平均推送数据 bytes |
| sum        | 窗口内，总计推送数据 bytes     |
| max_by_sec | 窗口内，单秒推送数据 bytes 峰值 |

<br/>

- extracted_record_count

| 聚合方式   | 说明                                           |
| :--------- | :--------------------------------------------- |
| avg_by_sec | 窗口内，每秒平均拉取并处理的数据条数 |
| sum        | 窗口内，总计拉取并处理的数据条数（包含被过滤数据） |
| max_by_sec | 窗口内，单秒拉取并处理的数据条数峰值（包含被过滤数据） |

<br/>

- extracted_data_bytes

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，每秒平均拉取并处理的数据 bytes（包含被过滤数据） |
| sum | 窗口内，总计拉取并处理的数据 bytes（包含被过滤数据） |
| max_by_sec | 窗口内，单秒拉取并处理的数据 bytes 峰值（包含被过滤数据） |

## sinker

### monitor.log

```
2024-02-29 01:25:09.554461 | sinker | rt_per_query | avg=3369 | sum=23585 | max=6408
2024-02-29 01:25:09.554503 | sinker | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554544 | sinker | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586
2024-02-29 01:25:09.554582 | sinker | records_per_query | avg=1 | sum=13 | max=2
```

### counter 说明

| counter | 窗口类型 | 说明 |
| :-------- | :-------- | :-------- |
| rt_per_query | 时间窗口 | 单次写入耗时，单位：微秒 |
| records_per_query | 时间窗口 | 单次写入数据条数 |
| record_count | 时间窗口 | 写入数据条数 |
| data_bytes | 时间窗口 | 写入数据 bytes |

<br/>

- rt_per_query

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg | 窗口内，平均单次写入耗时 |
| sum | 窗口内，写入目标的总耗时 |
| max | 窗口内，单次写入目标的最大耗时 |

<br/>

- record_count

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，每秒平均写入数据条数 |
| sum | 窗口内，总计写入数据条数 |
| max_by_sec | 窗口内，单秒写入数据条数峰值 |

<br/>

- data_bytes

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，每秒平均写入 bytes |
| sum | 窗口内，总计写入 bytes |
| max_by_sec | 窗口内，单秒写入 bytes 峰值 |

<br/>

- records_per_query

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg | 窗口内，每次写入数据条数平均值 |
| sum | 窗口内，总计写入数据条数 |
| max | 窗口内，单次写入数据条数峰值 |


## pipeline
### monitor.log
```
2024-02-29 01:25:09.554348 | pipeline | record_size | avg=45
2024-02-29 01:25:09.554387 | pipeline | buffer_size | avg=3 | sum=13 | max=4
2024-02-29 01:25:09.554423 | pipeline | sinked_count | latest=13
```

### counter 说明

| counter | 窗口类型 | 说明 |
| :-------- | :-------- | :-------- |
| record_size | 时间窗口 | 单条数据大小，单位：byte |
| buffer_size | 时间窗口 | 当前内存中缓存的数据条数 |
| sinked_count | 无窗口 | 该任务已同步数据条数 |

<br/>

- record_size

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg | 窗口内，平均每条数据大小 |

<br/>

- buffer_size

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg | 窗口内，平均缓存的数据条数 |
| sum | 窗口内，总共缓存的数据条数 |
| max | 窗口内，最大缓存的数据条数 |

<br/>

- sinked_count

| 聚合方式 | 说明 |
| :-------- | :-------- |
| latest | 该任务已同步数据条数 |
