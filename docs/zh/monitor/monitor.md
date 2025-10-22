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
| avg_by_sec | 所有 sub counter 的总和 / 时间窗口 | 最近 10s 内，平均每秒写入目标库的数据条数 |
| max | sub counter 中具有最大值的那一个 | 最近 10s 内，单次写入目标库的最大数据条数 |
| max_by_sec | 将 sub counter 按时间顺序分布到每一秒，并对属于同一秒的 sub counter 各自取和，然后找出具有最大取和的那一秒的数据 | 单秒内从源库拉取的最大数据条数 |

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
2024-02-29 01:25:09.554271 | extractor | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554311 | extractor | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586
```

### counter 说明
| counter | counter 类型 | 说明 |
| :-------- | :-------- | :-------- |
| record_count | 时间窗口 | 拉取数据条数 |
| data_bytes | 时间窗口 | 拉取数据 bytes |

<br/>

- record_count

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，平均每秒拉取数据条数 |
| sum | 窗口内，总共拉取数据条数 |
| max_by_sec | 窗口内，每秒最大拉取数据条数 |

任务指标中同时会提供两组吞吐相关的指标：

- `extractor_rps_*` / `extractor_bps_*`：记录抽取端在过滤前的原始行数和字节速率。
- `extractor_pushed_rps_*` / `extractor_pushed_bps_*`：记录过滤后、准备进入队列的数据量。

<br/>

- data_bytes

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，每秒平均拉取数据 bytes |
| sum | 窗口内，总共拉取数据 bytes |
| max_by_sec | 窗口内，每秒最大拉取数据 bytes |

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
| avg_by_sec | 窗口内，平均每秒写入数据条数 |
| sum | 窗口内，写入总条数 |
| max_by_sec | 窗口内，最大每秒写入数据条数 |

<br/>

- data_bytes

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg_by_sec | 窗口内，平均每秒写入 bytes |
| sum | 窗口内，写入总 bytes |
| max_by_sec | 窗口内，最大每秒写入 bytes |

<br/>

- records_per_query

| 聚合方式 | 说明 |
| :-------- | :-------- |
| avg | 窗口内，平均每次写入数据条数 |
| sum | 窗口内，写入总条数 |
| max | 窗口内，最大每次写入数据条数 |


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