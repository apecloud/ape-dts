# Monitoring info
Counters are used to record the task status, they will be periodically logged in monitor.log (configuration: [pipeline] checkpoint_interval_secs).

# Time window counters
This type of counter is an array of sub-counters. During task execution, whenever there is a state change (e.g., successfully writing a batch entries to target), a new sub-counter is generated to record the increment info (e.g., number of entries written to target).

- The counter has a time window (configuration: [pipeline] counter_time_window_secs), expired sub-counters will be discarded.
- The counter is used for real-time monitoring, such as the number of synchronized entries in time window.
- The counter has aggregation algorithms, such as the average count of synchronized entries per second.

## Aggregation algorithms

| Aggregation | Description | Example |
| :-------- | :-------- | :-------- | 
| sum | sum of sub-counters | count of synchronized entries in last 10 seconds |
| avg | sum of sub-counters / number of sub-counters | average time cost for each write to target in last 10 seconds |
| avg_by_sec | sum of all sub-counters / time window | average number of entries written to target per second in last 10 seconds |
| max | the sub-counter with the maximum value | maximum number of entries written to target in a single batch in last 10 seconds |
| max_by_sec | sums the sub-counters for each second, and finds the second with the maximum sum | Maximum number of entries written to target in a single second |

# No window counter

A simple counter to record accumulated data, such as the number of migrated MySQL records.

## Aggregation algorithms

| Aggregation | Description | Example |
| :-------- | :-------- | :-------- |
| latest | Current value | Number of synchronized data entries by the task |


# Counter details

## Time window configuration

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

### counters
| Counter | Counter Type | Description |
| :-------- | :-------- | :-------- |
| record_count | time window | Number of data entries pulled |
| data_bytes | time window | Data bytes pulled |

<br/>

- record_count

| Aggregation | Description |
| :-------- | :-------- |
| avg_by_sec | Average number of entries pulled per second in time window |
| sum | Number of entries pulled in time window |
| max_by_sec | Maximum number of entries pulled per second in time window |

Task metrics also expose two groups of throughput gauges:

- `extractor_rps_*` / `extractor_bps_*`: raw rows and bytes pulled from upstream before any task-level filtering.
- `extractor_pushed_rps_*` / `extractor_pushed_bps_*`: rows and bytes that remain after filtering and are ready to be queued.

<br/>

- data_bytes

| Aggregation | Description |
| :-------- | :-------- |
| avg_by_sec | Average data bytes pulled per second in time window |
| sum | Data bytes pulled in time window |
| max_by_sec | Maximum data bytes pulled per second in window |

## sinker

### monitor.log

```
2024-02-29 01:25:09.554461 | sinker | rt_per_query | avg=3369 | sum=23585 | max=6408
2024-02-29 01:25:09.554503 | sinker | record_count | avg_by_sec=13 | sum=13 | max_by_sec=13
2024-02-29 01:25:09.554544 | sinker | data_bytes | avg_by_sec=586 | sum=586 | max_by_sec=586
2024-02-29 01:25:09.554582 | sinker | records_per_query | avg=1 | sum=13 | max=2
```

### counter Description

| counter | Counter Type | Description |
| :-------- | :-------- | :-------- |
| rt_per_query | time window | Time taken for a single write, in microseconds |
| records_per_query | time window | Number of entries per single write |
| record_count | time window | Number of entries written to target |
| data_bytes | time window |Data bytes written to target |

<br/>

- rt_per_query

| Aggregation | Description |
| :-------- | :-------- |
| avg | Average time taken for a single write in window |
| sum | Total time taken for writes to target in window |
| max | Maximum time taken for a single write in window |

<br/>

- record_count

| Aggregation | Description |
| :-------- | :-------- |
| avg_by_sec | Average number of entries written per second in window |
| sum | Total number of entries written in window |
| max_by_sec | Maximum number of entries written per second in window |

<br/>

- data_bytes

| Aggregation | Description |
| :-------- | :-------- |
| avg_by_sec | Average bytes written per second in window |
| sum | Total bytes written in window |
| max_by_sec |Maximum bytes written per second in window |

<br/>

- records_per_query

| Aggregation | Description |
| :-------- | :-------- |
| avg | Average number of entries per query in window |
| sum | Total number of entries written in window |
| max | Maximum number of entries written per query in window |


## pipeline
### monitor.log
```
2024-02-29 01:25:09.554348 | pipeline | record_size | avg=45
2024-02-29 01:25:09.554387 | pipeline | buffer_size | avg=3 | sum=13 | max=4
2024-02-29 01:25:09.554423 | pipeline | sinked_count | latest=13
```

### counter Description

| Counter | Counter Type | Description |
| :-------- | :-------- | :-------- |
| record_size | time window | Size of a single entry, in bytes |
| buffer_size | time window | Number of entries cached in pipeline |
| sinked_count | no window | Total Number of entries handled by task |

<br/>

- record_size

| Aggregation | Description |
| :-------- | :-------- |
| avg | Average size of each entry in window |

<br/>

- buffer_size

| Aggregation | Description |
| :-------- | :-------- |
| avg | Average number of cached entries in window |
| sum | Total number of cached entries in window |
| max | Maximum number of cached entries in window |

<br/>

- sinked_count

| Aggregation | Description |
| :-------- | :-------- |
| latest | Number of entries handled by task |