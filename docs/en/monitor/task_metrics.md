# Task metrics reference

Task metrics summarize extractor, pipeline, sinker, and checker activity at task
level. They are available through two outputs:

- `task.log`: emitted as a JSON object by `TaskMonitor`. JSON field names use
  `snake_case`, as listed in the **Task log field** column below. This output is
  available whether or not the `metrics` crate feature is enabled.
- Prometheus: when the `metrics` feature is enabled, the current values are
  exposed as gauges at `GET /metrics`. `GET /healthz` reports the health of the
  metrics HTTP service.

The metrics HTTP address, workers, and constant labels are configured in the
`[metrics]` section.

Parallel utilization, batch duration, partitioner duration, and the separate
`pipeline_sink_operations_total` are also logged with the existing counters under
`pipeline | <pipeline_id>` in `monitor.log`. Snapshot tasks additionally report
`pipeline | global` aggregates. Task JSON and Prometheus remain task aggregates.
See [pipeline monitoring](monitor.md#pipeline) for the format and no-sample behavior.

## Collection and aggregation

- Task metrics are refreshed when `TaskMonitor` is flushed. In the normal
  pipeline flow, the refresh interval is controlled by
  `[pipeline] checkpoint_interval_secs`.
- Utilization, batch duration/count, and partitioner duration accumulate for the
  current task run. The pipeline monitor remains registered through the final
  flush; window settings do not apply.
- Throughput and response-time metrics use the rolling window configured by
  `[pipeline] counter_time_window_secs`.
- A time-window counter retains at most
  `[pipeline] counter_max_sub_count` samples. At higher event rates, its
  statistics cover the newest retained samples in the window.
- For throughput and response-time metrics, `max`, `min`, and `avg` describe
  the maximum, minimum, and arithmetic mean of per-second values containing
  samples in the current window. Seconds without samples are excluded.
- Existing metrics use integers and truncate division. Utilization, batch duration,
  and partitioner duration retain fractional values.
- Sinker RPS/BPS and cumulative RT per second align all participating sinker
  monitors to one sampling instant, sum each second, then calculate min/max/avg.
  RPS/BPS use `RecordCount`/`DataBytes`. Task metrics still filter completed-table
  monitors; other components retain their existing incremental task aggregation.
  Batched RT samples use the metric submission time, which can differ from the
  actual database request completion time.
- Global window statistics in `monitor.log` merge retained samples: per-call
  averages use total value / total count, extrema cover all valid samples, and
  rates sum aligned seconds before calculating min/max/avg. Unexpired samples
  from completed tables still participate in global statistics, so global and
  task metrics can differ when tables finish.
- A task-log field is present only after its source counter has been populated.
  A registered Prometheus gauge is `0` until a value is published. Existing
  gauges can retain their last value when a field is absent.

## Extractor metrics

`extractor_*` measures traffic extracted from the source. The current
source-side byte/record accounting may not include every byte transferred by
the database protocol. `extractor_pushed_*` measures the `DtData` records that
remain after processing and filtering and are pushed to the pipeline.

| Task log field             | Prometheus metric          | Unit      | Meaning                                                                        |
| -------------------------- | -------------------------- | --------- | ------------------------------------------------------------------------------ |
| `extractor_rps_max`        | `extractor_rps_max`        | records/s | Highest source extraction rate in one sampled second of the window.            |
| `extractor_rps_min`        | `extractor_rps_min`        | records/s | Lowest source extraction rate in one sampled second of the window.             |
| `extractor_rps_avg`        | `extractor_rps_avg`        | records/s | Average source extraction rate across sampled seconds of the window.           |
| `extractor_bps_max`        | `extractor_bps_max`        | bytes/s   | Highest source extraction byte rate in one sampled second of the window.       |
| `extractor_bps_min`        | `extractor_bps_min`        | bytes/s   | Lowest source extraction byte rate in one sampled second of the window.        |
| `extractor_bps_avg`        | `extractor_bps_avg`        | bytes/s   | Average source extraction byte rate across sampled seconds of the window.      |
| `extractor_pushed_rps_max` | `extractor_pushed_rps_max` | records/s | Highest rate of records pushed to the pipeline after processing and filtering. |
| `extractor_pushed_rps_min` | `extractor_pushed_rps_min` | records/s | Lowest rate of records pushed to the pipeline after processing and filtering.  |
| `extractor_pushed_rps_avg` | `extractor_pushed_rps_avg` | records/s | Average rate of records pushed to the pipeline after processing and filtering. |
| `extractor_pushed_bps_max` | `extractor_pushed_bps_max` | bytes/s   | Highest byte rate pushed to the pipeline after processing and filtering.       |
| `extractor_pushed_bps_min` | `extractor_pushed_bps_min` | bytes/s   | Lowest byte rate pushed to the pipeline after processing and filtering.        |
| `extractor_pushed_bps_avg` | `extractor_pushed_bps_avg` | bytes/s   | Average byte rate pushed to the pipeline after processing and filtering.       |
| `extractor_plan_records`   | `extractor_plan_records`   | records   | Source records estimated by the snapshot extraction plan. Snapshot tasks only. |

## Pipeline metrics

| Task log field         | Prometheus metric      | Unit              | Meaning                                                                                                                                    |
| ---------------------- | ---------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `pipeline_queue_size`  | `pipeline_queue_size`  | records           | Current number of records buffered in the pipeline queue.                                                                                  |
| `pipeline_queue_bytes` | `pipeline_queue_bytes` | bytes             | Current estimated bytes buffered in the pipeline queue.                                                                                    |
| `timestamp`            | `timestamp`            | Unix milliseconds | Greatest source-position timestamp observed by the pipeline. CDC tasks only. A value of `0` means the position has no parseable timestamp. |

## Partitioner metrics

Currently only `ChunkPartitioner::partition_dml` calls from `SnapshotParallelizer`
are measured. One sample P covers the wall-clock duration of chunk grouping,
rebalance, and partition materialization. Input size accounting, sink dispatch,
mutex waits, writes, and checkpoints are outside this interval.

| Task log / Prometheus field | Unit | Meaning |
| --- | --- | --- |
| `partitioner_duration_seconds_sum` | seconds | Total partition call duration since task start, `sum(P)`. |
| `partitioner_duration_seconds_min` | seconds | Shortest partition call since task start. |
| `partitioner_duration_seconds_max` | seconds | Longest partition call since task start. |
| `partitioner_duration_seconds_avg` | seconds | Arithmetic mean per call, `sum(P) / partition_call_count`. |

All four fields are floating-point gauges in seconds, accumulated since the current
task run started. They are independent of `counter_time_window_secs` and
`counter_max_sub_count`. Each pipeline stores sums, counts, and extrema instead of
historical samples. Task metrics read the pipeline counters through the final flush.
A new task run starts from zero; checkpoint recovery does not restore these statistics.
The internal call count is independent of `pipeline_sink_operations_total` and is not exported.
Empty input, error returns, and panic unwinding also record elapsed time. A subsequent
sink failure does not discard a completed partition sample. Zero durations participate
in both the average and minimum.

Once measurement is enabled, all four fields are zero until the first sample.
Recorded values persist throughout the task run. Tasks without measurement omit
these JSON fields; registered Snapshot Prometheus gauges remain zero. Raw partitioning, other partitioners,
and CDC paths are not measured yet.

## Sinker metrics

| Task log field                 | Prometheus metric              | Unit          | Meaning                                                                                                                                                                                                                                                                     |
| ------------------------------ | ------------------------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sinker_rps_max`               | `sinker_rps_max`               | records/s     | Highest sink write rate in one sampled second of the window.                                                                                                                                                                                                                |
| `sinker_rps_min`               | `sinker_rps_min`               | records/s     | Lowest sink write rate in one sampled second of the window.                                                                                                                                                                                                                 |
| `sinker_rps_avg`               | `sinker_rps_avg`               | records/s     | Average sink write rate across sampled seconds of the window.                                                                                                                                                                                                               |
| `sinker_bps_max`               | `sinker_bps_max`               | bytes/s       | Highest sink write byte rate in one sampled second of the window.                                                                                                                                                                                                           |
| `sinker_bps_min`               | `sinker_bps_min`               | bytes/s       | Lowest sink write byte rate in one sampled second of the window.                                                                                                                                                                                                            |
| `sinker_bps_avg`               | `sinker_bps_avg`               | bytes/s       | Average sink write byte rate across sampled seconds of the window.                                                                                                                                                                                                          |
| `sinker_rt_max`                | `sinker_rt_max`                | milliseconds  | Highest per-second sum of recorded sink-operation response times in the window. This is not a per-request latency percentile.                                                                                                                                               |
| `sinker_rt_min`                | `sinker_rt_min`                | milliseconds  | Lowest per-second sum of recorded sink-operation response times in the window.                                                                                                                                                                                              |
| `sinker_rt_avg`                | `sinker_rt_avg`                | milliseconds  | Average per-second sum of recorded sink-operation response times across sampled seconds.                                                                                                                                                                                    |
| `sinker_workers_configured`    | `sinker_workers_configured`    | workers       | Number of sinker instances registered for the task.                                                                                                                                                                                                                         |
| `sinker_workers_busy`          | `sinker_workers_busy`          | workers       | Number of registered sinkers currently executing a tracked sinker operation. Tracked operations include data writes, metadata refresh, and control-item processing such as table-finish handling; `close` is not tracked. This is a point-in-time value sampled at refresh. |
| `sinker_workers_per_drain_max` | `sinker_workers_per_drain_max` | workers/drain | Maximum number of distinct sinkers that received non-empty business data in one pipeline drain during the current window.                                                                                                                                                   |
| `sinker_workers_per_drain_avg` | `sinker_workers_per_drain_avg` | workers/drain | Average number of distinct sinkers that received non-empty business data per pipeline drain during the current window.                                                                                                                                                      |
| `sinker_sinked_records`        | `sinker_sinked_records`        | records       | Cumulative number of records successfully written to the target.                                                                                                                                                                                                            |
| `sinker_sinked_bytes`          | `sinker_sinked_bytes`          | bytes         | Cumulative estimated bytes successfully written to the target.                                                                                                                                                                                                              |
| `sinker_ddl_count`             | `sinker_ddl_count`             | operations    | Cumulative number of DDL operations processed by the sink side. CDC tasks only.                                                                                                                                                                                             |

### DML batch utilization and duration

A batch is one `BaseParallelizer::sink_dml` invocation and includes all its
partitions. A partition can execute multiple SQL requests. Timing starts at
dispatch, after queue consumption by `drain()`. Existing
`sinker_workers_per_drain_*` names are retained for compatibility.

These eight metrics measure successful, nonempty DML batches through
`BaseParallelizer::sink_dml`, including snapshot, table, partition, and serial
parallelizers in Snapshot or CDC tasks. Paths that call sinkers directly, such as
`MergeParallelizer`, are not instrumented yet.
`K = min(parallel_size, sinkers.len())`, using the concurrency limit passed to
`BaseParallelizer::sink_dml` (1 for serial execution). Internal W sums the time spent
inside all nonempty `sink_dml` calls after acquiring the sinker mutex, including
connection-pool waits, internal retries, and checker waits. D spans dispatch through
completion of every partition, including mutex waits and scheduling but excluding
partition/rebalance computation and checkpoints. Each batch has `U = W / (K * D)`.

| Task log / Prometheus field | Unit | Meaning |
| --- | --- | --- |
| `pipeline_sink_parallel_utilization_avg` | 0..1 | Arithmetic mean of U across all valid pipeline sink operations since task start, `sum(U) / N`. |
| `pipeline_sink_parallel_utilization_min` | 0..1 | Lowest U since task start. |
| `pipeline_sink_parallel_utilization_max` | 0..1 | Highest U since task start. |
| `pipeline_sink_duration_seconds_sum` | seconds | Sum of pipeline sink operation wall-clock durations since task start, `sum(D)`. |
| `pipeline_sink_duration_seconds_min` | seconds | Shortest pipeline sink operation wall-clock duration since task start. |
| `pipeline_sink_duration_seconds_max` | seconds | Longest pipeline sink operation wall-clock duration since task start. |
| `pipeline_sink_duration_seconds_avg` | seconds | Average pipeline sink operation wall-clock duration, `sum(D) / N`. |
| `pipeline_sink_operations_total` | batches | Number of valid pipeline sink operations N since task start. |

All fields accumulate statistics for the current task run and use Prometheus gauges.
Utilization, batch durations, partitioner durations, and batch totals all use the
common no-window counters. `pipeline_sink_operations_total` increments once per valid pipeline sink operation and
emits a separate `pipeline_sink_operations_total | latest=N` line in monitor.log.

Neither `counter_time_window_secs` nor `counter_max_sub_count` applies.
Completed batches update sums, counts, and extrema without retaining individual
samples. The task's single pipeline monitor remains registered through the final
flush. Count and sums accumulate during the run; min/max/avg cover all valid samples.
A new task run starts from zero; checkpoint recovery does not restore these statistics.
Utilization averages give each batch equal weight. W is internal and is not exported.
`sum(D)` is the total elapsed time of successful sink operations in the pipeline.

Empty, failed, canceled, non-DML, and invalid measurements do not contribute to N.
Once measurement is enabled, N=0 produces zero for all eight fields.
Idle time and table completion do not reset recorded values. Use N to distinguish
no samples from valid zero utilization. Tasks without measurement omit these JSON fields; registered
Prometheus gauges remain zero. For K=4, partitions taking 100/10/10/10 ms give U=0.325;
only two 100 ms partitions give U=0.5. High utilization can include connection-pool
waits, so interpret it alongside D, throughput, and destination latency.

## Checker metrics

Checker metrics are populated only when a data checker is running.

| Task log field         | Prometheus metric      | Unit      | Meaning                                                             |
| ---------------------- | ---------------------- | --------- | ------------------------------------------------------------------- |
| `checker_miss_count`   | `checker_miss_total`   | records   | Cumulative number of records missing from the target.               |
| `checker_diff_count`   | `checker_diff_total`   | records   | Cumulative number of records whose source and target values differ. |
| `checker_pending`      | `checker_queue_size`   | records   | Current number of unresolved records tracked by the checker.        |
| `checker_rps_max`      | `checker_rps_max`      | records/s | Highest check rate in one sampled second of the window.             |
| `checker_rps_min`      | `checker_rps_min`      | records/s | Lowest check rate in one sampled second of the window.              |
| `checker_rps_avg`      | `checker_rps_avg`      | records/s | Average check rate across sampled seconds of the window.            |
| `checker_miss_rps_max` | `checker_miss_rps_max` | records/s | Highest missing-record rate in one sampled second of the window.    |
| `checker_miss_rps_min` | `checker_miss_rps_min` | records/s | Lowest missing-record rate in one sampled second of the window.     |
| `checker_miss_rps_avg` | `checker_miss_rps_avg` | records/s | Average missing-record rate across sampled seconds of the window.   |
| `checker_diff_rps_max` | `checker_diff_rps_max` | records/s | Highest differing-record rate in one sampled second of the window.  |
| `checker_diff_rps_min` | `checker_diff_rps_min` | records/s | Lowest differing-record rate in one sampled second of the window.   |
| `checker_diff_rps_avg` | `checker_diff_rps_avg` | records/s | Average differing-record rate across sampled seconds of the window. |

## Snapshot progress metrics

| Task log field            | Prometheus metric | Unit    | Meaning                                                                                                                   |
| ------------------------- | ----------------- | ------- | ------------------------------------------------------------------------------------------------------------------------- |
| `progress`                | `progress`        | percent | Snapshot completion percentage, calculated as `finished_progress_count * 100 / total_progress_count` and capped at `100`. |
| `total_progress_count`    | Not exported      | tables  | Total number of tables used as the snapshot progress denominator.                                                         |
| `finished_progress_count` | Not exported      | tables  | Number of tables counted as finished by the snapshot pipeline.                                                            |

## Reserved fields

`TaskMetricsType` also defines `delay` and `pipeline_record_size_max`. The current
`TaskMonitor` does not populate them and the Prometheus exporter does not
register them. They should not be used for alerts or dashboards until an
implementation is added.
