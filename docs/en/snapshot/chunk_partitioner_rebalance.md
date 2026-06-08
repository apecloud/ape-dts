# Snapshot Chunk Partitioner Rebalance

`ChunkPartitioner` is the downstream partitioning strategy used by snapshot writes. Its main goal is to reduce sink-side long tails.

It only affects the DML write queue when `[parallelizer].parallel_type=snapshot`. It does not change source-side extraction, and it does not rewrite snapshot chunk ids used by checkpointing:

- `[extractor].parallel_type` / `[extractor].parallel_size` control source-side snapshot extraction concurrency.
- `[extractor].batch_size` controls source-side fetch batch size. In extractor chunk mode, it is also the target chunk size.
- `[parallelizer].parallel_size` controls downstream sink concurrency.
- `[sinker].batch_size` controls how many rows each sinker writes per internal batch.
- chunk partitioner rebalance only turns snapshot insert rows already in the pipeline into a partition queue that is easier for sinkers to consume dynamically.

## How It Works

After snapshot parallelizer receives a batch of `RowData`, `ChunkPartitioner` first groups rows by `schema.table.chunk_id`. Rebalance strategies work on top of those logical groups:

- They can sort partitions by cost, largest first, so large partitions are scheduled earlier.
- They can split an oversized snapshot insert chunk into multiple contiguous sub-partitions when it is safe.
- Rows-only strategies can sort chunks by `schema.table.chunk_id`, merge chunks from the same table, then cut the merged table group into row-count based partitions.
- Splitting does not modify the original `chunk_id` on each row and does not create new checkpoint chunks.

Splitting is enabled only for pure snapshot `Insert` DML. Mixed DML containing `Update` or `Delete` automatically falls back to keeping logical chunks intact.

## Configuration

Configure it under `[parallelizer]`:

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
rebalance_cost=rows
rebalance_max_partitions_per_sinker=2
rebalance_min_partition_rows=200
rebalance_split_skew_ratio=1.0
```

| Config | Description | Default |
| :--- | :--- | :--- |
| `rebalance_strategy` | snapshot chunk rebalance strategy | `none` |
| `rebalance_cost` | cost metric used to measure partition size | `rows` |
| `rebalance_max_partitions_per_sinker` | max split partitions per effective sinker | `2` |
| `rebalance_min_partition_rows` | minimum rows kept in each split partition | `[sinker].batch_size` |
| `rebalance_split_skew_ratio` | skew threshold used by the auto_split strategy | `1.0` |

`rebalance_max_partitions_per_sinker` defaults to `2`. Setting it to `0` is invalid.

`rebalance_min_partition_rows` defaults to `[sinker].batch_size` so that split partitions do not become much smaller than the sinker's own write batch. Setting it to `0` is invalid.

### rebalance_strategy

| Value | Behavior | Best For |
| :--- | :--- | :--- |
| `none` | Default. Keeps first-seen logical chunk order after grouping; no sorting or splitting | Debugging, conservative behavior, or tasks without obvious sink-side long tails |
| `auto_split` | Sorts by cost; splits pure insert chunks only when there are too few partitions or the largest partition is clearly skewed | Snapshot write tasks with obvious sink-side long tails |
| `chunk_largest_first` | Sorts logical chunks by cost, largest first; does not split logical chunks | Keeping chunk integrity while scheduling large chunks first |
| `table_min_rows` | Sorts chunks by `schema.table.chunk_id`, merges chunks from the same table, then cuts each merged group by `rebalance_min_partition_rows` | Table-level row-count partitioning with predictable partition size |
| `table_even` | Sorts and merges chunks from the same table, processes larger merged groups first, and splits a merged group into up to `[parallelizer].parallel_size` nearly even partitions only when it has at least `[parallelizer].parallel_size * rebalance_min_partition_rows` rows | More even sink work within large tables while keeping small table groups intact |

### rebalance_cost

| Value | Behavior | Best For |
| :--- | :--- | :--- |
| `rows` | Uses row count as the cost metric | Default. Most MySQL/PG snapshot tasks where row width is similar |
| `bytes` | Uses estimated row bytes as primary cost and row count as tie-breaker | Tasks with large JSON, LOB, wide strings, or highly uneven row width |

`rows` is cheaper and matches row-count based batch writing well. `bytes` can better detect wide-row cost, but it requires scanning row data size and has higher CPU overhead in the partitioner.

`table_min_rows` and `table_even` ignore `rebalance_cost`; they use row counts only.

### rebalance_max_partitions_per_sinker

This controls the hard cap for split partitions:

```text
max partitions = effective sinkers * rebalance_max_partitions_per_sinker
```

The partitioner also applies the `rebalance_min_partition_rows` batch-derived cap, and uses the smaller value.

Recommendations:

- Keep the default `2` for most tasks.
- Use `1` when target-side request count or scheduling overhead needs a tighter bound.
- Increase it when a few very large chunks still cause long tails after tuning `rebalance_min_partition_rows`.

### rebalance_min_partition_rows

This controls the lower bound of split granularity. It is not the sinker batch size, but it defaults to `[sinker].batch_size`.

For `table_min_rows`, this is the target size of each partition, except the last partition of a merged table group may be smaller. For `table_even`, a merged group with fewer than `[parallelizer].parallel_size * rebalance_min_partition_rows` rows is kept as one partition; larger groups are split into nearly even partitions aligned near multiples of this value when possible.

Recommendations:

- Keep the default for most tasks.
- If sink-side long tail is obvious, lower it moderately, for example to `[sinker].batch_size / 2`, so large chunks can be split more finely.
- For HTTP/stream-load sinks, or any target with high request overhead, keep it larger to avoid too many small requests.
- Avoid values below `50` unless you are debugging long tails or processing very small data sets.

### rebalance_split_skew_ratio

This only affects `auto_split`. It means:

```text
largest partition cost > average cost per sinker * rebalance_split_skew_ratio
```

When the condition is met, `auto_split` continues splitting the largest insert partition.

Recommendations:

- `1.0`: default, aggressive enough to split clear sink-side long tails.
- `1.5`: more conservative, useful when the target has higher request overhead.
- `3.0` or higher: more conservative, useful for request-heavy sinks or targets under connection/lock pressure.

## Recommended Configurations

### General Snapshot Writes

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
rebalance_cost=rows
```

This is the default behavior. It keeps logical chunk order after grouping and does not add sink-side sorting or splitting.

If sink-side long tails are obvious, enable `auto_split` and tune from there.

### Large Single Table with Uneven Chunks

```ini
[extractor]
parallel_type=chunk
parallel_size=4
batch_size=10000

[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=auto_split
rebalance_cost=rows
rebalance_split_skew_ratio=1.5
```

Use this when source-side chunk extraction is already enabled but some chunks are still much larger than others. Tune extractor chunking first, then use sink-side rebalance to reduce write long tails.

### Uneven Row Width

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=auto_split
rebalance_cost=bytes
```

Use this when the same batch contains large JSON, LOB, wide strings, or other rows with very different write cost. `bytes` schedules wide rows more accurately, at the cost of more partitioner CPU.

### Targets with High Request Overhead

```ini
[sinker]
batch_size=1000

[parallelizer]
parallel_type=snapshot
parallel_size=4
rebalance_strategy=chunk_largest_first
rebalance_cost=rows
```

Use this for StarRocks, Doris, ClickHouse, or other HTTP/stream-load style sinks, or when the target is sensitive to small requests. Sorting without splitting reduces extra request count.

### Row-Count Partitions per Table

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=table_min_rows
rebalance_min_partition_rows=2000
```

Use this when chunks from the same table should be merged and emitted as predictable row-count partitions. Chunks are sorted by `schema.table.chunk_id` first, so non-contiguous chunk ids from the same table can still be merged into one table group.

### Even Partitions per Table

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=table_even
rebalance_min_partition_rows=2000
```

Use this when large tables should be split into a small number of similarly sized partitions while small table groups stay intact. Larger merged groups are planned first. Each large merged table group can produce up to `[parallelizer].parallel_size` partitions, so the total partition count can exceed `[parallelizer].parallel_size` when one batch contains multiple large tables.

### Severe Long Tail with Automatic Split

```ini
[sinker]
batch_size=200

[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=auto_split
rebalance_cost=rows
rebalance_min_partition_rows=200
```

Use this when one logical chunk is very large and keeps one sinker busy for much longer than others. `auto_split` fills sinker concurrency first, then continues splitting only while the largest partition is clearly skewed.

### Debugging or Most Conservative Behavior

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
```

Use this when debugging row order, checkpoint behavior, or target writes. It has the most obvious long-tail risk.

## Tuning Order

If a snapshot task is slow, check in this order:

1. Source extraction is slow: tune `[extractor].parallel_type`, `[extractor].parallel_size`, `[extractor].batch_size`, and partition columns first.
2. Sink concurrency is too low: tune `[parallelizer].parallel_size`, and make sure `[sinker].max_connections` is not below active sinker demand.
3. Sink-side long tail is obvious: tune chunk partitioner rebalance, for example use `auto_split`, lower `rebalance_split_skew_ratio`, or switch to `rebalance_cost=bytes`.
4. Target request count or RT becomes worse: increase `[sinker].batch_size` / `rebalance_min_partition_rows`, or switch to `chunk_largest_first`.

## Notes

- This feature is mainly for snapshot writes. It is not intended to solve CDC update/delete ordering problems.
- Rebalance does not increase source extraction concurrency. If extraction is slow, tune extractor settings first.
- Splitting does not modify the original row `chunk_id` and does not create new checkpoint chunks.
- Output partition count is not fixed to `[parallelizer].parallel_size`. The base parallelizer dynamically assigns pending partitions to available sinkers.
- Too small `rebalance_min_partition_rows` can increase SQL building, HTTP requests, monitor updates, and Vec split overhead.
