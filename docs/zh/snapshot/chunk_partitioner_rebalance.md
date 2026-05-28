# Snapshot Chunk Partitioner Rebalance

`ChunkPartitioner` 是 snapshot 写入阶段的下游分片策略，主要用于缓解目标端写入长尾。

它只影响 `[parallelizer].parallel_type=snapshot` 的 DML 写入队列，不改变源端全量拉取方式，也不改变 checkpoint 里的 snapshot chunk id：

- `[extractor].parallel_type` / `[extractor].parallel_size` 控制源端如何并发拉取数据。
- `[extractor].batch_size` 控制源端拉取 batch 大小；在 extractor chunk 模式下也作为目标 chunk 大小。
- `[parallelizer].parallel_size` 控制目标端 sinker 写入并发。
- `[sinker].batch_size` 控制单个 sinker 内部每次批量写入的行数。
- chunk partitioner rebalance 只负责把已经进入 pipeline 的 snapshot insert rows 拆成更适合 sinker 动态调度的 partition 队列。

## 工作方式

snapshot parallelizer 收到一批 `RowData` 后，`ChunkPartitioner` 会先按 `schema.table.chunk_id` 分组。不同 rebalance 策略在这个分组结果上工作：

- 可以按成本从大到小排序，让大 partition 先进入 sinker。
- 可以在安全条件满足时，把过大的 snapshot insert chunk 拆成多个连续子 partition。
- 拆分时不会修改每行原始 `chunk_id`，也不会生成新的 checkpoint chunk。

拆分只对纯 `Insert` 的 snapshot DML 启用。包含 `Update` / `Delete` 的混合 DML 会自动降级为不拆分 logical chunk，避免破坏顺序语义。

## 配置项

配置位于 `[parallelizer]`：

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=adaptive
rebalance_cost=rows
rebalance_min_partition_rows=200
rebalance_split_skew_ratio=2.0
```

| 配置 | 作用 | 默认 |
| :--- | :--- | :--- |
| `rebalance_strategy` | snapshot chunk rebalance 策略 | `adaptive` |
| `rebalance_cost` | 判断 partition 大小的成本口径 | `rows` |
| `rebalance_min_partition_rows` | 拆分后单个 partition 的最小行数 | `[sinker].batch_size` |
| `rebalance_split_skew_ratio` | adaptive 策略下判定大 chunk 明显倾斜的阈值 | `2.0` |

`rebalance_min_partition_rows` 默认跟随 `[sinker].batch_size`，目的是避免拆出比 sinker 批量写入还小很多的 partition。配置为 `0` 会报错。

### rebalance_strategy

| 取值 | 行为 | 适合场景 |
| :--- | :--- | :--- |
| `adaptive` | 默认策略。按成本排序；partition 太少或最大 partition 明显倾斜时，拆分纯 insert 大 chunk | 推荐大多数 snapshot 写入任务使用 |
| `chunk_largest_first` | 只按成本从大到小排序，不拆分 logical chunk | 希望保持 chunk 完整、但想让大 chunk 先写的任务 |
| `split_large_insert` | 只要安全且未达到上限，就持续拆分大 insert chunk | 单个或少数 chunk 特别大、目标端写入长尾明显的任务 |
| `none` | 按 logical chunk 分组后保持首次出现顺序，不排序、不拆分 | 排查问题、极端保守场景 |

### rebalance_cost

| 取值 | 行为 | 适合场景 |
| :--- | :--- | :--- |
| `rows` | 以行数作为成本 | 默认值。大多数行宽接近的 MySQL/PG snapshot 任务 |
| `bytes` | 以估算字节数作为主要成本，行数作为 tie-breaker | 行宽差异明显、包含大 JSON/LOB/宽字段的任务 |

`rows` 的开销更低，也更贴近批量写入的行数粒度。`bytes` 更能识别宽行带来的写入成本，但 partitioner 需要扫描行的 data size，CPU 开销更高。

### rebalance_min_partition_rows

这个参数控制拆分粒度下限。它不是 sinker 的 batch size，但默认等于 `[sinker].batch_size`。

建议：

- 一般保持默认。
- 如果目标端写入长尾明显，可以适当调小，例如设置为 `[sinker].batch_size / 2`，让大 chunk 拆得更细。
- 如果目标端是 HTTP/stream load 类 sinker，或者请求开销较高，应保持较大值，避免小 partition 造成请求数过多。
- 不建议小于 `50`，除非是专门排查长尾或小数据量任务。

### rebalance_split_skew_ratio

仅影响 `adaptive` 策略。含义是：

```text
最大 partition 成本 > 平均每个 sinker 成本 * rebalance_split_skew_ratio
```

满足条件时，adaptive 会继续拆分最大 insert partition。

建议：

- 默认 `2.0`：偏保守，适合多数任务。
- `1.5`：更积极，适合长尾明显、目标端能承受更多小任务的场景。
- `3.0` 或更高：更保守，适合请求型 sinker 或目标端连接/锁等待压力较大的场景。

## 推荐配置

### 通用 snapshot 写入

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=adaptive
rebalance_cost=rows
```

这是推荐默认组合。它会优先保持较少任务数，只在 chunk 数量不足或明显倾斜时拆分。

### 单表大数据，chunk 分布不均

```ini
[extractor]
parallel_type=chunk
parallel_size=4
batch_size=10000

[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=adaptive
rebalance_cost=rows
rebalance_split_skew_ratio=1.5
```

适合源端已经启用 chunk 拉取，但部分 chunk 仍明显更大的场景。先调好 extractor 的 chunk 切分，再通过 sink 侧 rebalance 缓解写入长尾。

### 行宽差异明显

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=adaptive
rebalance_cost=bytes
```

适合同一批数据中存在大 JSON、LOB、宽字符串等情况。`bytes` 能更准确地把宽行成本排到前面，但 CPU 成本高于 `rows`。

### 目标端请求成本高

```ini
[sinker]
batch_size=1000

[parallelizer]
parallel_type=snapshot
parallel_size=4
rebalance_strategy=chunk_largest_first
rebalance_cost=rows
```

适合 StarRocks、Doris、ClickHouse 等 HTTP/stream load 类写入，或目标端对小请求敏感的场景。只排序不拆分，可以减少额外请求数。

### 长尾非常明显，且目标端可承受更多并发任务

```ini
[sinker]
batch_size=200

[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=split_large_insert
rebalance_cost=rows
rebalance_min_partition_rows=200
```

适合单个 logical chunk 特别大，导致某个 sinker 长时间独占的任务。这个策略更激进，可能增加调度和写入请求开销，不建议作为普通默认。

### 排查问题或需要最保守行为

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
```

适合排查数据顺序、checkpoint、目标端写入行为时使用。性能长尾最明显。

## 调优顺序

如果 snapshot 总体慢，建议按下面顺序判断：

1. 源端拉取慢：优先调 `[extractor].parallel_type`、`[extractor].parallel_size`、`[extractor].batch_size` 和 partition column。
2. 目标端并发不足：调 `[parallelizer].parallel_size`，同时确认 `[sinker].max_connections` 不低于活跃 sinker 需求。
3. 写入阶段长尾明显：调 chunk partitioner rebalance，例如使用 `adaptive`、降低 `rebalance_split_skew_ratio` 或切换 `rebalance_cost=bytes`。
4. 目标端请求数过多或 RT 变差：增大 `[sinker].batch_size` / `rebalance_min_partition_rows`，或改用 `chunk_largest_first`。

## 注意事项

- 该功能主要给 snapshot 使用，不适合用来处理 CDC 的 update/delete 顺序问题。
- rebalance 不会提高源端拉取并发；源端慢时应先调 extractor。
- 拆分不会修改 row 的原始 `chunk_id`，不会产生新的 checkpoint chunk。
- 输出 partition 数不是固定等于 `[parallelizer].parallel_size`。base parallelizer 会让空闲 sinker 动态取下一个 pending partition。
- 过小的 `rebalance_min_partition_rows` 可能增加 SQL 构建、HTTP 请求、monitor 更新和 Vec 拆分开销。
