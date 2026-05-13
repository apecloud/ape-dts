# Multiple Tables in Parallel for Snapshot Task

By default, when a snapshot task includes multiple tables, ape-dts migrates tables one at a time in order, sorted by **database name first, then table name**.

If you have sufficient resources (memory, CPU), you can enable parallel table migration to accelerate.

## Configuration
- With the following configuration, ape-dts will run up to 4 table workers at a time. When any table completes, it will pick the next remaining table, keeping up to 4 tables in progress.

```
[extractor]
parallel_type=table
parallel_size=4
```

`parallel_type=table` means snapshot concurrency is allocated across tables. `parallel_type=chunk` means concurrency is allocated within a single table by chunk splitting. In both modes, `parallel_size` is the effective snapshot concurrency knob.

Deprecated config: `[runtime] tb_parallel_size` is deprecated and should not be used in new configs. It is kept only for backward compatibility, and only as a fallback when `[extractor] parallel_size` is not set.

Legacy example:

```ini
[runtime]
tb_parallel_size=4
```

## Difference from [parallelizer] parallel_size
- In snapshot tasks, `[extractor].parallel_size` controls source-side snapshot extraction concurrency.
- `[parallelizer].parallel_size` controls downstream sink parallelism after data has been extracted.

```
[parallelizer]
parallel_type=snapshot
parallel_size=8
```

## Scenarios
- Snapshot migration (Source: MySQL, Postgres, MongoDB)
- Snapshot check (Source: MySQL, Postgres, MongoDB)
