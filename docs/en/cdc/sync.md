# Sync CDC data

Subscribe to data changes in the source database and sync them to the target.

Prerequisites
- MySQL: Enables binlog in the source database;
- PG: Sets `wal_level = logical` in the source database;
- Mongo: The source instance must be ReplicaSet;
- For more information, refer to [init test env](../../../dt-tests/README.md).

## Validate CDC-applied data

If you need validation in the CDC pipeline, use the [inline cdc check flow](../snapshot/check.md#inline-cdc-check).

Compared with the default CDC-only sync path, inline cdc check requires:
- keep `[sinker] sink_type=write`
- add `[checker] enable=true`
- add `[resumer] resume_type=from_target` or `from_db`
- use `[parallelizer] parallel_type=rdb_merge`

The checker reuses the parsed `[sinker]` target directly, so `[checker]` must not set `db_type`, `url`, `username`, or `password`.

This flow is currently supported only for MySQL and PostgreSQL write sinkers.

Inline cdc check is best-effort: CDC writes stay on the main path. If the checker queue reaches
`[checker].queue_size`, the oldest pending checker batch is dropped instead of blocking new writes.
Checker-side runtime errors are logged, but they do not block CDC writes, checkpoint persistence,
or metadata refresh delivery on the main path.

## Local benchmark reference

The table below records local quick reruns for `1,000,000`-row `mixed_write`.
Use it as a local reference only.

Notes:
- MySQL rows use `sysbench`; PostgreSQL rows use `pgbench`.
- This table uses `mixed_write` with `1,000,000` rows and workload threads `32` / `64`.
- `check off` means CDC only: `[extractor] extract_type=cdc`, `[sinker] sink_type=write`,
  `[parallelizer] parallel_type=rdb_merge`.
- `check on` means the same CDC path plus inline cdc check:
  `[checker] enable=true`, `[checker] batch_size=200`, and
  `[resumer] resume_type=from_target`.
- The shared task-side tuning in these reruns is:
  `[sinker] batch_size=200`, `[parallelizer] parallel_size=8`,
  `[pipeline] buffer_size=16000`, `[pipeline] checkpoint_interval_secs=10`.
- `Workload tx events` is the workload tool's own transaction count, not the CDC row count.
- `Sinker decay vs off` compares each `check on` row with the `check off` row at the same engine and thread count.
- `Final equal` means the final validation passed after catch-up.

| Engine     | Threads | Mode        | Workload tx events |          TPS | Workload duration | End-to-end catch-up | Sinker avg rate | Checker avg rate | Sinker decay vs off | Pipeline fill | Checker fill | Queue drops | Checker diff total | Final equal |
| ---------- | ------: | ----------- | -----------------: | -----------: | ----------------: | ------------------- | --------------: | ---------------: | ------------------: | ------------: | -----------: | ----------: | -----------------: | ----------- |
| MySQL      |    `32` | `check off` |            `10364` |   `687.30/s` |          `15.08s` | `caught up`         |     `3662.41/s` |              `-` |          `baseline` |        `100%` |          `-` |         `0` |                `-` | `yes`       |
| MySQL      |    `32` | `check on`  |             `7733` |   `514.10/s` |          `15.03s` | `caught up`         |     `1810.09/s` |      `1803.50/s` |            `-50.6%` |        `100%` |         `0%` |         `0` |              `560` | `yes`       |
| MySQL      |    `64` | `check off` |            `11857` |   `787.87/s` |          `15.05s` | `caught up`         |     `4298.00/s` |              `-` |          `baseline` |        `100%` |          `-` |         `0` |                `-` | `yes`       |
| MySQL      |    `64` | `check on`  |            `11857` |   `786.17/s` |          `15.08s` | `caught up`         |     `2989.77/s` |      `3005.42/s` |            `-30.4%` |        `100%` |      `2.25%` |         `0` |              `654` | `yes`       |
| PostgreSQL |    `32` | `check off` |            `96302` |  `6420.84/s` |             `15s` | `caught up`         |     `9547.28/s` |              `-` |          `baseline` |       `16.0%` |          `-` |         `0` |                `-` | `yes`       |
| PostgreSQL |    `32` | `check on`  |           `114086` |  `7611.53/s` |             `15s` | `caught up`         |     `2918.79/s` |      `5519.26/s` |            `-69.4%` |        `100%` |       `2.8%` |         `0` |            `18625` | `yes`       |
| PostgreSQL |    `64` | `check off` |           `106898` |  `7158.22/s` |             `15s` | `caught up`         |    `10562.80/s` |              `-` |          `baseline` |        `100%` |          `-` |         `0` |                `-` | `yes`       |
| PostgreSQL |    `64` | `check on`  |           `156715` | `10169.59/s` |             `15s` | `caught up`         |     `2869.77/s` |      `5433.13/s` |            `-72.8%` |        `100%` |       `9.6%` |         `0` |            `25058` | `yes`       |

From the table:
- All listed runs show `Queue drops = 0`.
- All listed runs show `Final equal = yes`.
- In all listed `check on` runs, `Sinker avg rate` is lower than the matching `check off` run.
- In the PostgreSQL `check on` rows, `Pipeline fill` is `100%` while `Checker fill` stays below `10%`.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

# Parallelizer

- MySQL/PG: `parallel_type=rdb_merge` for normal CDC sync and inline cdc check
- Mongo: parallel_type=mongo
- Redis: parallel_type=redis

# Other configurations

- For [filter] and [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/cdc
    - dt-tests/tests/pg_to_pg/cdc
    - dt-tests/tests/mongo_to_mongo/cdc
    - dt-tests/tests/redis_to_redis/cdc

- Modify performance parameters if needed:
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
