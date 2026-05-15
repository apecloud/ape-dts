# Migrate snapshot data

If the snapshot task contains multiple databases/tables, tables will be sorted **first by database name and then table name**, and by default they will be migrated to the target **one by one**. To run multiple tables concurrently, use `[extractor] parallel_type=table` with `[extractor] parallel_size > 1`.

If the table has a single primary/unique key, the extractor will use this key as the sorting column and pull data in batches of size [pipeline] `buffer_size`, starting from the smallest value and moving upwards.

If the table does not have a sorting column, the extractor will pull all data in stream.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

# Parallelizer

- Redis_to_Redis: parallel_type=redis
- Others: parallel_type=snapshot

# Other configurations

- For [filter] and [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/snapshot
    - dt-tests/tests/pg_to_pg/snapshot
    - dt-tests/tests/mongo_to_mongo/snapshot
    - dt-tests/tests/redis_to_redis/snapshot

- Modify performance parameters if needed:
```
[extractor]
batch_size=10000

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=10000

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
