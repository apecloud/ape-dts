# Sync CDC data

Subscribe to data changes in the source database and sync them to the target.

Prerequisites
- MySQL: Enables binlog in the source database;
- PG: Sets `wal_level = logical` in the source database;
- Mongo: The source instance must be ReplicaSet;
- For more information, refer to [init test env](../../../dt-tests/README.md).

## Inline cdc check

To validate CDC-applied data, enable `[checker]` in the CDC task config. This is the inline cdc
check flow described in [Data Check](../snapshot/check.md).
In inline cdc check, `[checker]` reuses the parsed `[sinker]` target directly and must not
set its own `db_type`, `url`, `username`, or `password`.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

# Parallelizer

- MySQL/PG: parallel_type=rdb_merge
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
