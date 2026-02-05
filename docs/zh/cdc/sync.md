# 增量数据同步

订阅源库的数据变更，并同步到目标库。

前提条件
- MySQL：源库开启 binlog；
- PG：源库设置 `wal_level = logical`；
- Mongo：源库需为 ReplicaSet；
- 详情请参考 [测试环境搭建](../../../dt-tests/README_ZH.md)。

## CDC + Checker

要校验 CDC 落库后的数据，可在 CDC 任务配置中开启 `[checker]`。配置与限制请参考 [数据校验](../snapshot/check.md)。
若 `[checker]` 与 `[sinker]` 同时指定目标，优先使用 `[checker]`。

# 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

# 并发算法

- MySQL/PG：parallel_type=rdb_merge
- Mongo：parallel_type=mongo
- Redis：parallel_type=redis

# 其他配置参考

- [filter]、[route] 等配置请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/cdc
    - dt-tests/tests/pg_to_pg/cdc
    - dt-tests/tests/mongo_to_mongo/cdc
    - dt-tests/tests/redis_to_redis/cdc

- 按需修改性能参数：
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
