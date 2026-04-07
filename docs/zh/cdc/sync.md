# 增量数据同步

订阅源库的数据变更，并同步到目标库。

前提条件
- MySQL：源库开启 binlog；
- PG：源库设置 `wal_level = logical`；
- Mongo：源库需为 ReplicaSet；
- 详情请参考 [测试环境搭建](../../../dt-tests/README_ZH.md)。

## 校验 CDC 落库数据

若需要在 CDC 链路中同时做数据校验，请使用 [数据校验](../snapshot/check.md#inline-cdc-check) 中定义的 inline cdc check。

相较默认的纯 CDC 同步链路，inline cdc check 还要求：
- 保持 `[sinker] sink_type=write`
- 增加 `[checker] enable=true`
- 增加 `[resumer] resume_type=from_target` 或 `from_db`
- 使用 `[parallelizer] parallel_type=rdb_merge`

在该模式下，checker 会直接复用 `[sinker]` 已解析的目标端配置，因此 `[checker]` 不接受单独设置 `db_type`、`url`、`username`、`password`。

当前该模式仅支持 MySQL / PostgreSQL 的 write sinker。

# 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

# 并发算法

- MySQL/PG：普通 CDC 同步与 inline cdc check 都使用 `parallel_type=rdb_merge`
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
