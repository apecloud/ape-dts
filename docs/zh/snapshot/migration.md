# 全量数据迁移

如果全量任务包含多个库/多张表，则会按照 **先库后表** 排序，默认 **依次同步** 各张表，每次有且只有一张表处于同步中。若需要多表并发，可配置 `[extractor] parallel_type=table` 且 `[extractor] parallel_size > 1`。

如果表具有单一主键/唯一键，则 extractor 会以此键作为排序列，并从小到大分片拉取每批大小为 [pipeline] 的 `buffer_size`。

如果表没有排序列，则 extractor 会流式拉取该表所有数据。

# 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

# 并发算法

- Redis_to_Redis：parallel_type=redis
- 其它：parallel_type=snapshot

# 其他配置参考

- [filter]、[router] 等配置请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/snapshot
    - dt-tests/tests/pg_to_pg/snapshot
    - dt-tests/tests/mongo_to_mongo/snapshot
    - dt-tests/tests/redis_to_redis/snapshot

- 按需修改性能参数：
```
[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
max_rps=10000

[sinker]
batch_size=200

[parallelizer]
parallel_size=8
```
