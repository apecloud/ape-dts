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

inline cdc check 是 best-effort 的：CDC 写入仍走主路径。若 checker 队列达到
`[checker].queue_size`，会淘汰最旧的待校验 batch，而不是阻塞新的写入。
checker 侧的运行时错误会被记录日志，但不会阻塞主路径上的 CDC 写入、checkpoint 持久化
或元数据刷新投递。

## 本地性能测试参考

下表记录了 `1,000,000` 行 `mixed_write` 的本地 quick rerun，仅作为本地参考。

说明：
- MySQL 行使用 `sysbench`，PostgreSQL 行使用 `pgbench`。
- 这组数据使用 `1,000,000` 行的 `mixed_write`，workload 并发为 `32` / `64`。
- `check off` 表示纯 CDC 路径：`[extractor] extract_type=cdc`、`[sinker] sink_type=write`、
  `[parallelizer] parallel_type=rdb_merge`。
- `check on` 表示在相同 CDC 路径上启用 inline cdc check：
  `[checker] enable=true`、`[checker] batch_size=200`、
  `[resumer] resume_type=from_target`。
- 这组复测共用的任务侧调优参数为：
  `[sinker] batch_size=200`、`[parallelizer] parallel_size=8`、
  `[pipeline] buffer_size=16000`、`[pipeline] checkpoint_interval_secs=10`。
- `Workload tx events` 是压测工具自身统计的事务数，不是 CDC 行数。
- `Sinker 相比 off 衰减` 表示同引擎、同并发下，`check on` 相对 `check off` 的对比结果。
- `最终一致` 表示追平后的最终校验通过。

| 引擎       | 并发 | 模式        | Workload tx events |          TPS | Workload 时长 | End-to-end 追平 | Sinker 平均速率 | Checker 平均速率 | Sinker 相比 off 衰减 | Pipeline 填充率 | Checker 填充率 | Queue drops | Checker diff total | 最终一致 |
| ---------- | ---: | ----------- | -----------------: | -----------: | ------------: | --------------- | --------------: | ---------------: | -------------------: | --------------: | -------------: | ----------: | -----------------: | -------- |
| MySQL      | `32` | `check off` |            `10364` |   `687.30/s` |      `15.08s` | `caught up`     |     `3662.41/s` |              `-` |           `baseline` |          `100%` |            `-` |         `0` |                `-` | `yes`    |
| MySQL      | `32` | `check on`  |             `7733` |   `514.10/s` |      `15.03s` | `caught up`     |     `1810.09/s` |      `1803.50/s` |             `-50.6%` |          `100%` |           `0%` |         `0` |              `560` | `yes`    |
| MySQL      | `64` | `check off` |            `11857` |   `787.87/s` |      `15.05s` | `caught up`     |     `4298.00/s` |              `-` |           `baseline` |          `100%` |            `-` |         `0` |                `-` | `yes`    |
| MySQL      | `64` | `check on`  |            `11857` |   `786.17/s` |      `15.08s` | `caught up`     |     `2989.77/s` |      `3005.42/s` |             `-30.4%` |          `100%` |        `2.25%` |         `0` |              `654` | `yes`    |
| PostgreSQL | `32` | `check off` |            `96302` |  `6420.84/s` |         `15s` | `caught up`     |     `9547.28/s` |              `-` |           `baseline` |         `16.0%` |            `-` |         `0` |                `-` | `yes`    |
| PostgreSQL | `32` | `check on`  |           `114086` |  `7611.53/s` |         `15s` | `caught up`     |     `2918.79/s` |      `5519.26/s` |             `-69.4%` |          `100%` |         `2.8%` |         `0` |            `18625` | `yes`    |
| PostgreSQL | `64` | `check off` |           `106898` |  `7158.22/s` |         `15s` | `caught up`     |    `10562.80/s` |              `-` |           `baseline` |          `100%` |            `-` |         `0` |                `-` | `yes`    |
| PostgreSQL | `64` | `check on`  |           `156715` | `10169.59/s` |         `15s` | `caught up`     |     `2869.77/s` |      `5433.13/s` |             `-72.8%` |          `100%` |         `9.6%` |         `0` |            `25058` | `yes`    |

从表格可直接看到：
- 所有样本的 `Queue drops` 都是 `0`。
- 所有样本的 `最终一致` 都是 `yes`。
- 所有 `check on` 样本的 `Sinker 平均速率` 都低于对应的 `check off` 样本。
- PostgreSQL 的 `check on` 两行里，`Pipeline 填充率` 都是 `100%`，而 `Checker 填充率` 仍低于 `10%`。

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
