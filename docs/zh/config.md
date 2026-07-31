# 配置详情

不同任务类型需要不同的参数，详情请参考 [任务模版](/docs/templates/) 和 [教程](/docs/en/tutorial/)。

版本之间的配置变化请参考 [配置变更记录](/docs/zh/config_changelog.md)。

# [extractor]

| 配置                 | 作用                                                                        | 示例                                                                                                 | 默认                                                                                   |
| :------------------- | :-------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------- |
| db_type              | 源端数据库类型                                                              | mysql                                                                                                | 必填                                                                                   |
| extract_type         | 拉取类型，支持值由 `db_type` 决定                                           | snapshot                                                                                             | 必填                                                                                   |
| url                  | 数据库 URL；账号密码可写入 URL，也可单独配置                                | `mysql://127.0.0.1:3307`                                                                             | 空                                                                                     |
| username             | 数据库连接账号                                                              | root                                                                                                 | 空                                                                                     |
| password             | 数据库连接密码                                                              | password                                                                                             | 空                                                                                     |
| ssl_mode             | MySQL/PostgreSQL TLS 模式：`disable`、`require`、`verify_ca`、`verify_full` | verify_full                                                                                          | 不设置                                                                                 |
| ssl_ca_path          | TLS 校验使用的 CA 证书路径                                                  | /etc/ssl/certs/ca.pem                                                                                | 空                                                                                     |
| max_connections      | 源端连接池最大连接数                                                        | 10                                                                                                   | 10                                                                                     |
| batch_size           | 批量拉取行数；使用 chunk 切分时，也作为源端目标 chunk 大小                  | 10000                                                                                                | `[pipeline].buffer_size / 有效 snapshot 并发数。为0的话直接使用[pipeline].buffer_size` |
| max_rps              | 源端每秒最大记录数，`0` 表示不限制                                          | 1000                                                                                                 | 0                                                                                      |
| max_mbps             | 源端每秒最大 MiB，`0` 表示不限制                                            | 100                                                                                                  | 0                                                                                      |
| app_name             | 连接应用名，当前用于 MongoDB                                                | APE_DTS                                                                                              | APE_DTS                                                                                |
| parallel_type        | 全量拉取并发策略                                                            | table                                                                                                | table                                                                                  |
| parallel_size        | 源端 snapshot worker 上限                                                   | 4                                                                                                    | 1；兼容回退到 `[runtime].tb_parallel_size`                                             |
| partition_cols       | MySQL/PostgreSQL 全量同步的数据切分列，每张表只支持一列                     | json:[{"db":"db_1","tb":"tb_1","partition_col":"id"},{"db":"db_2","tb":"tb_2","partition_col":"id"}] | 空                                                                                     |
| is_direct_connection | MongoDB driver 的 `directConnection` 选项                                   | true                                                                                                 | 不设置（使用 driver 默认行为）                                                         |
| is_cluster           | Redis snapshot/CDC/snapshot-and-CDC 是否使用集群模式                        | true                                                                                                 | 不设置或空（根据实际连接的 Redis 节点自动判断）                                        |

## url 转义

- 如果用户名/密码中包含特殊字符，需要对相应部分进行通用的 url 百分号转义，如：

```
create user user1@'%' identified by 'abc%$#?@';
对应的 url 为：
url=mysql://user1:abc%25%24%23%3F%40@127.0.0.1:3307?ssl-mode=disabled
```

通过 `username`、`password` 单独配置的账号密码会由 DTS 做百分号编码后合并进 URL。设置
`ssl_mode` 后，`ssl_ca_path` 仍是可选项；是否必须提供 CA 取决于校验模式和服务端 TLS 配置。

## extractor.parallel_type

- `table`：把全量并发度分配给多张表。若 `parallel_size=4`，则最多可同时拉取 4 张表。
- `chunk`：把全量并发度分配给单表内部的 chunk 切分。若 `parallel_size=4`，则单张表最多可同时运行 4 个 chunk worker。
- 当 `parallel_type=chunk` 时，`[extractor].batch_size` 也作为目标 chunk 大小。chunk 边界会受实际数据分布影响，因此实际行数可能有偏差，但 extractor 会尽量让每个 chunk 接近 `batch_size`。
- 这两种模式下，真正控制并发上限的都是 `parallel_size`。
- MySQL 和 PostgreSQL 的 snapshot extractor 同时支持 `table` 与 `chunk`。
- MongoDB 的 snapshot extractor 当前只支持 `table`，不支持 `chunk`。
- 废弃兼容说明：`[runtime] tb_parallel_size` 仅作为旧配置兼容 fallback 保留，只有在未设置 `[extractor] parallel_size` 时才会生效。

## Redis 源端集群模式

- `[extractor].url` 可以指向源端集群中任意可访问的节点。DTS 会通过 `CLUSTER NODES` 发现所有源端 master 节点，并为每个 master 启动一个 PSYNC extractor。
- `[extractor].is_cluster` 默认留空。留空时，DTS 会连接 `[extractor].url` 对应的 Redis 节点，并根据节点实际返回的 cluster 状态自动判断是否使用 Redis Cluster 模式。
- `[extractor].is_cluster=true` 时，DTS 强制按 Redis Cluster 模式处理，会发现并同步整个源端集群。
- `[extractor].is_cluster=false` 时，DTS 强制按单节点 Redis 处理，只对 `[extractor].url` 指向的节点执行 PSYNC。该模式可用于源端实际是 Redis Cluster，但只希望同步其中一个节点的场景。

## Mongo 源端连接模式

- `[extractor].is_direct_connection` 会映射到 MongoDB driver 的 `directConnection` 选项。
- 省略该配置时，由 driver 根据 URL 自动推断拓扑。Replica set 和 sharded cluster 场景推荐保持省略。
- 只有明确需要直连某个 MongoDB 节点时才设置该参数。连接 sharded cluster 的 `mongos`
  执行 CDC 或 snapshot 时，不要设置为 `true`。

# [sinker]

| 配置                           | 作用                                                                                                              | 示例                     | 默认                                                        |
| :----------------------------- | :---------------------------------------------------------------------------------------------------------------- | :----------------------- | :---------------------------------------------------------- |
| db_type                        | 目标数据库类型                                                                                                    | mysql                    | 除 `sink_type=dummy` 外必填                                 |
| sink_type                      | 目标端操作类型，支持值由 `db_type` 决定                                                                           | write                    | 有 `[sinker]` 时为 write；standalone checker 省略时为 dummy |
| url                            | 数据库 URL；账号密码可写入 URL，也可单独配置                                                                      | `mysql://127.0.0.1:3307` | 空                                                          |
| username                       | 数据库连接账号                                                                                                    | root                     | 空                                                          |
| password                       | 数据库连接密码                                                                                                    | password                 | 空                                                          |
| ssl_mode                       | MySQL/PostgreSQL TLS 模式：`disable`、`require`、`verify_ca`、`verify_full`                                       | verify_full              | 不设置                                                      |
| ssl_ca_path                    | TLS 校验使用的 CA 证书路径                                                                                        | /etc/ssl/certs/ca.pem    | 空                                                          |
| batch_size                     | 批量写入行数，必须大于 `0`                                                                                        | 200                      | 200                                                         |
| max_connections                | 目标端连接池最大连接数                                                                                            | 10                       | 10                                                          |
| max_rps                        | 目标端每秒最大记录数，`0` 表示不限制                                                                              | 1000                     | 0                                                           |
| max_mbps                       | 目标端每秒最大 MiB，`0` 表示不限制                                                                                | 100                      | 0                                                           |
| replace                        | 插入冲突时是否替换已有行，适用于 MySQL/PostgreSQL 全量及增量任务                                                  | false                    | true                                                        |
| disable_foreign_key_checks     | 写入 MySQL/PostgreSQL 时是否禁用外键检查                                                                          | true                     | true                                                        |
| transaction_isolation          | MySQL/TiDB 目标端事务隔离级别：`default`、`read_uncommitted`、`read_committed`、`repeatable_read`、`serializable` | read_committed           | default                                                     |
| conflict_policy                | 结构迁移冲突策略：`interrupt` 或 `ignore`                                                                         | interrupt                | interrupt                                                   |
| app_name                       | 连接应用名，当前用于 MongoDB                                                                                      | APE_DTS                  | APE_DTS                                                     |
| is_direct_connection           | MongoDB driver 的 `directConnection` 选项                                                                         | true                     | 不设置（使用 driver 默认行为）                              |
| is_cluster                     | Redis 是否使用集群模式                                                                                            | true                     | 不设置或空（根据实际连接的 Redis 节点自动判断）             |
| mongo_require_shard_key_filter | MongoDB update/delete/upsert filter 缺少完整目标 shard key 时是否提前失败                                         | true                     | true                                                        |

## Redis 目标端集群模式

- `[sinker].url` 可以指向目标端集群中任意可访问的节点。DTS 会通过 `CLUSTER NODES` 发现所有目标端 master 节点，并按 key slot 将 Redis 命令路由到对应节点。
- Redis 目标端集群模式下，DTS 会按目标端 master 节点创建 sinker，不会用 `[parallelizer].parallel_size` 限制 sinker 数量。
- `[sinker].is_cluster` 默认留空。留空时，DTS 会连接 `[sinker].url` 对应的 Redis 节点，并根据节点实际返回的 cluster 状态自动判断是否使用 Redis Cluster 模式。
- `[sinker].is_cluster=true` 时，DTS 强制按 Redis Cluster 模式写入目标端集群。
- `[sinker].is_cluster=false` 时，DTS 强制按单节点 Redis 写入，只写入 `[sinker].url` 指向的节点。

## Mongo 目标端连接和 shard key 模式

- `[sinker].is_direct_connection` 会映射到 MongoDB driver 的 `directConnection` 选项。省略该配置时，
  由 driver 根据 URL 自动推断拓扑。目标端是 sharded cluster 时，应通过 `mongos` 连接，不要设置为 `true`。
- `[sinker].mongo_require_shard_key_filter=true` 是默认行为。目标 collection 是 sharded collection 时，
  DTS 会检查 update/delete/upsert 的 filter 是否包含完整目标 shard key，缺少 shard key 字段时提前失败。
- 普通迁移建议保持 `mongo_require_shard_key_filter=true`。只有明确接受 MongoDB 服务端路由行为时，
  才建议设置为 `false`，例如在兼容 MongoDB 版本上进行受控的 best-effort 迁移。

# [checker]

通用的数据/结构校验参数。该 section 对应以下模式：

- standalone snapshot/struct/check-log：设置 `[sinker].sink_type=check`。目标连接、认证、TLS、
  连接数以及数据库专属参数全部沿用 `[sinker]` 配置。
- inline snapshot：使用 `extract_type=snapshot`、`[sinker].sink_type=write`，并增加
  `[checker]` section。每次 sink 成功后同步执行校验。
- inline CDC：使用 `extract_type=cdc`、`[sinker].sink_type=write`，并设置
  `[checker_cdc].is_enabled=true`。写入后通过 CDC checker queue 异步校验。

| 配置                  | 作用                           | 示例 | 默认                  |
| :-------------------- | :----------------------------- | :--- | :-------------------- |
| batch_size            | 单次 checker 查询的最大行数    | 200  | 200                   |
| sample_percent        | snapshot/CDC 校验的百分比抽样  | 25   | 空（校验全部行/变更） |
| recheck_count         | 临时不一致的重试次数           | 4    | 0                     |
| recheck_interval_secs | 重试间隔（秒）                 | 5    | 0                     |
| recheck_queue_size    | 待重试行数上限                 | 10000 | 10000                |
| recheck_queue_memory_mb | 待重试数据内存上限（MiB）    | 256  | 256                   |

说明：

- checker 仅支持 `[pipeline].pipeline_type=basic`。
- `sample_percent` 有效范围为 `1..=100`，只适用于 snapshot check 和 inline CDC check。
  Standalone snapshot 在抽取阶段采样；inline snapshot/CDC 会完整写入，再在 fetch 目标数据前按
  key hash 确定性采样。
- Standalone snapshot check 支持 MySQL、PostgreSQL、MongoDB；standalone struct check
  支持 MySQL、PostgreSQL。
- Inline snapshot check 支持 MySQL、PostgreSQL、MongoDB 写入目标。
- inline CDC reconciliation 不使用 `recheck_count` 和 `recheck_interval_secs`。
- Retry buffer 达到行数或内存上限时，不会丢弃检查结果；新发现的不一致会跳过重试并立即输出。

## Standalone 目标示例

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://source-host:3306

[sinker]
db_type=mysql
sink_type=check
url=mysql://target-host:3306
username=root
password=target-password
max_connections=8

[checker]
batch_size=200
sample_percent=25
recheck_count=4
recheck_interval_secs=5
recheck_queue_size=10000
recheck_queue_memory_mb=256
```

# [checker_output]

校验结果输出配置。省略该 section 时，默认写入 `runtime.log_dir/check` 下的本地日志。

| 配置                 | 作用                                                         | 示例          | 默认 |
| :------------------- | :----------------------------------------------------------- | :------------ | :--- |
| output_type          | 输出介质：`logs` 或 `s3`                                | logs          | logs |
| output_full_row      | diff 日志是否包含完整源端/目标端行                           | false         | false |
| output_revise_sql    | 是否生成修复语句到 `sql.log`                               | true          | false |
| revise_match_full_row| 生成修复语句时是否用完整行作为匹配条件                       | false         | false |
| check_log_dir        | 本地校验日志目录                                             | /tmp/check    | 空（使用 `runtime.log_dir/check`） |
| check_log_file_size  | `diff.log`、`miss.log`、`sql.log` 单文件大小上限       | 100mb         | 100mb |
| check_log_max_rows   | CDC `diff.log`/`miss.log` 快照最大行数                   | 1000          | 1000 |
| s3_bucket            | S3 bucket，`output_type=s3` 时必填                          | my-bucket     | - |
| s3_access_key_id     | S3 access key                                                | AKIA...       | 空 |
| s3_secret_access_key | S3 secret key                                                | ****          | 空 |
| s3_region            | S3 region                                                    | us-east-1     | 空 |
| s3_endpoint          | 自定义 S3 endpoint                                           | https://...   | 空 |
| s3_root_dir          | S3 helper 使用的本地/挂载根目录                              | /tmp/check    | 空 |
| s3_root_url          | S3 helper 使用的根 URL                                       | s3://bucket   | 空 |
| s3_key_prefix        | 上传日志的 key prefix                                        | task1/check   | 空 |

`output_type=s3` 仅支持 standalone snapshot check 和 inline CDC check。S3 模式仍会使用配置的
本地滚动日志目录和限制，再执行上传。Struct check、check-log review 和 inline snapshot check
只支持 `output_type=logs`。

# [checker_cdc]

仅用于 CDC 异步校验的配置。

| 配置                    | 作用                         | 示例 | 默认 |
| :---------------------- | :--------------------------- | :--- | :--- |
| is_enabled              | 启用 inline CDC check        | true | false |
| queue_size              | 待处理 CDC checker batch 数  | 200  | 200 |
| check_log_interval_secs | CDC 校验结果周期输出间隔（秒）| 30   | 30 |

Inline CDC check 还要求：

- `[extractor].extract_type=cdc`
- `[sinker].sink_type=write`，目标为 MySQL/PostgreSQL
- `[parallelizer].parallel_type=rdb_merge`
- `[resumer].resume_type=from_target` 或 `from_db`

CDC checker queue 用于主动与迁移 pipeline 解耦；队列满时淘汰最旧的待处理 batch，不阻塞写入。
Checker 处理/输出失败只记录日志，不让主 CDC 写入链路失败。Snapshot 和 struct check 不使用该队列。

# [filter]

| 配置             | 作用                                       | 示例                                                                                                                                 | 默认 |
| :--------------- | :----------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :--- |
| do_dbs           | 需同步的库，和 do_tbs 取并集               | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| ignore_dbs       | 需过滤的库，和 ignore_tbs 取并集           | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| do_tbs           | 需同步的表，和 do_dbs 取并集               | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_tbs       | 需过滤的表，和 ignore_dbs 取并集           | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_cols      | 某些表需过滤的列                           | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -    |
| do_events        | 需同步的事件                               | insert、update、delete                                                                                                               | \*   |
| do_ddls          | 需同步的 ddl，适用于 mysql cdc 任务        | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -    |
| do_dcls          | 需同步的 DCL，适用于支持的结构任务         | create_user,grant                                                                                                                    | -    |
| do_structures    | 结构迁移任务中需同步的结构                 | mysql/pg: database,table,constraint,sequence,comment,index；mongo: collection,shardkey                                               | \*   |
| ignore_cmds      | 需忽略的命令，适用于 redis 增量任务        | flushall,flushdb                                                                                                                     | -    |
| where_conditions | 全量同步时，对源端 select sql 添加过滤条件 | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -    |

## 取值范围

- 所有配置项均支持多条配置，如 do_dbs 可包含多个库，以 , 分隔。
- 如某配置项需匹配所有条目，则设置成 \*，如 do_dbs=\*。
- 如某配置项不匹配任何条目，则设置成空，如 ignore_dbs=。
- ignore_cols 和 where_conditions 是 JSON 格式，应包含 "json:" 前缀。
- do_events 取值：insert、update、delete 中的一个或多个。
- do_dcls 取值：create_user、alter_user、create_role、drop_user、drop_role、grant、revoke、
  set_role 中的一个或多个。
- do_structures 用于选择结构对象类型。MySQL/PostgreSQL 常用取值包括 **database**、**table**、
  **constraint**、**sequence**、**comment**、**index**。MongoDB 支持 **collection**、**shardkey**。MongoDB 不使用独立的 **database** 结构类型，database 会在创建
  collection 时由 MongoDB 隐式创建。**shardkey** 用于同步源端 sharded collection 的分片定义，
  只有目标端通过 `mongos` 连接时才会真正执行。

## 优先级

- ignore_tbs + ignore_dbs > do_tbs + do_dbs。
- 如果某张表既匹配了 ignore 项，又匹配了 do 项，则该表会被过滤。
- 如果 do_tbs 和 do_dbs 都有配置，**则同步范围为二者并集**，如果 ignore_tbs 和 ignore_dbs 均有配置，**则过滤范围为二者并集**。

## 通配符

| 通配符 | 意义               |
| :----- | :----------------- |
| \*     | 匹配多个字符       |
| ?      | 匹配 0 或 1 个字符 |

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs

## 转义符

| 数据库 | 转义前      | 转义后              |
| :----- | :---------- | :------------------ |
| mysql  | db\*&#      | \`db\*&#\`          |
| mysql  | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg     | db\*&#      | "db\*&#"            |
| pg     | db*&#.tb*$# | "db*&#"."tb*$#"     |

如果表名/库名包含特殊字符，需要用相应的转义符括起来。

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs。

# [router]

| 配置      | 作用                                                    | 示例                                                                         | 默认 |
| :-------- | :------------------------------------------------------ | :--------------------------------------------------------------------------- | :--- |
| db_map    | 库级映射                                                | db_1:dst_db_1,db_2:dst_db_2                                                  | -    |
| tb_map    | 表级映射                                                | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2                      | -    |
| col_map   | 列级映射                                                | json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}] | -    |
| topic_map | 表名 -> kafka topic 映射，适用于 mysql/pg -> kafka 任务 | \*.\*:default_topic,test_db_2.\*:topic2,test_db_2.tb_1:topic3                | \*   |

## 取值范围

- 一个映射规则包括源和目标， 以 : 分隔。
- 所有配置项均支持配置多条，如 db_map 可包含多个库映射，以 , 分隔。
- col_map 是 JSON 格式，应包含 "json:" 前缀。
- 如果不配置，则默认 **源库/表/列** 与 **目标库/表/列** 一致，这也是大多数情况。

## 优先级

- tb_map > db_map。
- col_map 只专注于 **列** 映射，而不做 **库/表** 映射。也就是说，如果某张表需要 **库 + 表 + 列** 映射，需先配置好 tb_map 或 db_map。
- topic_map，test_db_2.tb_1:topic3 > test_db_2.\*:topic2 > \*.\*:default_topic。

## 通配符

不支持。

## 转义符

和 [filter] 的规则一致。

# [pipeline]

| 配置                     | 作用                                                                                                 | 示例  | 默认                                        |
| :----------------------- | :--------------------------------------------------------------------------------------------------- | :---- | :------------------------------------------ |
| buffer_size              | 内存中最多缓存数据的条数，数据同步采用多线程 & 批量写入，故须配置此项                                | 16000 | 16000                                       |
| buffer_memory_mb         | 可选，缓存数据使用内存上限，如果已超上限，则即使数据条数未达 buffer_size，也将阻塞写入。0 代表不设置 | 200   | 0                                           |
| checkpoint_interval_secs | 任务当前状态（统计数据，同步位点信息等）写入日志的频率，单位：秒                                     | 10    | 10                                          |
| batch_sink_interval_secs | 非空写入批次的最大等待时间，单位：秒                                                                 | 1     | 0                                           |
| counter_time_window_secs | 监控统计信息的时间窗口                                                                               | 10    | 和 [pipeline] checkpoint_interval_secs 一致 |
| counter_max_sub_count    | 子计数器数量上限                                                                                     | 1000  | 1000                                        |
| pipeline_type            | pipeline 实现类型，当前仅支持 `basic`                                                                | basic | basic                                       |

# [parallelizer]

| 配置                                | 作用                                                | 示例     | 默认                |
| :---------------------------------- | :-------------------------------------------------- | :------- | :------------------ |
| parallel_type                       | 并发类型                                            | snapshot | serial              |
| parallel_size                       | 并发线程数                                          | 8        | 1                   |
| rebalance_strategy                  | snapshot chunk 写入阶段 rebalance 策略              | none     | none                |
| rebalance_cost                      | rebalance 判断 partition 大小的成本口径             | rows     | rows                |
| rebalance_max_partitions_per_sinker | 每个有效 sinker 最多拆出的 partition 数             | 2        | 2                   |
| rebalance_min_partition_rows        | snapshot insert chunk 拆分后单个 partition 最小行数 | 200      | [sinker].batch_size |
| rebalance_split_skew_ratio          | auto_split 策略下判定最大 partition 明显倾斜的阈值  | 1.0      | 1.0                 |

## parallel_type 类型

| 类型      | 并行策略                                                                                                                                                                             | 适用任务                            | 优点 | 缺点                                         |
| :-------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------- | :--- | :------------------------------------------- |
| snapshot  | 缓存中的数据分成 parallel_size 份，多线程并行，且批量写入目标                                                                                                                        | mysql/pg/mongo 全量                 | 快   |                                              |
| serial    | 单线程，依次单条写入目标                                                                                                                                                             | 所有                                |      | 慢                                           |
| rdb_merge | 将缓存中的行级变更整合成适合写入的 insert + delete 批次，再按 parallel_size 并行下发；用于 MySQL/PG CDC、check、review、revise 链路 | mysql/pg 增量、校验、review、revise | 快   | 最终一致性，破坏源端事务在目标端重放的完整性 |
| mongo     | merge parallelizer 的 Mongo 版，也用于 standalone MongoDB check 和 review 链路                                                        | mongo 增量、校验、review            |      |                                              |
| redis     | 单线程，批量/串行（由 sinker 的 batch_size 决定）写入                                                                                                                                | redis 全量/增量                     |      |                                              |

## snapshot chunk rebalance

当 `[parallelizer].parallel_type=snapshot` 时，snapshot parallelizer 会使用 chunk partitioner 对下游写入队列做 rebalance。它主要用于 snapshot 写入阶段，缓解目标端 sinker 的长尾问题；不会改变源端 extractor 并发，也不会修改 checkpoint 中的 chunk id。

默认行为：

```ini
[parallelizer]
parallel_type=snapshot
parallel_size=8
rebalance_strategy=none
rebalance_cost=rows
```

默认 `rebalance_strategy=none` 会在 logical chunk 分组后保持顺序，不额外做目标端排序或拆分。如果写入阶段长尾明显，可以使用 `rebalance_strategy=auto_split`。如果希望按表做 rows-only 分片，可以使用 `table_min_rows` 或 `table_even`。行宽接近时使用默认 `rebalance_cost=rows`；如果存在大 JSON、LOB、宽字符串等行宽差异明显的场景，可以使用 `rebalance_cost=bytes`。如果目标端请求成本高，或不希望拆分 logical chunk，可以使用 `rebalance_strategy=chunk_largest_first`。

更多场景化配置建议见 [Snapshot Chunk Partitioner Rebalance](/docs/zh/snapshot/chunk_partitioner_rebalance.md)。

# [runtime]

| 配置                     | 作用                          | 示例                        | 默认          |
| :----------------------- | :---------------------------- | :-------------------------- | :------------ |
| log_level                | 日志级别                      | info/warn/error/debug/trace | info          |
| log4rs_file              | log4rs 配置地点，通常不需要改 | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir                  | 日志输出目录                  | ./logs                      | ./logs        |
| check_result_stdout_only | stdout 仅输出校验结果日志     | true/false                  | false         |

通常不需要修改。

需要注意的是，日志文件中包含了该任务的进度信息，这些信息可用于任务 [断点续传](/docs/zh/snapshot/resume.md)。所以如果你有多个任务，**请为每个任务设置独立的日志目录**。

# [global]

| 配置    | 作用           | 示例       | 默认 |
| :------ | :------------- | :--------- | :--- |
| task_id | 任务唯一标识符 | cdc_task_1 |      |

在某些场景下，task_id 用于区分任务的唯一性，例如使用数据库断点续传时。默认情况下，它将根据关键配置信息自动生成。

# [resumer]

| 配置                 | 作用                                                    | 示例                                   | 默认                |
| :------------------- | :------------------------------------------------------ | :------------------------------------- | :------------------ |
| resume_type          | `dummy`、`from_log`、`from_target` 或 `from_db`         | from_target                            | dummy               |
| log_dir              | `from_log` 使用的日志目录                               | ./logs                                 | `[runtime].log_dir` |
| config_file          | `from_log` 使用的可选 resume 配置文件                   | ./resume.config                        | 空                  |
| url                  | `from_db` 使用的数据库 URL                              | `mysql://127.0.0.1:3306`               | `from_db` 时必填    |
| db_type              | `from_db` 使用的数据库类型                              | mysql                                  | `from_db` 时必填    |
| username             | `from_db` 使用的数据库账号                              | root                                   | 空                  |
| password             | `from_db` 使用的数据库密码                              | password                               | 空                  |
| ssl_mode             | `from_db` 使用的 MySQL/PostgreSQL TLS 模式              | verify_full                            | 不设置              |
| ssl_ca_path          | `from_db` 使用的 CA 证书路径                            | /etc/ssl/certs/ca.pem                  | 空                  |
| is_direct_connection | `from_db` 使用的 MongoDB driver `directConnection` 选项 | true                                   | 不设置              |
| table_full_name      | `from_db` 或 `from_target` 保存断点状态的目标表         | apecloud_metadata.apedts_task_position | 空                  |
| max_connections      | resumer 连接池最大连接数                                | 5                                      | 5                   |

详情请参考断点续传文档：[断点续传](/docs/zh/snapshot/resume.md)。

`resume_type=from_target` 会复用已解析的 sinker 目标；standalone checker 使用 dummy 或省略
sinker 时，会复用 checker 目标。旧配置 `resume_from_log`、`resume_log_dir`、
`resume_config_file` 会直接报错，请分别迁移到 `resume_type=from_log`、`log_dir`、`config_file`。

# [tracing]

| 配置              | 作用                               | 示例   | 默认   |
| :---------------- | :--------------------------------- | :----- | :----- |
| task_summary_mode | trace 聚合模式：`task` 或 `marker` | marker | marker |
| output_format     | trace 输出格式：`plain` 或 `json`  | json   | plain  |

# [metacenter]

该可选 section 用于 MySQL `dbengine` 元数据中心模式。

| 配置                | 作用                                      | 示例                     | 默认      |
| :------------------ | :---------------------------------------- | :----------------------- | :-------- |
| type                | 元数据中心类型：`basic` 或 `dbengine`     | dbengine                 | basic     |
| url                 | 元数据库 URL，MySQL `dbengine` 模式下必填 | `mysql://127.0.0.1:3306` | 必填      |
| username            | 元数据库账号                              | root                     | 空        |
| password            | 元数据库密码                              | password                 | 空        |
| ssl_mode            | MySQL TLS 模式                            | verify_full              | 不设置    |
| ssl_ca_path         | CA 证书路径                               | /etc/ssl/certs/ca.pem    | 空        |
| ddl_conflict_policy | DDL 冲突策略：`interrupt` 或 `ignore`     | interrupt                | interrupt |

元数据中心 URL 必须与 extractor URL 及实际目标端 URL 不同。

# [data_marker]

存在该 section 时，会加载拓扑 marker 配置。

| 配置         | 作用         | 默认 |
| :----------- | :----------- | :--- |
| topo_name    | 拓扑名称     | 必填 |
| topo_nodes   | 拓扑节点列表 | 空   |
| src_node     | 源节点       | 必填 |
| dst_node     | 目标节点     | 必填 |
| do_nodes     | 包含的节点   | 必填 |
| ignore_nodes | 排除的节点   | 空   |
| marker       | marker 值    | 必填 |

# [processor]

| 配置          | 作用                          | 默认 |
| :------------ | :---------------------------- | :--- |
| lua_code_file | DTS 加载的 Lua processor 文件 | 空   |

# [metrics]

只有使用 `metrics` feature 构建 DTS 时才支持该 section。

| 配置      | 作用                            | 示例          | 默认    |
| :-------- | :------------------------------ | :------------ | :------ |
| http_host | metrics HTTP 监听地址           | 0.0.0.0       | 0.0.0.0 |
| http_port | metrics HTTP 端口               | 9090          | 9090    |
| workers   | metrics HTTP worker 数量        | 2             | 2       |
| labels    | 逗号分隔的 `key=value` 指标标签 | env=prod,az=a | 空      |
