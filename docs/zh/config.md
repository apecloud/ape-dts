# 配置详情

不同任务类型需要不同的参数，详情请参考 [任务模版](/docs/templates/) 和 [教程](/docs/en/tutorial/)。

# 示例: MySQL -> MySQL

# [extractor]

| 配置            | 作用                                              | 示例                                                                                                 | 默认                           |
| :-------------- | :------------------------------------------------ | :--------------------------------------------------------------------------------------------------- | :----------------------------- |
| db_type         | 源库类型                                          | mysql                                                                                                | -                              |
| extract_type    | 拉取类型（全量：snapshot，增量：cdc）             | snapshot                                                                                             | -                              |
| url             | 数据库 URL。也可以在 URL 中直接指定用户名和密码。 | mysql://127.0.0.1:3307 或 mysql://root:password@127.0.0.1:3307                                       |
| username        | 数据库连接账号                                    | root                                                                                                 |
| password        | 数据库连接密码                                    | password                                                                                             | -                              |
| max_connections | 最大连接数                                        | 10                                                                                                   | 目前是 10，未来可能会动态适配  |
| batch_size      | 批量拉取数据条数                                  | 10000                                                                                                | 和 [pipeline] buffer_size 一致 |
| parallel_size   | 全量同步时，单表并行拉取任务数                    | 4                                                                                                    | 1                              |
| partition_cols  | 全量同步时，指定分区列，用于数据切分，仅支持单列  | json:[{"db":"db_1","tb":"tb_1","partition_col":"id"},{"db":"db_2","tb":"tb_2","partition_col":"id"}] | -                              |
## url 转义

- 如果用户名/密码中包含特殊字符，需要对相应部分进行通用的 url 百分号转义，如：

```
create user user1@'%' identified by 'abc%$#?@';
对应的 url 为：
url=mysql://user1:abc%25%24%23%3F%40@127.0.0.1:3307?ssl-mode=disabled
```

# [sinker]

| 配置            | 作用                                                                          | 示例                                                           | 默认                          |
| :-------------- | :---------------------------------------------------------------------------- | :------------------------------------------------------------- | :---------------------------- |
| db_type         | 目标库类型                                                                    | mysql                                                          | -                             |
| sink_type       | 写入类型（写入：write，空写入：dummy）                                          | write                                                          | write                         |
| url             | 数据库 URL。也可以在 URL 中直接指定用户名和密码。                             | mysql://127.0.0.1:3307 或 mysql://root:password@127.0.0.1:3307 |
| username        | 数据库连接账号                                                                | root                                                           |
| password        | 数据库连接密码                                                                | password                                                       |
| batch_size      | 批量写入数据条数，1 代表串行                                                  | 200                                                            | 200                           |
| max_connections | 最大连接数                                                                    | 10                                                             | 目前是 10，未来可能会动态适配 |
| replace         | 插入数据时，如果已存在于目标库，是否强行替换，适用于 mysql/pg 的全量/增量任务 | false                                                          | true                          |

# [checker]

`[checker]` 对应三种已文档化的数据校验形态：
- standalone snapshot check：只运行 snapshot 校验任务，不执行写入。设置 `sink_type=dummy`
  或直接省略 `[sinker]`，并在 `[checker]` 中显式配置校验目标。
- inline snapshot check：用于 `sink_type=write` 的 snapshot 任务，checker 会在写入后执行，
  并直接复用 `[sinker]` 已解析的目标端配置。
- inline cdc check：用于 `extract_type=cdc` 且 `sink_type=write` 的 CDC 任务，checker 会在
  写入后校验已落库变更，直接复用 `[sinker]` 目标，并要求持久化 checker 状态。

struct check 复用 standalone snapshot check 的目标选择规则。

| 配置                        | 作用                                                           | 示例        | 默认                             |
| :-------------------------- | :------------------------------------------------------------- | :---------- | :------------------------------- |
| queue_size                  | checker 队列容量，按待处理批次/消息数计数                      | 200         | 200                              |
| max_connections             | checker 连接池最大连接数                                       | 8           | 8                                |
| batch_size                  | checker 的分块大小；inline cdc check 下也用于控制 checker 分块 | 200         | 200                              |
| output_full_row             | diff 日志是否输出全量行                                        | false       | false                            |
| output_revise_sql           | 是否将生成的修复 SQL 写入 `sql.log`                            | false       | false                            |
| revise_match_full_row       | 生成修复 SQL 时是否按全量行匹配                                | false       | false                            |
| retry_interval_secs         | 重试间隔（秒），inline cdc check 下强制为 0                    | 0           | 0                                |
| max_retries                 | 重试次数，inline cdc check 下强制为 0                          | 0           | 0                                |
| check_log_dir               | 校验日志目录                                                   | /tmp/check  | 空（默认 runtime.log_dir/check） |
| check_log_file_size         | 单类日志文件大小上限（`diff.log` / `miss.log` / `sql.log`）    | 100mb       | 100mb                            |
| check_log_max_rows          | 单类日志最大行数（`diff.log` / `miss.log`）                    | 1000        | 1000                             |
| db_type                     | 校验目标库类型（仅 standalone 目标配置）                       | mysql       | -                                |
| url                         | 校验目标 URL（仅 standalone 目标配置）                         | mysql://... | -                                |
| username                    | 校验目标用户名（仅 standalone 目标配置）                       | root        | 空                               |
| password                    | 校验目标密码（仅 standalone 目标配置）                         | password    | 空                               |
| cdc_check_log_s3            | 定期将 CDC 校验快照上传至 S3                                   | false       | false                            |
| cdc_check_log_interval_secs | CDC 校验快照输出间隔（秒）                                     | 10          | 10                               |
| s3_bucket                   | 校验日志上传的 S3 存储桶                                       | my-bucket   | -                                |
| s3_access_key_id            | S3 访问密钥 ID                                                 | AKIA...     | -                                |
| s3_secret_access_key        | S3 秘密访问密钥                                                | ****        | -                                |
| s3_region                   | S3 区域                                                        | us-east-1   | -                                |
| s3_endpoint                 | S3 端点                                                        | https://... | -                                |
| s3_key_prefix               | 校验日志的 S3 键前缀                                           | task1/check | 空                               |

说明：

**通用行为**
- checker 仅支持 `[pipeline] pipeline_type=basic`。
- `queue_size` 统计的是 checker DML 队列中的待处理批次数，不是行数。checkpoint、`refresh_meta`
  这类控制信号会绕过这条队列。
- 在 inline 写后校验链路里，如果 checker DML 队列已满，会丢弃最旧的待校验批次并记录 warning
  日志，而不是阻塞写入路径。
- checker 运行时错误（批次校验失败、checkpoint 失败、输出失败）只会记录日志，不影响主 CDC
  写入链路；checkpoint 和元数据刷新投递仍按 best-effort 处理。

**目标选择与适用形态**
- 对 inline 写后校验链路来说，一个排队批次通常接近实际写入批大小；实践中多数情况下约等于
  `[sinker].batch_size` 行，但最后一个批次可能更小，上游分片策略也会影响实际条数。
- 对 standalone / dummy-sinker 校验链路来说，进入队列的单批大小由上游 parallelizer 决定；
  出队后，checker 会再按 `[checker].batch_size` 对非 CDC 数据做内部切块处理。
- struct 任务只支持 standalone 目标选择规则。若为 struct 任务启用 `[checker]`，请使用 `sink_type=dummy` 或直接省略 `[sinker]`。
- inline snapshot check 仅支持 `[extractor] extract_type=snapshot`、`[sinker] sink_type=write`，
  且 `[sinker].db_type` 为 `mysql`、`pg`、`mongo` 的写入链路。
- inline cdc check 当前仅支持 `[extractor] extract_type=cdc`、`[sinker] sink_type=write`，
  且 `[sinker].db_type` 为 `mysql` 或 `pg` 的场景。
- 在 inline cdc check 中，checker 使用 `[checker].batch_size`，不会 fallback 到
  `[sinker].batch_size`。例如 `[checker].batch_size=100`、`queue_size=200` 时，队列最多可积压 200 个待处理批次；若这些批次都打满，大约就是 20,000 行待校验数据。
- 在 inline snapshot check 与 inline cdc check 中，`[checker]` 不接受 `db_type`、`url`、
  `username`、`password`；checker 会直接复用 `[sinker]` 已解析的目标端配置。
- 在 inline cdc check 中，必须配置 `[resumer] resume_type=from_target` 或 `from_db` 来持久化
  checker 状态。
- 对 inline cdc check，下面这些组合会直接报 `ConfigError`：缺少 `[checker]` 且
  `parallel_type=rdb_check`；`[pipeline].pipeline_type != basic`；`[sinker].sink_type != write`；
  `[sinker].db_type` 不属于 `mysql` / `pg`；以及在 `[checker]` 中显式填写目标端字段
  `db_type` / `url` / `username` / `password`。

**inline cdc check 的日志 / 重试行为**
- 对 inline cdc check，`[checker].batch_size` 会继续生效并控制 checker 分块；
  `max_retries` 与 `retry_interval_secs` 会强制按 0 处理。
- 当 `check_log_dir` 为空时，统一使用 `runtime.log_dir/check` 作为 checker 日志目录（包含 CDC 校验输出）。
- 在 inline cdc check 下，会始终先在 `check_log_dir` 本地落盘周期性校验快照；
  `cdc_check_log_s3` 仅控制是否上传 S3。
- `check_log_file_size` 限制本地 `diff.log` / `miss.log` / `sql.log` 的大小，`summary.log` 不受该限制。
- `check_log_max_rows` 仅对 CDC 校验快照的 `diff.log` / `miss.log` 生效；命中任一阈值时仅保留最新记录。

# [filter]

| 配置             | 作用                                       | 示例                                                                                                                                 | 默认 |
| :--------------- | :----------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :--- |
| do_dbs           | 需同步的库，和 do_tbs 取并集               | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| ignore_dbs       | 需过滤的库，和 ignore_tbs 取并集           | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| do_tbs           | 需同步的表，和 do_dbs 取并集               | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_tbs       | 需过滤的表，和 ignore_dbs 取并集           | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_cols      | 某些表需过滤的列                           | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -    |
| do_events        | 需同步的事件                               | insert、update、delete                                                                                                               | -    |
| do_ddls          | 需同步的 ddl，适用于 mysql cdc 任务        | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -    |
| do_structures    | 需同步的结构，适用于 mysql/pg 结构迁移任务 | database,table,constraint,sequence,comment,index                                                                                     | \*   |
| ignore_cmds      | 需忽略的命令，适用于 redis 增量任务        | flushall,flushdb                                                                                                                     | -    |
| where_conditions | 全量同步时，对源端 select sql 添加过滤条件 | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -    |

## 取值范围

- 所有配置项均支持多条配置，如 do_dbs 可包含多个库，以 , 分隔。
- 如某配置项需匹配所有条目，则设置成 \*，如 do_dbs=\*。
- 如某配置项不匹配任何条目，则设置成空，如 ignore_dbs=。
- ignore_cols 和 where_conditions 是 JSON 格式，应包含 "json:" 前缀。
- do_events 取值：insert、update、delete 中的一个或多个。

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
| max_rps                  | 可选，限制每秒最多同步数据的条数，避免对数据库性能影响                                               | 1000  | -                                           |
| counter_time_window_secs | 监控统计信息的时间窗口                                                                               | 10    | 和 [pipeline] checkpoint_interval_secs 一致 |

# [parallelizer]

| 配置          | 作用       | 示例     | 默认   |
| :------------ | :--------- | :------- | :----- |
| parallel_type | 并发类型   | snapshot | serial |
| parallel_size | 并发线程数 | 8        | 1      |

## parallel_type 类型

| 类型      | 并行策略                                                                                                           | 适用任务                | 优点 | 缺点                                         |
| :-------- | :----------------------------------------------------------------------------------------------------------------- | :---------------------- | :--- | :------------------------------------------- |
| snapshot  | 缓存中的数据分成 parallel_size 份，多线程并行，且批量写入目标                                                      | mysql/pg/mongo 全量     | 快   |                                              |
| serial    | 单线程，依次单条写入目标                                                                                           | 所有                    |      | 慢                                           |
| rdb_merge | 将缓存中的增量数据（insert, update, delete）整合成 insert + delete 数据，多线程并行，且批量写入目标                | mysql/pg 增量任务       | 快   | 最终一致性，破坏源端事务在目标端重放的完整性 |
| mongo     | rdb_merge 的 mongo 版                                                                                              | mongo 增量              |      |                                              |
| rdb_check | 校验模式；必须配置 `[checker]`，standalone snapshot / struct check 不写入目标端，inline cdc check 仍是先写入再校验 | 支持 checker 的校验任务 |      |                                              |
| redis     | 单线程，批量/串行（由 sinker 的 batch_size 决定）写入                                                              | redis 全量/增量         |      |                                              |

# [runtime]
| 配置        | 作用                          | 示例                        | 默认          |
| :---------- | :---------------------------- | :-------------------------- | :------------ |
| log_level   | 日志级别                      | info/warn/error/debug/trace | info          |
| log4rs_file | log4rs 配置地点，通常不需要改 | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir     | 日志输出目录                  | ./logs                      | ./logs        |

通常不需要修改。

需要注意的是，日志文件中包含了该任务的进度信息，这些信息可用于任务 [断点续传](/docs/zh/snapshot/resume.md)。所以如果你有多个任务，**请为每个任务设置独立的日志目录**。

# [global]

| 配置    | 作用           | 示例       | 默认 |
| :------ | :------------- | :--------- | :--- |
| task_id | 任务唯一标识符 | cdc_task_1 |      |

在某些场景下，task_id 用于区分任务的唯一性，例如使用数据库断点续传时。默认情况下，它将根据关键配置信息自动生成。

# [resumer]

| 配置            | 作用                                                           | 示例                                        | 默认                                   |
| :-------------- | :------------------------------------------------------------- | :------------------------------------------ | :------------------------------------- |
| resume_type     | 类型: [from_log;from_target;from_db]                           | from_target                                 |                                        |
| log_dir         | resume_type 为 from_log 时有效，日志目录位置                   | ./logs                                      |                                        |
| url             | resume_type 为 from_db 时有效，数据库连接 URL                  | mysql://xxx:xxx@127.0.0.1:3306              |                                        |
| db_type         | resume_type 为 from_db 时有效，数据库类型                      | mysql                                       |                                        |
| table_full_name | resume_type 为 from_db 或 from_target 时有效，用于记录的表全名 | apecloud_metadata_test.apedts_task_position | apecloud_metadata.apedts_task_position |
| max_connections | 断点续传连接池的最大连接数                                     | 1                                           | 1                                      |

详情请参考断点续传文档：[断点续传](/docs/zh/snapshot/resume.md)。
