# 配置详情

不同任务类型需要不同的参数，详情请参考 [任务模版](/docs/templates/) 和 [教程](/docs/en/tutorial/)。

# 示例: MySQL -> MySQL

# [extractor]

| 配置            | 作用                                              | 示例                                                           | 默认                           |
| :-------------- | :------------------------------------------------ | :------------------------------------------------------------- | :----------------------------- |
| db_type         | 源库类型                                          | mysql                                                          | -                              |
| extract_type    | 拉取类型（全量：snapshot，增量：cdc）             | snapshot                                                       | -                              |
| url             | 数据库 URL。也可以在 URL 中直接指定用户名和密码。 | mysql://127.0.0.1:3307 或 mysql://root:password@127.0.0.1:3307 |
| username        | 数据库连接账号                                    | root                                                           |
| password        | 数据库连接密码                                    | password                                                       | -                              |
| max_connections | 最大连接数                                        | 10                                                             | 目前是 10，未来可能会动态适配  |
| batch_size      | 批量拉取数据条数                                  | 10000                                                          | 和 [pipeline] buffer_size 一致 |

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
| sink_type       | 拉取类型（写入：write，校验：check）                                          | write                                                          | write                         |
| url             | 数据库 URL。也可以在 URL 中直接指定用户名和密码。                             | mysql://127.0.0.1:3307 或 mysql://root:password@127.0.0.1:3307 |
| username        | 数据库连接账号                                                                | root                                                           |
| password        | 数据库连接密码                                                                | password                                                       |
| batch_size      | 批量写入数据条数，1 代表串行                                                  | 200                                                            | 200                           |
| max_connections | 最大连接数                                                                    | 10                                                             | 目前是 10，未来可能会动态适配 |
| replace         | 插入数据时，如果已存在于目标库，是否强行替换，适用于 mysql/pg 的全量/增量任务 | false                                                          | true                          |

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

| 类型      | 并行策略                                                                                            | 适用任务                | 优点 | 缺点                                         |
| :-------- | :-------------------------------------------------------------------------------------------------- | :---------------------- | :--- | :------------------------------------------- |
| snapshot  | 缓存中的数据分成 parallel_size 份，多线程并行，且批量写入目标                                       | mysql/pg/mongo 全量     | 快   |                                              |
| serial    | 单线程，依次单条写入目标                                                                            | 所有                    |      | 慢                                           |
| rdb_merge | 将缓存中的增量数据（insert, update, delete）整合成 insert + delete 数据，多线程并行，且批量写入目标 | mysql/pg 增量任务       | 快   | 最终一致性，破坏源端事务在目标端重放的完整性 |
| mongo     | rdb_merge 的 mongo 版                                                                               | mongo 增量              |      |                                              |
| rdb_check | 和 snapshot 类似，但如果源表没有主键/唯一键，则采用单线程串行写入                                   | mysql/pg/mongo 全量校验 |      |                                              |
| redis     | 单线程，批量/串行（由 sinker 的 batch_size 决定）写入                                               | redis 全量/增量         |      |                                              |

不同任务类型需要不同的 parallel_type，详情请参考各个示例。

# [runtime]

| 配置        | 作用                          | 示例                        | 默认          |
| :---------- | :---------------------------- | :-------------------------- | :------------ |
| log_level   | 日志级别                      | info/warn/error/debug/trace | info          |
| log4rs_file | log4rs 配置地点，通常不需要改 | ./log4rs.yaml               | ./log4rs.yaml |
| log_dir     | 日志输出目录                  | ./logs                      | ./logs        |

通常不需要修改。

需要注意的是，日志文件中包含了该任务的进度信息，这些信息可用于任务 [断点续传](/docs/zh/snapshot/resume.md)。所以如果你有多个任务，**请为每个任务设置独立的日志目录**。
