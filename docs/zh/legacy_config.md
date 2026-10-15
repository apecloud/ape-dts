# Legacy config

本文用于维护已有的 `[filter]` / `[router]` 配置。创建任务请使用[配置参考](config.md)和[过滤与路由配置](rules.md)。

只有配置文件中**没有任何 `[rules.*]` 段**时，才会读取本文的配置。加入任意 `[rules.*]` 段（包括空段）后，整个 `[filter]` 和 `[router]` 都不再生效，因此过滤、事件类型、WHERE 条件和路由应一起迁移。

## [filter]

| 配置             | 作用                                       | 示例                                                                                                                                 | 默认 |
| :--------------- | :----------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- | :--- |
| do_dbs           | 需同步的库/schema               | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| ignore_dbs       | 需排除的库/schema           | db_1,db_2*,\`db*&#\`                                                                                                                 | -    |
| do_tbs           | 需同步的表               | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_tbs       | 需排除的表           | db_1.tb_1,db_2*.tb_2*,\`db*&#\`.\`tb*&#\`                                                                                            | -    |
| ignore_cols      | 某些表需过滤的列                           | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -    |
| do_events        | 需同步的事件                               | insert、update、delete                                                                                                               | \*   |
| do_ddls          | 需同步的 ddl，适用于 mysql cdc 任务        | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -    |
| do_dcls          | 需同步的 DCL，适用于支持的结构任务         | create_user,grant                                                                                                                    | -    |
| do_structures    | 结构迁移任务中需同步的结构                 | mysql/pg: database,table,constraint,sequence,comment,index；mongo: collection,shardkey                                               | \*   |
| ignore_cmds      | 需忽略的命令，适用于 redis 增量任务        | flushall,flushdb                                                                                                                     | -    |
| where_conditions | 全量同步时，对源端 select sql 添加过滤条件 | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -    |

### 取值范围

- 所有配置项均支持多条配置，如 do_dbs 可包含多个库，以 , 分隔。
- 如某配置项需匹配所有条目，则设置成 \*，如 do_dbs=\*。
- 资源列表留空表示不选择条目。`do_events` 和 `do_structures` 未设置或留空时，仍使用选择全部类型的默认值。
- ignore_cols 和 where_conditions 是 JSON 格式，应包含 "json:" 前缀。
- do_events 取值：insert、update、delete 中的一个或多个。
- do_dcls 取值：create_user、alter_user、create_role、drop_user、drop_role、grant、revoke、
  set_role 中的一个或多个。
- do_structures 用于选择结构对象类型。MySQL/PostgreSQL 常用取值包括 **database**、**table**、
  **constraint**、**sequence**、**comment**、**index**。MongoDB 支持 **collection**、**shardkey**。MongoDB 不使用独立的 **database** 结构类型，database 会在创建
  collection 时由 MongoDB 隐式创建。**shardkey** 用于同步源端 sharded collection 的分片定义，
  只有目标端通过 `mongos` 连接时才会真正执行。

### 优先级

- 表规则优先于库/schema 规则；表规则未命中时再使用库/schema 规则。
- 同一层同时命中 do 和 ignore 时，ignore 优先；表级 do 可以覆盖库级 ignore。
- 未命中表级规则时继续检查库/schema 级规则。

### 通配符

| 通配符 | 意义               |
| :----- | :----------------- |
| \*     | 匹配零个或多个字符       |
| ?      | 匹配 0 或 1 个字符 |

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs

### 转义符

| 数据库 | 转义前      | 转义后              |
| :----- | :---------- | :------------------ |
| mysql  | db\*&#      | \`db\*&#\`          |
| mysql  | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg     | db\*&#      | "db\*&#"            |
| pg     | db*&#.tb*$# | "db*&#"."tb*$#"     |

如果表名/库名包含特殊字符，需要用相应的转义符括起来。

适用范围：do_dbs，ignore_dbs，do_tbs，ignore_tbs。

## [router]

| 配置      | 作用                                                    | 示例                                                                         | 默认 |
| :-------- | :------------------------------------------------------ | :--------------------------------------------------------------------------- | :--- |
| db_map    | 库级映射                                                | db_1:dst_db_1,db_2:dst_db_2                                                  | -    |
| tb_map    | 表级映射                                                | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2                      | -    |
| col_map   | 列级映射                                                | json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}] | -    |
| topic_map | 表名 -> kafka topic 映射，适用于 mysql/pg -> kafka 任务 | \*.\*:default_topic,test_db_2.\*:topic2,test_db_2.tb_1:topic3                | - |

### 取值范围

- 一个映射规则包括源和目标， 以 : 分隔。
- 所有配置项均支持配置多条，如 db_map 可包含多个库映射，以 , 分隔。
- col_map 是 JSON 格式，应包含 "json:" 前缀。
- 如果不配置，则默认 **源库/表/列** 与 **目标库/表/列** 一致，这也是大多数情况。

### 优先级

- tb_map > db_map。
- col_map 只专注于 **列** 映射，而不做 **库/表** 映射。也就是说，如果某张表需要 **库 + 表 + 列** 映射，需先配置好 tb_map 或 db_map。
- topic_map，test_db_2.tb_1:topic3 > test_db_2.\*:topic2 > \*.\*:default_topic。

### 通配符

库、表、列映射使用精确名称。`topic_map` 支持整个尾部段的 `*`，例如 `db.*` 和 `*.*`。

### 转义符

和 [filter] 的规则一致。


## 从旧配置迁移

| 旧配置 | 配置 |
| --- | --- |
| `[filter] do_dbs / ignore_dbs` | 顶层资源配置段的 `do / ignore`；MySQL 为 `[rules.db]`，PG 为 `[rules.schema]` |
| `do_tbs / ignore_tbs` | `[rules.table] do / ignore` |
| `ignore_cols` 的 JSON 列表 | `[rules.column] ignore=db.table.column,...` |
| `do_events / do_ddls / do_dcls / do_structures / ignore_cmds` | 对应公共配置段的 `do` 或 `ignore` |
| `where_conditions` 的 JSON 列表 | `[rules.table] where_conditions=db.table:"SQL",...` |
| `[router] db_map / tb_map / col_map` | 对应资源配置段的 `route` |
| `[router] topic_map` | `[rules.table]` 下的 `to_topic`，即通用分发映射 `to_<kind>` 的 topic 特例 |

MSSQL 旧二段标识迁移时需显式补上 schema。不要只新增 `route` 或 `to_topic` 而保留旧 `[filter]` 的资源选择，因为任意 `[rules.*]` 都会整体切换配置模式。
