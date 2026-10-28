# 过滤与路由配置

通过 `[source.*]` 按源端对象配置需要同步哪些库、表和列，以及它们在目标端使用的名称。配置写在任务的 INI 文件中；连接地址、任务类型等参数见[配置参考](config.md)。已有 `[filter]` / `[router]` 文件的说明和迁移方法见 [Legacy config](legacy_config.md)。

## 配置一个 MySQL → MySQL 全量任务

下面的任务将 `inventory` 同步到 `archive`，排除 `tmp` 开头的表。`orders` 表写入 `archive.order_history`，只同步 `id > 100` 的行，排除 `secret` 开头的列，并将 `id` 改名为 `order_id`。

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://user:password@source:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://user:password@target:3306

[source.db]
include=inventory
route=inventory:archive

[source.table]
exclude=inventory.tmp*
where_conditions=inventory.orders:"id > 100"
route=inventory.orders:archive.order_history

[source.column]
exclude=inventory.orders.secret*
route=inventory.orders.id:archive.order_history.order_id

[source.event]
include=insert

[parallelizer]
parallel_type=snapshot
parallel_size=4

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
```

请替换连接地址，并在目标端准备对应的表结构。全量数据任务不会自动创建目标表；结构迁移请参阅[结构迁移](structure/migration.md)。未写入的目标列应允许 NULL 或有默认值。

## 配置段和名称写法

一个名称中的各级对象用 `.` 分隔，多条规则用 `,` 分隔，名称映射用 `源名称:目标名称` 表示。

| 数据库类型 | 配置段 | 表、集合或库编号示例 |
| --- | --- | --- |
| MySQL、TiDB、ClickHouse、StarRocks、Doris | `source.db`、`source.table`、`source.column` | `inventory.orders`、`inventory.orders.id` |
| PostgreSQL | `source.schema`、`source.table`、`source.column` | `inventory.orders`、`inventory.orders.id` |
| SQL Server | `source.db`、`source.schema`、`source.table`、`source.column` | `market.inventory.orders`、`market.inventory.orders.id` |
| MongoDB | `source.db`、`source.collection` | `inventory.orders` |
| Redis | `source.db` | `0`、`1` |

PostgreSQL 的连接数据库由 URL 指定，表名前一段是 schema。SQL Server 的表名包含数据库、schema、表三段。`[source.database]` 是 `[source.db]` 的别名，使用其中一种即可。当前数据库不支持的资源类型会被忽略。

| 配置项 | 作用 | 示例 |
| --- | --- | --- |
| `include` | 选择需要同步的对象 | `inventory.orders,inventory.event*` |
| `exclude` | 排除对象 | `inventory.tmp*` |
| `where_conditions` | 全量迁移、全量校验时的查询条件 | `inventory.orders:"id > 100"` |
| `route` | 修改目标名称，使用完整名称 | `inventory.orders:archive.order_history` |
| `to_<kind>` | 分发映射：决定数据发送到目标端的哪个资源；`to_topic` 是分发到 topic 的特例 | `to_topic=inventory.*:events,*.*:default_events` |

至少配置一条资源 `include` 规则。只配置 `route` 或 `exclude` 不会自动选择所有表。不需要改名时省略 `route`，目标沿用源端名称。

## 选择和排除对象

```ini
[source.db]
include=inventory,tenant_*
exclude=tenant_private*

[source.table]
include=other.orders*,tenant_private.public_orders
exclude=inventory.tmp*,other.orders_archive*
```

这会选择 `inventory` 和 `tenant_` 开头的库，并排除 `tenant_private` 开头的库。即使表级 include 显式选择了 `tenant_private.public_orders`，它仍被库级 exclude 排除；`inventory.tmp*` 不会同步。`other.orders_archive` 同时符合表级 `include` 和 `exclude`，会被排除。

先检查所有适用层级的 `exclude`，命中任意一条就排除；再检查 `include`，命中任意一条才同步。库/schema 和表级包含规则取并集，排除规则也取并集。列规则仅控制已选中表的数据列，不会重新选中被排除的表。

### 通配符与正则

以下写法适用于资源 `include` / `exclude`，每一段分别匹配：

| 规则 | 匹配示例 | 不匹配示例 |
| --- | --- | --- |
| `inventory.order*` | `inventory.order`、`inventory.orders`、`inventory.order2026` | `inventory.preorder` |
| `inventory.*log` | `inventory.log`、`inventory.audit_log` | `inventory.log_backup` |
| `inventory.order*log` | `inventory.orderlog`、`inventory.order_audit_log` | `inventory.orderlog_backup` |
| `inventory.*audit*` | `inventory.audit`、`inventory.daily_audit_log` | `inventory.orders` |
| `inventory.*` | `inventory` 下所有表 | 其他库中的表 |
| `tenant_*.orders` | `tenant_.orders`、`tenant_eu.orders` | `other.orders` |
| `*.*` | 所有库/schema 下的表 | — |
| `inventory.order?` | `inventory.order`、`inventory.order1` | `inventory.order12` |
| `inventory.r#order[0-9]+#` | `inventory.order1`、`inventory.order12` | `inventory.order`、`inventory.order1_backup` |

`*` 匹配零个或多个字符；`?` 匹配零个或一个字符。正则表达式放在 `r#` 和 `#` 之间。需要匹配多个备选名称时，例如写成 `inventory.r#(?:orders|events)[0-9]+#`。

### 名称中有特殊字符

名称本身包含 `*`、`?`、`.`、`,`、`:`、`+`、括号等字符时，用源数据库的标识符引号包围该段，表示精确名称。

| 数据库 | 精确匹配名为 `order*` 的表 | 名称中包含引号 |
| --- | --- | --- |
| MySQL | `` `inventory`.`order*` `` | ``` `inventory`.`a``b` ``` 表示表名 ``a`b`` |
| PostgreSQL | `"inventory"."order*"` | `"inventory"."a""b"` 表示表名 `a"b` |
| SQL Server | `[market].[inventory].[order*]` | `[market].[inventory].[a]]b]` 表示表名 `a]b` |

```ini
[source.table]
include=`a.b,c:d`.`order*`,`inventory`.`r#literal#`
```

上例精确选择库 `a.b,c:d` 中的表 `order*`，以及 `inventory` 中名为 `r#literal#` 的表。引号内的星号和 `r#…#` 不表示模式。

### 选择列

```ini
[source.table]
include=inventory.orders,inventory.events

[source.column]
include=inventory.orders.id,inventory.orders.amount*,inventory.orders.secret
exclude=inventory.orders.secret,inventory.events.debug*
```

`orders` 只同步 `id` 和 `amount` 开头的列；`secret` 虽然在 `include` 中，仍因 `exclude` 被排除。`events` 没有列级 `include`，同步除 `debug` 开头以外的列。列选择用于数据同步和数据校验，不会自动调整目标表结构。

## 按行筛选：WHERE 条件

```ini
[source.table]
include=inventory.orders,inventory.events
; 外层双引号用于配置字符串；内部双引号写成 \"，反斜杠写成 \\。
where_conditions=inventory.orders:"id > 100 AND note IN ('keep\"quote','other')",inventory.events:"kind IN ('sale','refund')"
```

外层双引号是配置文件的字符串定界符，不属于 SQL。内部转义与 JSON 字符串一致：`\"` 表示双引号，`\\` 表示反斜杠，也可使用 `\n`、`\t`、`\uXXXX`。SQL 自身的字段名和字符串写法遵循源数据库语法。PostgreSQL 的带引号字段可以写成 `inventory.orders:"\"OrderId\" > 100"`。

表名使用精确名称，不支持通配符或正则。同一张表重复配置时，第一条条件生效。条件不向下继承，库/schema 上的条件不会自动作用于表。

WHERE 条件用于全量迁移和全量数据校验，不用于逐行过滤 CDC 变更。需要这些查询条件时，将它们放在 `[source.table]` 中。

## 事件、DDL、DCL 和结构类型

这些配置段使用 `include` / `exclude` 指定类型名称，名称精确匹配。单独写 `include=*` 表示全部，`include=` 表示不选择任何类型；不要把 `*` 与具体类型混写。`exclude=*` 不表示排除全部。

| 配置段 | 未设置 `include` 时 | 常用值 |
| --- | --- | --- |
| `[source.event]` | 全部 | `insert,update,delete` |
| `[source.ddl]` | 不同步 | `create_database,drop_database,alter_database,create_schema,drop_schema,alter_schema,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index` |
| `[source.dcl]` | 不同步 | `create_user,alter_user,create_role,drop_user,drop_role,grant,revoke,set_role` |
| `[source.structure]` | 全部 | `database,table,constraint,sequence,comment,index,udf,udt,rbac,collection,shardkey` |
| `[source.cmd]` | 全部 | Redis 命令名，使用小写，如 `set,del,flushall,flushdb` |

```ini
; 同步插入和更新，不同步删除。
[source.event]
include=*
exclude=delete

; 同步建表、修改表，不执行清空表。
[source.ddl]
include=create_table,alter_table,truncate_table
exclude=truncate_table

; 同步用户创建和授权，不同步删除用户。
[source.dcl]
include=create_user,grant,drop_user
exclude=drop_user

; 结构迁移包含库、表和约束，排除普通独立索引。
[source.structure]
include=database,table,constraint,index
exclude=index

; Redis 增量迁移不执行这两个命令。
[source.cmd]
exclude=flushall,flushdb
```

只配置当前任务需要的部分。DDL/DCL 取决于源、目标数据库及任务的支持范围。PostgreSQL CDC 同步 DDL 还需配置 `ddl_meta_tb` 和捕获触发器，见 [PostgreSQL 模板](../templates/pg_to_pg.md)。

结构迁移中，主键、唯一索引随表迁移，`index` 控制普通独立索引。PostgreSQL 的 sequence 和 comment 随表定义迁移，不能通过单独排除 `sequence` / `comment` 从表定义中移除它们。MongoDB 的结构类型参阅[结构迁移](structure/migration.md)。MongoDB DDL 类型使用 `mongo_create_collection`、`mongo_drop_collection`、`mongo_rename_collection`、`mongo_drop_database`、`mongo_create_index`、`mongo_drop_index`、`mongo_coll_mod`、`mongo_shard_collection`、`mongo_reshard_collection`、`mongo_refine_collection_shard_key`。

## 修改目标名称

`route` 只匹配精确名称，不支持通配路由。库/schema、表和列均可改名：

```ini
[source.db]
include=inventory,archive
route=inventory:archive,archive:history

[source.table]
route=inventory.orders:special.order_history

[source.column]
route=inventory.orders.id:special.order_history.order_id
```

| 源对象 | 目标对象 | 原因 |
| --- | --- | --- |
| `inventory.orders` | `special.order_history` | 表级规则优先 |
| `inventory.orders.id` | `special.order_history.order_id` | 列名映射 |
| `inventory.audit` | `archive.audit` | 使用库级规则 |
| `archive.audit` | `history.audit` | 使用另一条库级规则 |

每个对象只应用一次匹配，不会把 `inventory.audit` 连续改名成 `history.audit`。未配置映射的名称保持原样。列映射中的目标库和表必须与该表的路由结果一致；多个源列不能映射到同一个目标列。

PostgreSQL 使用 schema 规则：

```ini
[source.schema]
include=inventory
route=inventory:archive
[source.table]
route=inventory.orders:archive.order_history
[source.column]
route=inventory.orders.id:archive.order_history.order_id
```

SQL Server 写完整的数据库、schema、表路径：

```ini
[source.db]
include=market
[source.schema]
route=market.inventory:warehouse.sales
[source.table]
route=market.inventory.orders:warehouse.sales.order_history
[source.column]
route=market.inventory.orders.id:warehouse.sales.order_history.order_id
```

MongoDB 的集合使用 `[source.collection]`；例如 `route=inventory.orders:archive.order_history`。Redis 用 `[source.db]` 选择库编号，例如 `include=0,2`。

多个源库/schema 可以映射到同一个目标，正向路由允许多对一。全量写入监控在无法唯一确定来源时，使用路由后的目标表名称统计。校验和 Kafka 分发仍需要准确的源名称；请为这些功能配置完整且可区分的表映射，存在反向歧义时会返回错误。

## 分发映射

**`to_<kind>` 表示分发映射**，决定数据发送到目标端的哪个资源。`kind` 是接收数据的资源类型；`to_topic` 是其中 `kind=topic` 的特例。`route` 负责修改数据中的资源名称，`to_<kind>` 负责选择接收数据的位置，两者可以同时配置。

分发映射放在源资源对应的 `[source.<source_kind>]` 配置段下，格式为 `to_<kind>=源资源路径:目标资源路径`。配置段中的 `source_kind` 由源数据库决定，键名中的 `kind` 由目标数据库决定，两者可以不同。例如 `[source.table] to_topic=inventory.orders:events.orders` 表示将源表 `inventory.orders` 的数据发送到目标 topic `events.orders`。

| 目标资源类型 `<kind>` | INI 键名 | 目标资源路径示例 |
| --- | --- | --- |
| `db`（别名 `database`） | `to_db`（或 `to_database`） | `archive` |
| `schema` | `to_schema` | PostgreSQL：`sales`；SQL Server：`warehouse.sales` |
| `table` | `to_table` | MySQL / PostgreSQL：`archive.orders`；SQL Server：`warehouse.sales.orders` |
| `column` | `to_column` | MySQL / PostgreSQL：`archive.orders.id` |
| `collection` | `to_collection` | MongoDB：`archive.orders` |
| `topic` | `to_topic` | Kafka：`events.orders`，点号属于 topic 名称 |

目标路径按照目标数据库和资源类型填写。目标数据库不支持的已知资源类型会被忽略。

分发映射的匹配规则如下：

- 左侧始终使用源端名称，包括同时配置了 `route` 的情况；多个源资源可以映射到同一个目标资源。
- 支持精确名称，以及末尾连续的整段 `*`。例如两段路径的匹配优先级为 `inventory.orders`、`inventory.*`、`*.*`，与配置顺序无关。
- 不支持 `order*`、`?` 或正则这类模式；只有整个末尾段为 `*` 才表示通配。需要默认映射时，应覆盖所有末尾段，例如表级的 `*.*`。

上表说明通用配置的命名和路径规则；实际使用还取决于任务的目标写入支持。**当前内置写入流程使用的是 Kafka `to_topic`**，其他 `to_<kind>` 尚未接入目标写入流程。常规库、表、列改名使用 `route`。

### 拓扑示例：MySQL → Kafka → MySQL

本例将源 MySQL 的数据经 Kafka 写入另一套 MySQL。生产端用 `route` 修改消息中的库、表、列名，用 `to_topic` 将消息分发到三个 topic；消费端按消息中的名称写入目标表。这里 `kind=topic` 指 DTS 1 的直接目标端 Kafka；最终 MySQL 的库表名由消息携带。

```mermaid
flowchart LR
    S["源 MySQL"] --> P["DTS 1：MySQL → Kafka"]
    P --> T1["Kafka：events.orders"]
    P --> T2["Kafka：events.inventory"]
    P --> T3["Kafka：events.default"]
    T1 --> C1["DTS 2：Kafka → MySQL"]
    T2 --> C2["DTS 3：Kafka → MySQL"]
    T3 --> C3["DTS 4：Kafka → MySQL"]
    C1 --> D["目标 MySQL"]
    C2 --> D
    C3 --> D
```

先创建 `events.orders`、`events.inventory`、`events.default` 三个 topic，本例各使用 **1 个分区**。一个 Kafka 消费任务读取一个 topic 的一个分区，因此本例共启动 **4 个 DTS：1 个生产任务 + 3 个消费任务**。若合并为一个单分区 topic，则使用 1 个生产任务和 1 个消费任务。

**DTS 1：MySQL → Kafka，全量生产任务。** 保存为 `mysql_to_kafka.ini`，替换连接地址和账号。

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://user:password@source:3306

[sinker]
db_type=kafka
sink_type=write
url=kafka-host:9092
with_field_defs=true

[source.db]
include=inventory,other
route=inventory:archive

[source.table]
route=inventory.orders:archive.order_history
to_topic=*.*:events.default,inventory.*:events.inventory,inventory.orders:events.orders

[source.column]
route=inventory.orders.id:archive.order_history.order_id

[parallelizer]
parallel_type=snapshot
parallel_size=4

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs/mysql_to_kafka
log_level=info
```

| 源表 | 命中的分发规则 | Kafka topic | 消费任务 | 消息中的表名 / 最终目标表 |
| --- | --- | --- | --- | --- |
| `inventory.orders` | 精确表名 | `events.orders` | DTS 2 | `archive.order_history` |
| `inventory.audit` | `inventory.*` | `events.inventory` | DTS 3 | `archive.audit` |
| `other.orders` | `*.*` | `events.default` | DTS 4 | `other.orders` |

目标 MySQL 需提前创建对应表结构。其中 `inventory.orders.id` 在消息和目标表中都名为 `order_id`，其余未配置映射的列保持源名称。topic 只决定消息由哪个消费任务读取，最终目标表由消息中的库、表名确定。

这里的 `to_topic` 沿用通用分发规则：左侧使用源表名，精确表名优先于库/schema 通配规则，最后使用 `*.*`。例如消息里的表名已改为 `archive.order_history`，仍按源表名 `inventory.orders` 选择 `events.orders`。

**DTS 2：Kafka → MySQL，消费 `events.orders`。** 保存为 `orders_to_mysql.ini`：

```ini
[extractor]
db_type=kafka
extract_type=cdc
url=kafka-host:9092
group=orders_to_mysql
topic=events.orders
partition=0
offset=0
ack_interval_secs=1

[sinker]
db_type=mysql
sink_type=write
url=mysql://user:password@target:3306
batch_size=200

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs/orders_to_mysql
log_level=info
```

将此文件复制为另两个消费任务配置，只修改下表中的配置项；三个任务连接同一个目标 MySQL：

| 任务 / 配置文件 | `[extractor] topic` | `[extractor] group` | `[runtime] log_dir` |
| --- | --- | --- | --- |
| DTS 2 / `orders_to_mysql.ini` | `events.orders` | `orders_to_mysql` | `./logs/orders_to_mysql` |
| DTS 3 / `inventory_to_mysql.ini` | `events.inventory` | `inventory_to_mysql` | `./logs/inventory_to_mysql` |
| DTS 4 / `default_to_mysql.ini` | `events.default` | `default_to_mysql` | `./logs/default_to_mysql` |

先启动三个消费任务，再启动生产任务。这里 `partition=0` 对应各 topic 的唯一分区，`offset=0` 用于从新建 topic 的起始位置读取；已有数据的 topic 应按实际续传位置设置 offset。多个分区需要分别配置消费任务。

消费任务的 `extract_type=cdc` 表示持续读取 Kafka 消息，生产端仍然是 MySQL 全量任务。本例过滤、改名和分发都在生产任务中配置；消费任务直接按消息中的库、表、列名落表。Kafka 订阅通过 `[extractor]` 的 `topic`、`partition`、`offset` 指定。

PostgreSQL → Kafka → PostgreSQL 使用相同的任务拓扑；生产端库级规则改用 `[source.schema]`，两端数据库类型、连接地址和目标表结构改为 PostgreSQL。
