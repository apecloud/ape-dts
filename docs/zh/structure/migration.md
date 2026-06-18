# 结构迁移

- 使用范围：MySQL、PG、MongoDB。
- 迁移内容：
  - MySQL/PostgreSQL：database(mysql)、schema(pg)、table、comment、index、sequence(pg)、constraints。
  - MongoDB：collection、index、shardkey。

# 示例: MySQL -> MySQL / Mongo -> Mongo

参考 [MySQL 任务模版](../../templates/mysql_to_mysql.md)、[MySQL 教程](../../en/tutorial/mysql_to_mysql.md)、
[Mongo 任务模版](../../templates/mongo_to_mongo.md) 和 [Mongo 教程](../../en/tutorial/mongo_to_mongo.md)。

## 说明

结构迁移采用单线程串行执行，需注意的配置有：

```
[extractor]
extract_type=struct

[sinker]
sink_type=struct
batch_size=1

[parallelizer]
parallel_type=serial
parallel_size=1
```

需设置失败策略，包括 interrupt 和 ignore 两种，默认是 interrupt。

- interrupt：一旦某个结构迁移失败，任务退出。
- ignore：某个结构迁移失败，不影响其他结构继续迁移，但会记录错误日志。

```
[sinker]
conflict_policy=interrupt
```

# 分阶段结构迁移

在包含 结构迁移 + 数据迁移 的完整数据迁移中，有时为了提升数据迁移的速度，会将整个过程拆分成 3 个步骤：

1. 迁移 库表结构 + 主键/唯一键（这些是后续数据迁移所必须的）；
2. 数据迁移；
3. 迁移索引+约束。

为此，我们提供了 2 种 filter 机制（其他配置保持不变）。

## 只迁移 库表结构 + 主键 + 唯一键

```
[filter]
do_structures=database,table
```

## 只迁移 索引 + 约束

```
[filter]
do_structures=constraint,index
```

## MongoDB 结构类型

```
[filter]
do_structures=collection,shardkey
```

MongoDB 不使用独立的 `database` 结构类型。`collection` 会创建选中的 collection 并同步
collection options，`shardkey` 会同步源端 sharded collection 的分片定义。
如果目标端是 sharded cluster，应通过 `mongos` 连接；DTS 会在需要时先执行 `enableSharding`，
再执行 `shardCollection`。如果目标端不是 `mongos`，shardkey 语句会被忽略。
