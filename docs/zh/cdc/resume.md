# 断点续传

任务启动时，可以基于配置获取到上次执行中最新的任务位点，从而继续任务而不需要从头开始

## 支持范围

- MySQL 源端
- Postgres 源端
- Mongo 源端

## CDC + Checker

如需校验 CDC 落库后的数据，可在 CDC 任务配置中开启 `[checker]`。配置与限制请参考 [数据校验](../snapshot/check.md)。
若 `[checker]` 与 `[sinker]` 同时指定目标，优先使用 `[checker]`。

## 位点记录

增量任务进度会定期记录在 position.log 中。任务进度记录频率取决于 `pipeline.checkpoint_interval_secs` 配置，默认值 10s

无论你是否启用断点续传，任务运行过程中都会以日志的方式记录位点信息，位置在 logs 目录(`runtime.log_dir`)下的 position.log

此外，你可以通过配置将任务位点持久化在 `目标数据库` 或 `指定数据库`，往数据库记录位点大约会花费 `150byte`存储。
为确保位点信息不被其他任务影响，建议通过配置文件指定 task_id: `global.task_id`
**该功能仅支持目标数据库/指定数据库是 MySQL 或 PG**
**需要目标账号具备创建 MySQL database/PG schema 以及创建 table 的权限**

## 位点读取

你可以通过配置选择 `从日志`、`从目标库`或`从指定数据库`读取位点

# 进度日志

详细解释可参考 [位点信息](../monitor/position.md)

## MySQL position.log

# 配置

## 使用目标库断点续传

```
[global]
//[可选]
task_id=task1

[resumer]
resume_type=from_target
//[可选]默认值是apecloud_metadata.apedts_task_position
table_full_name=apecloud_resumer_test.ape_task_position
max_connections=1
```

任务启动时会自动确保目标存在配置中的`apecloud_resumer_test.ape_task_position`库表，初始化一个最大连接数是 1 的连接池，用于后续的 resume 相关位点记录和查询

## 使用指定数据库断点续传

```
[global]
//[可选]
task_id=task1

[resumer]
resume_type=from_db
url=mysql://xxx:xxx@127.0.0.1:3306
db_type=mysql
//[可选]默认值是apecloud_metadata.apedts_task_position
table_full_name=apecloud_resumer_test.ape_task_position
max_connections=1
```

任务启动时会初始化一个最大连接数是 1 的连接池，自动确保配置数据库实例中存在配置中的`apecloud_resumer_test.ape_task_position`库表，用于后续的 resume 相关位点记录和查询

## 使用日志断点续传

```
[runtime]
log_dir=/logs

[resumer]
resume_type=from_log
//[可选]默认使用runtime.log_dir
log_dir=/other_logs
```

从`/other_logs`下寻找 position.log 做断点续传
