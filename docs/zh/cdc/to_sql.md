# 根据增量数据生成 sql

订阅源库的数据变更，并根据变更生成正向或反向 sql，结果存储在 sql.log。

支持数据库：
- MySQL
- PG

支持变更类型：
- 仅支持 DML 变更

## CDC + Checker

如需校验 CDC 落库后的数据，可在 CDC 任务配置中开启 `[checker]`。配置与限制请参考 [数据校验](../snapshot/check.md)。
若 `[checker]` 与 `[sinker]` 同时指定目标，优先使用 `[checker]`。

# 示例

## 示例 1：生成正向 sql
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=0
binlog_filename=
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_tbs=test_db.*
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql
```

源端执行 sql：
```
use test_db;
insert into test_tb values(1, 1);
update test_tb set value=2 where id=1;
delete from test_tb where id=1;
```

生成 sql.log：
```
INSERT INTO `test_db`.`test_tb`(`id`,`value`) VALUES(1,1);
UPDATE `test_db`.`test_tb` SET `id`=1,`value`=2 WHERE `id` = 1;
DELETE FROM `test_db`.`test_tb` WHERE `id` = 1;
```

## 示例 2：生成反向 sql
在示例 1 的配置基础上，添加：

```
[sinker]
reverse=true
```

源端执行 sql：
```
use test_db;
insert into test_tb values(1, 1);
update test_tb set value=2 where id=1;
delete from test_tb where id=1;
```

生成 sql.log：
```
DELETE FROM `test_db`.`test_tb` WHERE `id` = 1;
UPDATE `test_db`.`test_tb` SET `id`=1,`value`=1 WHERE `id` = 1;
INSERT INTO `test_db`.`test_tb`(`id`,`value`) VALUES(1,2);
```

## 设置起止时间
如果想订阅某一段时间的变更，添加配置：

```
[extractor]
start_time_utc=2024-10-09 02:00:00
end_time_utc=2024-10-09 03:00:00
```

# 数据回滚
如果在源库错误地执行了某些 sql，想将数据回滚，可以：
- 生成反向 sql，并从最后一条到第一条反序执行。
