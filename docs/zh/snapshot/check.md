# 数据校验

数据迁移完成后，需要对源数据和目标数据进行逐行逐列比对。如果数据量过大，可以进行抽样校验。请确保需要校验的表具有主键/唯一键。

支持对 MySQL/PG/Mongo 进行比对。

# 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

## 抽样校验

在全量校验配置下，添加 `sample_interval` 配置。即，每 3 条记录采样 1 次。
```
[extractor]
sample_interval=3
```

## 说明

此配置和全量同步任务的基本一致，两者的不同之处是：

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# 校验结果

校验结果以 json 格式写入日志中，包括 diff.log 和 miss.log。日志存放在 log/check 子目录中。

## 差异日志（diff.log）

差异日志包括库（schema）、表（tb）、主键/唯一键（id_col_values）、差异列的源值和目标值（diff_col_values）。

```json
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## 缺失日志（miss.log）

缺失日志包括库（schema）、表（tb）和主键/唯一键（id_col_values），diff_col_values 为空。

```json
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null},"diff_col_values":{}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"},"diff_col_values":{}}
```

## 输出修复 SQL

业务若需要人工修复差异数据，可以在 `[sinker]` 中开启 SQL 输出：

```
[sinker]
output_revise_sql=true
# 可选：强制使用全字段匹配 WHERE 条件
revise_match_full_row=true
```

开启后，diff/miss 日志会追加 `revise_sql` 字段。缺失记录会生成 `INSERT` 语句，差异记录会生成 `UPDATE` 语句。`revise_match_full_row=true` 时，即使表存在主键也会使用整行数据生成 WHERE 条件，以便通过完整行值定位目标数据。

示例：

```json
{
  "log_type": "Diff",
  "schema": "test_db_1",
  "tb": "one_pk_no_uk",
  "id_col_values": {"f_0": "4"},
  "diff_col_values": {"f_1": {"src": "2", "dst": "1"}},
  "revise_sql": "UPDATE `test_db_1`.`one_pk_no_uk` SET `f_1`='2' WHERE `f_0` = 4;"
}
```

# 反向校验

将 [extractor] 和 [sinker] 配置调换，即可进行反向校验。

# 其他配置

- 支持 [router]，详情请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
