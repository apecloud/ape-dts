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

校验结果以 json 格式写入日志中，包括 diff.log, miss.log, sql.log 和 summary.log。日志存放在 log/check 子目录中。

## 差异日志（diff.log）

差异日志包括库（schema）、表（tb）、主键/唯一键（id_col_values）、差异列的源值和目标值（diff_col_values）。

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

当源端与目标端的类型不同（如 Int32 对 Int64，或 None 对 Short），`src_type`/`dst_type` 会出现在对应列下，明确标出类型不一致。Mongo 也适用这一规则，差异日志会输出 BSON 类型名称。

只有在路由对 schema 或 table 进行重命名时，日志才会补充 `target_schema`/`target_tb` 来标识目的端真实库表；`schema`、`tb` 依旧表示源端，方便排查。

## 缺失日志（miss.log）

缺失日志包括库（schema）、表（tb）和主键/唯一键（id_col_values）。由于缺失记录不存在差异列，因此不会输出 `diff_col_values`。

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

## 输出完整行

当业务需要完整行内容用于排查异常，可以在 `[sinker]` 中开启全行日志：

```
[sinker]
output_full_row=true
```

开启后，所有 diff.log 会追加 `src_row` 与 `dst_row`，miss.log 会追加 `src_row`（当前仅支持 MySQL/PG/Mongo，Redis 仍不支持）。示例：

```json
{
  "schema": "test_db_1",
  "tb": "one_pk_multi_uk",
  "id_col_values": {
    "f_0": "5"
  },
  "diff_col_values": {
    "f_1": {
      "src": "5",
      "dst": "5000"
    },
    "f_2": {
      "src": "ok",
      "dst": "after manual update"
    }
  },
  "src_row": {
    "f_0": 5,
    "f_1": 5,
    "f_2": "ok"
  },
  "dst_row": {
    "f_0": 5,
    "f_1": 5000,
    "f_2": "after manual update"
  }
}
```

## 输出修复 SQL

业务若需要人工修复差异数据，可以在 `[sinker]` 中开启 SQL 输出：

```
[sinker]
output_revise_sql=true
# 可选：强制使用全字段匹配 WHERE 条件
revise_match_full_row=true
```

开启后，缺失记录的 `INSERT` 语句与差异记录的 `UPDATE` 语句会被写入 `sql.log`。`revise_match_full_row=true` 时，即使表存在主键也会使用整行数据生成 WHERE 条件，以便通过完整行值定位目标数据。若路由没有改名就不会输出 `target_schema`/`target_tb`，只在改名时才需要参考这两个字段决定 SQL 应执行的表。

生成的 SQL 本质上是 sinker 需要执行、用以把目标数据修正到和源端一致的 SQL；它直接使用了真正的目的端 schema/table，所以可以直接在目标执行（路由改名时仍可参考 `target_schema`/`target_tb` 判断最终目标对象）。

示例：

```json
{
  "schema": "test_db_1",
  "tb": "one_pk_no_uk",
  "target_schema": "target_db",
  "target_tb": "target_tb",
  "id_col_values": {"f_0": "4"},
  "diff_col_values": {"f_1": {"src": "2", "dst": "1"}}
}
```

`sql.log` 示例：

```sql
UPDATE `target_db`.`target_tb` SET `f_1`='2' WHERE `f_0` = 4;
```

缺失记录日志示例：

```json
{
  "schema": "test_db_1",
  "tb": "test_table",
  "id_col_values": {"id": "3"}
}
```

`sql.log` 示例：

```sql
INSERT INTO `test_db_1`.`test_table`(`id`,`name`,`age`,`email`) VALUES(3,'Charlie',35,'charlie@example.com');
```

### 概览日志（summary.log）
概览日志包含校验的总体结果，如 start_time, end_time, is_consistent，以及 miss, diff, extra 的数量。

```json
{"start_time": "2023-09-01T12:00:00+08:00", "end_time": "2023-09-01T12:00:01+08:00", "is_consistent": false, "miss_count": 1, "diff_count": 2, "extra_count": 1, "sql_count": 3}
```


# 反向校验

将 [extractor] 和 [sinker] 配置调换，即可进行反向校验。

# 其他配置

- 支持 [router]，详情请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
