# 数据校验

数据迁移完成后，需要对源数据和目标数据进行逐行逐列比对。如果数据量过大，可以进行抽样校验。请确保需要校验的表具有主键/唯一键。

支持对 MySQL、PostgreSQL、MongoDB 进行比对。

数据校验可用于 Snapshot 与 CDC 任务。若为 CDC 任务，保持 `[checker]` 开启并设置 `extract_type=cdc`，checker 会在数据写入目标端后进行校验。

## 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

### 抽样校验

在全量校验配置下，在 `[extractor]` 中添加 `sample_interval` 配置。例如设置 `sample_interval=3` 表示每 3 条记录采样 1 次。
```
[extractor]
sample_interval=3
```

## 限制

- 数据校验为源端驱动（仅验证 Source ∈ Target），无法发现目标端多余数据（幽灵数据）。如需检测目标端多余数据，可通过 [反向校验](#反向校验) 交换 extractor 和 checker 配置。
- 对于 MongoDB，`_id` 需为可哈希类型（例如 ObjectId/String/Int32/Int64）。不支持的 `_id` 类型会导致 checker 报错。

## DELETE 事件校验

在 CDC + Check 场景下，checker 会校验 DELETE 事件：通过主键在目标端查询，若目标端仍存在该行则判定为不一致，记录到 `diff.log`（`diff_col_values` 为空）。开启 `output_revise_sql=true` 时，会自动生成对应的 `DELETE` 修复语句写入 `sql.log`。

# 校验结果

校验结果以 JSON 格式写入日志，包括 diff.log、miss.log、sql.log 和 summary.log。日志存放在 `log/check` 子目录中。

## 差异日志（diff.log）

差异日志包括库（schema）、表（tb）、主键/唯一键（id_col_values）、差异列的源值和目标值（diff_col_values）。

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

当源端与目标端的类型不同（如 Int32 对 Int64，或 None 对 Short）时，`src_type`/`dst_type` 会出现在对应列下，明确标出类型不一致。MongoDB 也适用这一规则，差异日志会输出 BSON 类型名称。

只有在路由对 schema 或 table 进行重命名时，日志才会补充 `target_schema`/`target_tb` 来标识目的端真实库表。`schema`、`tb` 依旧表示源端，方便排查。

## 缺失日志（miss.log）

缺失日志包括库（schema）、表（tb）和主键/唯一键（id_col_values）。由于缺失记录不存在差异列，因此不会输出 `diff_col_values`。

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

## 输出完整行

当需要完整行内容用于排查问题时，可以在 `[checker]` 中开启全行日志：

```
[checker]
output_full_row=true
```

开启后，所有 diff.log 条目会追加 `src_row` 与 `dst_row`，miss.log 条目会追加 `src_row`（当前仅支持 MySQL/PG/Mongo，Redis 暂不支持）。示例：

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

如需人工修复差异数据，可以在 `[checker]` 中开启 SQL 输出：

```
[checker]
output_revise_sql=true
# 可选：强制使用全字段匹配 WHERE 条件
revise_match_full_row=true
```

开启后，缺失记录的 `INSERT` 语句与差异记录的 `UPDATE` 语句会被写入 `sql.log`。

当 `revise_match_full_row=true` 时，即使表存在主键也会使用整行数据生成 WHERE 条件，以便通过完整行值定位目标数据。

若路由没有对 schema 或 table 改名，则不会输出 `target_schema`/`target_tb`。这两个字段仅在路由改名时用于确定 SQL 应执行的目标表。

生成的 SQL 直接使用真正的目的端 schema/table，可以直接在目标端执行。路由改名时可参考 `target_schema`/`target_tb` 判断最终目标对象。

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

## 概览日志（summary.log）

概览日志包含校验的总体结果，如 start_time、end_time、is_consistent，以及 miss、diff 的数量。

```json
{"start_time": "2023-09-01T12:00:00+08:00", "end_time": "2023-09-01T12:00:01+08:00", "is_consistent": false, "miss_count": 1, "diff_count": 2, "sql_count": 3}
```

# 反向校验

数据校验为源端驱动，只验证源端数据是否存在于目标端。若需检测目标端中多余的数据（源端不存在），可通过交换 `[extractor]` 和 `[checker]` 的目标配置来进行反向校验：

```
# 原始：源端=A，目标端=B
# 反向：源端=B，目标端=A
[extractor]
url=<原 checker 的 url>

[checker]
url=<原 extractor 的 url>
```

# 配置

`[checker]` 的完整配置与目标选择规则请参考 [config.md](../config.md)。

## 重试机制

当 `max_retries > 0` 时，checker 会在检测到不一致时自动重试：
- 重试期间不记录日志，避免噪音
- 仅在最后一次检查时记录详细的 miss/diff 日志
- 适用于目标端数据尚未完全同步的场景

## 路由

支持 `[router]` 配置，详情请参考 [配置详解](../config.md)。

## 集成测试参考

参考各类型集成测试的 `task_config.ini`：
- dt-tests/tests/mysql_to_mysql/check
- dt-tests/tests/pg_to_pg/check
- dt-tests/tests/mongo_to_mongo/check
