# 结构校验

结构迁移后，您可使用两种校验方式：一种是 ape-dts 自带的校验器，另一种是第三方工具 [Liquibase](./check_by_liquibase.md)。本文档主要介绍前者。

结构校验与 CDC 无直接关系。"CDC + checker"指的是行级数据校验（见 [数据校验文档](../snapshot/check.md)）。

## 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md)

# 校验结果

以源端结构为基准，校验结果包括 **miss**、**diff** 以及 **summary** 等部分，均以 JSON 格式呈现。

`miss.log` 和 `diff.log` 均采用相同的 JSON 结构（`StructCheckLog`）：

```json
{
  "key": "type.schema.table", // 例如: table.db_name.tb_name 或 index.db.tb.idx
  "src_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)",  // 在 miss/diff 中出现
  "dst_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)"   // 仅在 diff 中出现
}
```

- `miss.log`（源端存在但目标端缺失）
```json
{"key":"table.struct_check_test_1.not_match_miss","src_sql":"CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY)"}
{"key":"index.struct_check_test_1.not_match_index.i6_miss","src_sql":"CREATE INDEX `i6_miss` ON `not_match_index` (`col6`)"}
```

- `diff.log`（两端都存在但不一致；同时包含 src_sql 和 dst_sql）
```json
{"key":"index.struct_check_test_1.not_match_index","src_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v1` (`col1`)","dst_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v2` (`col1`)"}
{"key":"table.struct_check_test_1.not_match_column","src_sql":"CREATE TABLE `not_match_column` (`id` int)","dst_sql":"CREATE TABLE `not_match_column` (`id` bigint)"}
```

- `summary.log`（校验结果概览）
```json
{"start_time": "2023-10-01T10:00:00+08:00", "end_time": "2023-10-01T10:00:05+08:00", "is_consistent": false, "miss_count": 8, "diff_count": 5, "sql_count": 14}
```

- `sql.log`（当配置 `output_revise_sql = true` 时生成）
```sql
CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY);
```

# 限制

- 结构校验为源端驱动（仅验证 Source ∈ Target），无法检测目标端多出的结构对象。
