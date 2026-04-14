# 结构校验

结构迁移后，您可使用两种校验方式：一种是 ape-dts 自带的校验器，另一种是第三方工具 [Liquibase](./check_by_liquibase.md)。本文档主要介绍前者。

结构校验与 CDC 无直接关系。这里的 `inline cdc check` 指行级数据校验
（见 [数据校验文档](../snapshot/check.md)）。

## 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md)

# 校验结果

结构校验会输出 JSON 格式的 `miss.log`、`diff.log`、`summary.log`；当 `output_revise_sql=true` 时，还会额外输出 `sql.log`。

`miss.log` 和 `diff.log` 均采用相同的 JSON 结构（`StructCheckLog`）：

```json
{
  "key": "type.schema.table", // 例如: table.db_name.tb_name 或 index.db.tb.idx
  "src_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)",  // 出现在 miss，以及包含源端定义的 diff 条目中
  "dst_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)"   // 出现在 diff 中；仅目标端存在的对象可能只有 dst_sql
}
```

- `miss.log`（源端存在但目标端缺失）
```json
{"key":"table.struct_check_test_1.not_match_miss","src_sql":"CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY)"}
{"key":"index.struct_check_test_1.not_match_index.i6_miss","src_sql":"CREATE INDEX `i6_miss` ON `not_match_index` (`col6`)"}
```

- `diff.log`（对象定义不一致，或对象仅存在于目标端）
```json
{"key":"index.struct_check_test_1.not_match_index","src_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v1` (`col1`)","dst_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v2` (`col1`)"}
{"key":"table.struct_check_test_1.not_match_column","src_sql":"CREATE TABLE `not_match_column` (`id` int)","dst_sql":"CREATE TABLE `not_match_column` (`id` bigint)"}
{"key":"index.struct_check_test_1.full_index_type.index_not_match_name_dst","dst_sql":"CREATE UNIQUE INDEX IF NOT EXISTS \"index_not_match_name_dst\" ON \"struct_check_test_1\".\"full_index_type\" USING btree (unique_col)"}
```

- `summary.log`（校验结果概览）
```json
{"start_time": "2023-10-01T10:00:00+08:00", "end_time": "2023-10-01T10:00:05+08:00", "is_consistent": false, "miss_count": 8, "diff_count": 5, "sql_count": 14}
```

- `sql.log`（当配置 `output_revise_sql=true` 时生成）
```sql
CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY);
```

# 适用范围

- 结构校验会对经过路由与过滤后选中的源端结构，与目标端对应结构进行对比。
- 仅存在于目标端的额外对象会记录到 `diff.log`。
- 过滤范围之外的库 / schema / 表对象不会被校验。
