# 结构校验

结构迁移后，您可使用两种校验方式：一种是 ape-dts 自带的校验器，另一种是第三方工具 [Liquibase](./check_by_liquibase.md)。本文档主要介绍前者。

结构校验与 CDC 无直接关系。这里的 `inline cdc check` 指行级数据校验
（见 [数据校验文档](../snapshot/check.md)）。

## 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md)

# 校验结果

结构校验会以 JSON Lines 输出 `miss.log`、`diff.log`、`summary.log`；当
`output_revise_sql=true` 时，还会额外输出 `sql.log`。`sql.log` 是纯 SQL 文件，每行一条
语句，不包含 JSON 包装，也不携带 schema/table/id metadata。

`miss.log` 和 `diff.log` 均采用相同的 JSON 结构（`StructCheckLog`）。`src_sql` 与
`dst_sql` 是可选字段，仅在对应侧存在结构定义时输出：

```json
{
  "schema": "db_name",
  "tb": "tb_name",
  "id_col_values": {
    "object_key": "type.schema.table"
  },
  "src_sql": "source definition SQL",
  "dst_sql": "target definition SQL"
}
```

结构日志没有顶层 `key`、`target_schema`、`target_tb` 字段。结构对象身份放在
`id_col_values.object_key`。源端定义存在时输出 `src_sql`，目标端定义存在时输出 `dst_sql`。
源端独有的缺失对象通常只有 `src_sql`；定义不一致的对象同时包含 `src_sql` 和 `dst_sql`；
目标端独有的额外对象只有 `dst_sql`。

`object_key` 格式：

```text
<object_type>.<schema>.<table_or_object>[.<sub_object>]
```

常见例子包括 `table.struct_check_test_1.not_match_column`、
`index.struct_check_test_1.not_match_index.i6_miss`、
`constraint.struct_check_test_1.not_match_missing.not_match_missing_pkey`、
`table_comment.struct_check_test_1.not_match_comment`、
`column_comment.struct_check_test_1.not_match_comment.id`。

- `miss.log`（源端存在但目标端缺失）
```json
{"schema":"struct_check_test_1","tb":"not_match_miss","id_col_values":{"object_key":"table.struct_check_test_1.not_match_miss"},"src_sql":"CREATE TABLE `not_match_miss` (`id` int NOT NULL, PRIMARY KEY (`id`))"}
{"schema":"struct_check_test_1","tb":"not_match_index","id_col_values":{"object_key":"index.struct_check_test_1.not_match_index.i6_miss"},"src_sql":"CREATE INDEX `i6_miss` ON `not_match_index` (`c6`)"}
```

- `diff.log`（对象定义不一致，或对象仅存在于目标端）
```json
{"schema":"struct_check_test_1","tb":"not_match_index","id_col_values":{"object_key":"index.struct_check_test_1.not_match_index"},"src_sql":"CREATE INDEX `i1` ON `not_match_index` (`c1`)","dst_sql":"CREATE INDEX `i1` ON `not_match_index` (`c2`)"}
{"schema":"struct_check_test_1","tb":"not_match_column","id_col_values":{"object_key":"table.struct_check_test_1.not_match_column"},"src_sql":"CREATE TABLE `not_match_column` (`id` int NOT NULL, PRIMARY KEY (`id`))","dst_sql":"CREATE TABLE `not_match_column` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}
{"schema":"struct_check_test_1","tb":"full_index_type","id_col_values":{"object_key":"index.struct_check_test_1.full_index_type.index_not_match_name_dst"},"dst_sql":"CREATE INDEX `index_not_match_name_dst` ON `full_index_type` (`c1`)"}
```

- `summary.log`（校验结果概览）
```json
{"start_time":"2023-10-01T10:00:00+08:00","end_time":"2023-10-01T10:00:05+08:00","is_consistent":false,"miss_count":8,"diff_count":5,"skip_count":0,"sql_count":14,"tables":[{"schema":"struct_check_test_1","tb":"not_match_column","checked_count":0,"miss_count":0,"diff_count":1,"skip_count":0},{"schema":"struct_check_test_1","tb":"not_match_miss","checked_count":0,"miss_count":1,"diff_count":0,"skip_count":0}]}
```

- `sql.log`（当配置 `output_revise_sql=true` 时生成）
```sql
CREATE TABLE IF NOT EXISTS `struct_check_test_1`.`not_match_miss` (`id` int NOT NULL, PRIMARY KEY (`id`));
```

# 适用范围

- 结构校验会对经过路由与过滤后选中的源端结构，与目标端对应结构进行对比。
- 仅存在于目标端的额外对象会记录到 `diff.log`。
- 过滤范围之外的库 / schema / 表对象不会被校验。
