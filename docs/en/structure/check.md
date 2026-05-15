# Structure Check

After structure migration, you can choose from two verification methods. One is the built-in checker provided by ape-dts, and the other is an open-source tool called [Liquibase](./check_by_liquibase.md). This document focuses on the built-in checker.

Structure check is independent of CDC. Here, `inline cdc check` refers to row-level data check
(see [data check docs](../snapshot/check.md)).

## Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md)

# Results

Structure check writes `miss.log` and `diff.log` as JSON Lines only when there are missing or
different objects. `summary.log` contains exactly one JSON line for the overall summary. When
`output_revise_sql=true` and repair SQL exists, it also writes repair statements to `sql.log`.
`sql.log` is plain SQL, one statement per line, with no JSON wrapper or schema/table/id metadata.

`miss.log` and `diff.log` use the same JSON structure (`StructCheckLog`). `src_sql` and
`dst_sql` are optional and appear only when the corresponding side has a definition:

```json
{
  "key": "index.db_name.tb_name.idx_name",
  "src_sql": "source definition SQL",
  "dst_sql": "target definition SQL"
}
```

`key` identifies the structure object and is always present. Structure logs do not contain
`schema`, `tb`, `id_col_values`, `target_schema`, or `target_tb`. `src_sql` is included when the
source-side definition exists; `dst_sql` is included when the target-side definition exists.
Source-only missing objects usually have only `src_sql`; objects with different definitions have
both `src_sql` and `dst_sql`; target-only extra objects have only `dst_sql`.

The structure key has this format:

```text
<object_type>.<schema>.<table_or_object>[.<sub_object>]
```

Common table-scoped examples include `table.struct_check_test_1.not_match_column`,
`index.struct_check_test_1.not_match_index.i6_miss`,
`constraint.struct_check_test_1.not_match_missing.not_match_missing_pkey`,
`table_comment.struct_check_test_1.not_match_comment`, and
`column_comment.struct_check_test_1.not_match_comment.id`. PostgreSQL global objects use
object-specific keys such as `udt.schema.type_name`, `udf.schema.function_name(arguments)`, and
`rbac.role.role_name`.

- `miss.log` (present in source but missing in target)
```json
{"key":"table.struct_check_test_1.not_match_miss","src_sql":"CREATE TABLE `not_match_miss` (`id` int NOT NULL, PRIMARY KEY (`id`))"}
{"key":"index.struct_check_test_1.not_match_index.i6_miss","src_sql":"CREATE INDEX `i6_miss` ON `not_match_index` (`c6`)"}
```

- `diff.log` (object definition differs, or the object exists only in the target)
```json
{"key":"index.struct_check_test_1.not_match_index.i1","src_sql":"CREATE INDEX `i1` ON `not_match_index` (`c1`)","dst_sql":"CREATE INDEX `i1` ON `not_match_index` (`c2`)"}
{"key":"table.struct_check_test_1.not_match_column","src_sql":"CREATE TABLE `not_match_column` (`id` int NOT NULL, PRIMARY KEY (`id`))","dst_sql":"CREATE TABLE `not_match_column` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}
{"key":"index.struct_check_test_1.full_index_type.index_not_match_name_dst","dst_sql":"CREATE INDEX `index_not_match_name_dst` ON `full_index_type` (`c1`)"}
```

- `summary.log` (overview of the check results)
```json
{"start_time":"2023-10-01T10:00:00+08:00","end_time":"2023-10-01T10:00:05+08:00","is_consistent":false,"miss_count":8,"diff_count":5,"skip_count":0,"sql_count":14,"tables":[{"schema":"struct_check_test_1","tb":"not_match_column","checked_count":0,"miss_count":0,"diff_count":1,"skip_count":0},{"schema":"struct_check_test_1","tb":"not_match_miss","checked_count":0,"miss_count":1,"diff_count":0,"skip_count":0}]}
```

- `sql.log` (generated when `output_revise_sql=true`)
```sql
CREATE TABLE IF NOT EXISTS `struct_check_test_1`.`not_match_miss` (`id` int NOT NULL, PRIMARY KEY (`id`));
```

# Scope

- Structure check compares the source structures selected by the configured routing and filters with the corresponding target structures.
- Extra objects that exist only in the target are reported in `diff.log`.
- Objects outside the selected databases/schemas and filters are not checked.
