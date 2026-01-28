# Check structures

After structure migration, you can choose from two methods for verification. One is provided by us, and the other is an open source tool called [Liquibase](./check_liquibase.md). This document primarily focuses on the former one.

Structure check is independent of CDC. CDC + checker applies to row-level data check (see data check docs).

# Example: MySQL -> MySQL
Refer to [task templates](../../templates/mysql_to_mysql.md)

# Results

Based on the source structures, the check results include **miss**, **diff**, **extra**, and **summary**, all presented in JSON.
`miss.log`, `diff.log`, `extra.log` all use the same JSON structure (`StructCheckLog`):

## Limitations

- Data check (checker) is source-driven (validates Source ∈ Target) and cannot detect extra rows that exist only in the target (ghost data). This means missing deletes on the target side will not be detected.

```json
{
  "key": "type.schema.table", // e.g., table.db_name.tb_name or index.db.tb.idx
  "src_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)",  // appears in miss/diff
  "dst_sql": "CREATE TABLE `table_name` (id INT PRIMARY KEY)"   // appears in diff/extra
}
```

- `miss.log` (Present in source but missing in destination)
```json
{"key":"table.struct_check_test_1.not_match_miss","src_sql":"CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY)"}
{"key":"index.struct_check_test_1.not_match_index.i6_miss","src_sql":"CREATE INDEX `i6_miss` ON `not_match_index` (`col6`)"}
```

- `diff.log` (Present in both but different; contains both src_sql and dst_sql)
```json
{"key":"index.struct_check_test_1.not_match_index","src_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v1` (`col1`)","dst_sql":"ALTER TABLE `not_match_index` ADD INDEX `idx_v2` (`col1`)"}
{"key":"table.struct_check_test_1.not_match_column","src_sql":"CREATE TABLE `not_match_column` (`id` int)","dst_sql":"CREATE TABLE `not_match_column` (`id` bigint)"}
```

- `extra.log` (Present in destination but missing in source)
```json
{"key":"index.struct_check_test_1.not_match_index.i5_diff_name_dst","dst_sql":"CREATE INDEX `i5_diff_name_dst` ON `not_match_index` (`col5`)"}
```

- `summary.log` (Overview of the check results)
```json
{"start_time": "2023-10-01T10:00:00+08:00", "end_time": "2023-10-01T10:00:05+08:00", "is_consistent": false, "miss_count": 8, "diff_count": 5, "extra_count": 1, "sql_count": 14}
```

- `sql.log` (generated when `output_revise_sql = true`)
```sql
CREATE TABLE IF NOT EXISTS `not_match_miss` (`id` int NOT NULL PRIMARY KEY);
```
