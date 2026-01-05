# Data Check

After data migration, you may want to compare the source and target data row by row and column by column. If the data volume is too large, you can perform sampling check. Please ensure that the tables to be checked have primary keys/unique keys.

Support comparison for MySQL/PG/Mongo.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

## Sampling Check

In the full check configuration, add `sample_interval` configuration. That is, sample 1 record for every 3 records.
```
[extractor]
sample_interval=3
```

## Note

This configuration is similar to the full synchronization task. The only differences are:

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# Check Results

The check results are written to the log in json format, including diff.log, miss.log, sql.log and summary.log. The logs are stored in the log/check subdirectory.

## Difference Log (diff.log)

Difference logs include database (schema), table (tb), primary key/unique key (id_col_values), source and target values of difference columns (diff_col_values).

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

When the source and target types are different (such as Int32 vs Int64, or None vs Short), `src_type`/`dst_type` will appear under the corresponding column, clearly marking the type inconsistency. Mongo also applies this rule, and the difference log will output the BSON type name.

Only when the route renames the schema or table, the log will supplement `target_schema`/`target_tb` to identify the real destination database table; `schema`, `tb` still represent the source, facilitating troubleshooting.

## Missing Log (miss.log)

Missing logs include database (schema), table (tb) and primary/unique key (id_col_values). Since missing records do not have difference columns, `diff_col_values` will not be output.

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

## Output Full Row

When the business needs full row content for troubleshooting exceptions, you can enable full row logging in `[sinker]`:

```
[sinker]
output_full_row=true
```

After enabling, all diff.log will append `src_row` and `dst_row`, and miss.log will append `src_row` (currently only supports MySQL/PG/Mongo, Redis is not supported yet). Example:

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

## Output Revise SQL

If the business needs to manually repair different data, you can enable SQL output in `[sinker]`:

```
[sinker]
output_revise_sql=true
# Optional: force WHERE clause to match the whole row
revise_match_full_row=true
```

After enabling, `INSERT` statements for missing records and `UPDATE` statements for differing records will be written to `sql.log`. When `revise_match_full_row=true`, even if the table has a primary key, it will use the entire row data to generate the WHERE condition, so as to locate the target data through the full row value. If the route is not renamed, `target_schema`/`target_tb` will not be output, and these two fields are only needed to determine the table where the SQL should be executed when renaming.

The generated SQL is essentially the SQL that the sinker needs to execute to correct the target data to be consistent with the source; it directly uses the real destination schema/table, so it can be executed directly at the target (refer to `target_schema`/`target_tb` to determine the final target object when routing renames).

Example:

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

`sql.log` example:

```sql
UPDATE `target_db`.`target_tb` SET `f_1`='2' WHERE `f_0` = 4;
```

Missing record log example:

```json
{
  "schema": "test_db_1",
  "tb": "test_table",
  "id_col_values": {"id": "3"}
}
```

`sql.log` example:

```sql
INSERT INTO `test_db_1`.`test_table`(`id`,`name`,`age`,`email`) VALUES(3,'Charlie',35,'charlie@example.com');
```

### Summary Log (summary.log)
The summary log contains the overall results of the check, such as start_time, end_time, is_consistent, and the number of miss, diff, extra.

```json
{"start_time": "2023-09-01T12:00:00+08:00", "end_time": "2023-09-01T12:00:01+08:00", "is_consistent": false, "miss_count": 1, "diff_count": 2, "extra_count": 1, "sql_count": 3}
```


# Reverse Check

Swap the [extractor] and [sinker] configurations to perform reverse check.

# Other Configurations

- Support [router], please refer to [config details](../config.md) for details.
- Refer to task_config.ini of each type of integration test:
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
