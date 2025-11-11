# Check data

After data migration, you may want to compare the source data and the target data. If there are too many records, try sampling check. Before you start, please ensure that the tables to be verified have primary/unique keys.

MySQL/PG/Mongo are currently supported for data check.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

## Sampling check

Based on full check configuration, add `sample_interval` for sampling check. The following code means that every 3 records will be sampled once.

```
[extractor]
sample_interval=3
```

## Note

While this configuration is similar to that of snapshot migration, the only differences are:

```
[sinker]
sink_type=check

[parallelizer]
parallel_type=rdb_check
```

# Results

The results are written to logs in JSON format, including diff.log and miss.log. The logs are stored in the log/check subdirectory.

## diff.log

The diff log includes the database (schema), table (tb), primary key/unique key (id_col_values), and the source and target values of the differing columns (diff_col_values).

```json
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"log_type":"Diff","schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1"}}}
```

## miss.log

The miss log includes the database (schema), table (tb), and primary key/unique key (id_col_values). Because missing rows lack differing columns, the log does not emit `diff_col_values`.

```json
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"log_type":"Miss","schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"log_type":"Miss","schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

## Output complete rows

When the business needs the full row content for troubleshooting, enable full-row logging in the `[sinker]` section:

```
[sinker]
output_full_row=true
```

When set to `true`, the checker appends `src_row` and `dst_row` to every diff log, and `src_row` to every miss log (full rows are currently available for MySQL, PostgreSQL, and MongoDB; Redis is not supported yet). Example:

```
{
  "log_type": "Diff",
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

## Output revise SQL

If you wanna to fix data manually, enable SQL generation in the `[sinker]` section:

```
[sinker]
output_revise_sql=true
# optional: force WHERE clause to match the whole row
revise_match_full_row=true
```

When `output_revise_sql` is `true`, every miss/diff log contains an extra `revise_sql` field. The checker automatically builds `INSERT` statements for missing rows and `UPDATE` statements for diffs. With `revise_match_full_row=true`, the `UPDATE` statement matches the entire target row even if a primary/unique key exists.

When routers rename schema or table names, the log emits `target_schema` and `target_tb` so you always know where the generated SQL should run (these fields point to the destination naming, while `schema`/`tb` still represent the source). They are omitted when the router leaves the names unchanged.

`revise_sql` captures the SQL the sinker should execute to reconcile the target engine with the source data captured in the diff. Because it is generated against the destination schema/table, you can run it directly on the sinker (and you can double-check the exact database/table via `target_schema`/`target_tb` when they appear).

Example:

```json
{
  "log_type": "Diff",
  "schema": "test_db_1",
  "tb": "one_pk_no_uk",
  "target_schema": "target_db",
  "target_tb": "target_tb",
  "id_col_values": {"f_0": "4"},
  "diff_col_values": {"f_1": {"src": "2", "dst": "1"}},
  "revise_sql": "UPDATE `target_db`.`target_tb` SET `f_1`='2' WHERE `f_0` = 4;"
}
```

## Output complete rows

When the business needs the full row content for troubleshooting, enable full-row logging in the `[sinker]` section:

```
[sinker]
output_full_row=true
```

When set to `true`, the checker appends `src_row` and `dst_row` to every diff log, and `src_row` to every miss log (full rows are currently available for MySQL, PostgreSQL, and MongoDB; Redis is not supported yet). Example:

```
{
  "log_type": "Diff",
  "schema": "test_db_1",
  "tb": "one_pk_multi_uk",
  "id_col_values": {
    "f_0": "5"
  },
  "diff_col_values": {
    "f_1": {
      "src": "5",
      "dst": "5000"
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

# Other configurations

- For [filter] and [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/check
    - dt-tests/tests/pg_to_pg/check
    - dt-tests/tests/mongo_to_mongo/check
