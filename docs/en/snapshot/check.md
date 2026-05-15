# Data Check

After data migration, you may want to compare the source and target data row by row and column by column. If the data volume is too large, you can perform a sampled check. Please ensure that the tables to be checked have primary keys/unique keys.

Supports comparison for MySQL, PostgreSQL, and MongoDB.

Snapshot and inline CDC checks support sampling via `[checker].sample_rate`.
For standalone MySQL/PostgreSQL/MongoDB snapshot check, `sample_rate` is applied during extraction
before rows enter later checker work. It is position-based inside each extractor stream rather than
key-hash based: MySQL/PostgreSQL parallel extraction counts positions independently inside each
chunk, nullable order-column rows are counted in their own NULL pass, and MongoDB counts positions
inside the collection cursor. For example, `sample_rate=25` keeps positions 1-25 in every
100-position window of each such stream. Inline snapshot check and inline CDC check write all
rows/changes first, then apply deterministic checker-side key-hash sampling before target fetch.
Rows/changes with the same key are sampled consistently.

Data check is documented in three flows:

## Check Flows

### Standalone snapshot check

- Use `extract_type=snapshot`.
- Set `sink_type=dummy` or omit `[sinker]`.
- Configure the checker target explicitly in `[checker]`, and set `[checker].enable=true`.
- Standalone snapshot checker targets support MySQL, PostgreSQL, and MongoDB.
- Use `parallel_type=rdb_merge` for MySQL/PostgreSQL checker targets, and `parallel_type=mongo`
  for MongoDB checker targets.
- This flow is data-only. It does not run structure check automatically; run standalone structure
  check explicitly when structure verification is required.

```text
source rows
    |
    v
[extractor snapshot]
    |
    v
[checker] ---- query target ----> [checker target]
    |
    +---- consistent -------> next batch
    |
    `---- inconsistent -----> retry / miss.log / diff.log
```

### Inline snapshot check

- Use `extract_type=snapshot` and `[sinker] sink_type=write`.
- The checker runs after sink and reuses the parsed `[sinker]` target directly.
- `[checker]` must not set `db_type`, `url`, `username`, or `password`.
- Currently supported only when `[sinker].db_type` is `mysql`, `pg`, or `mongo`.
- Keep the snapshot parallelizer (typically `parallel_type=snapshot`).

```text
source rows
    |
    v
[extractor snapshot]
    |
    v
[sinker write batch] -----> [target]
    |
    v
[checker same batch, same target]
    |
    +---- consistent -------> next batch
    |
    `---- inconsistent -----> retry -----> exhausted? -----> miss.log / diff.log
```

- This is “write-after-check + short convergence waiting”.
- It retries first and only writes miss/diff after the retry budget is exhausted.
- It does **not** maintain a long-lived inconsistency store.

### Inline cdc check

- Use `extract_type=cdc` and `[sinker] sink_type=write`.
- The checker validates applied CDC changes after they are written to the target.
- The checker reuses the parsed `[sinker]` target directly.
- Set `[checker].enable=true`.
- `[checker]` must not set `db_type`, `url`, `username`, or `password`.
- `[resumer] resume_type=from_target` or `from_db` is required to persist checker state.
- Currently supported only when `[sinker].db_type` is `mysql` or `pg`.
- Use `parallel_type=rdb_merge`.

```text
source CDC events
    |
    v
[extractor cdc]
    |
    v
[sinker write event batch] --> [target]
    |
    v
[checker same batch, same target]
    |
    +---- consistent --------> next batch / checkpoint
    |
    `---- inconsistent ------> checker state/store
                                   |
                                   +--> later events may reconcile old miss/diff
                                   `--> persisted with resumer / checkpoint state
```

- This is closer to “continuous reconciliation”.
- Inconsistencies enter checker state/store instead of being handled only by a short retry loop.
- Later CDC events may naturally cancel or reconcile older miss/diff records.

#### Inline cdc check configuration constraints

See [config.md](../config.md) for the complete inline CDC checker constraints, queue behavior,
retry rules, and S3 upload rules.

## Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md). The
templates now separate standalone snapshot check, inline snapshot check, and inline cdc check.

### Sampling Check

For snapshot and inline CDC checks, add `sample_rate` to the `[checker]` section. For standalone
MySQL/PostgreSQL/MongoDB snapshot check, sampling is applied during extraction and uses row position
within the current extractor stream. In parallel MySQL/PostgreSQL extraction, each chunk has its own
position counter; rows extracted by the nullable order-column pass also have their own counter.
Therefore `sample_rate=25` keeps positions 1-25 in each 100-position window for each stream, and the
final table-level ratio can vary, especially for small chunks. For inline snapshot check and inline
CDC check, `sample_rate=25` still writes all rows/changes, then checks rows/changes whose key hash
falls into the sampled percentage before target fetch/compare.

When standalone snapshot check resumes from a checkpoint, position counters restart from the resumed
extractor stream. If the previous run stopped after a sampled row but before the next sampled row, the
resumed run may re-window the following rows, so the sampled row set may differ from a single
uninterrupted run. Inline snapshot check and inline CDC check use key-hash sampling and keep the same
key sampling decision across resumes.

```
[checker]
enable=true
sample_rate=25
```

## Limitations

- Data check is source-driven (validates Source ∈ Target) and cannot detect extra rows that exist
  only in the target. To catch such cases, consider setting up a
  [Reverse Check](#reverse-check) by swapping source/target roles.
- For MongoDB, `_id` should be a hashable type (for example ObjectId/String/Int32/Int64). Rows whose `_id` cannot be hashed are skipped and counted in `summary.log.skip_count`; if a fetched target row has an unhashable `_id`, the check fails.

## DELETE Event Check (inline cdc check)

In inline cdc check, the checker validates DELETE events: it queries the target by primary key,
and if the row still exists in the target, it is reported as an identity-only inconsistency in
`diff.log` without `diff_col_values`. When `output_revise_sql=true`, a corresponding `DELETE`
repair statement is automatically generated in `sql.log`.

# Check Results

`diff.log` and `miss.log` are JSON Lines files and are generated only when there are diff or miss
entries; each non-empty line is one JSON object. `summary.log` contains exactly one JSON line for the
overall summary. `sql.log` is a plain SQL file, one generated repair statement per line, and contains
no JSON wrapper or schema/table/id metadata. `sql.log` is generated or written only when
`output_revise_sql=true` and repair SQL exists. By default, these logs are stored in
`runtime.log_dir/check`; if `[checker].check_log_dir` is set, that directory is used instead.

By default, stdout follows the normal runtime logging configuration. For orchestration paths that
need stdout to contain only check results, set `[runtime].check_result_stdout_only=true` for that
specific check task. In this mode, the files above remain the check artifacts, normal runtime stdout
is silenced, and miss, diff, summary, and SQL records are emitted to stdout as one line in the form
`<logger> - <payload>`. Only `summary_logger`, `miss_logger`, `diff_logger`, and `sql_logger` are
part of this stdout result stream. The JSON payloads are the same objects as the file logs; SQL
payloads are the same plain statements as `sql.log`.

## Difference Log (diff.log)

Difference logs include source database/table (`schema`/`tb`), primary key/unique key
(`id_col_values`), and source and target values of difference columns (`diff_col_values`).
When routing changes the destination object, `target_schema`/`target_tb` are also included.
SQL is never embedded in `diff.log`; repair SQL, if enabled, is written only to `sql.log`.

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

When the source and target types are different (such as Int32 vs Int64, or None vs Short), `src_type`/`dst_type` will appear under the corresponding column, clearly marking the type inconsistency. MongoDB also applies this rule, and the difference log will output the BSON type name.

`target_schema`/`target_tb` are omitted when the destination object is not explicitly different.
If either target name differs, both fields are included.

```json
{"schema":"test_db_1","tb":"orders","target_schema":"dst_db","target_tb":"orders","id_col_values":{"id":"8"},"diff_col_values":{"status":{"src":"paid","dst":"pending"}}}
```

## Missing Log (miss.log)

Missing logs include database (schema), table (tb), and primary/unique key (id_col_values). Since
missing records do not have difference columns, `diff_col_values` will not be output. SQL is never
embedded in `miss.log`; repair SQL, if enabled, is written only to `sql.log`.

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

Route rename example:

```json
{"schema":"test_db_1","tb":"orders","target_schema":"test_db_1","target_tb":"dst_orders","id_col_values":{"id":"9"}}
```

## Output Full Row

When you need full row content for troubleshooting, enable full row logging in `[checker]`. In
standalone snapshot check, configure the checker target explicitly in `[checker]`; in inline
snapshot check and inline cdc check, the checker reuses the parsed `[sinker]` target:

```
[checker]
enable=true
output_full_row=true
```

After enabling, all `diff.log` entries append `src_row` and `dst_row`, and all `miss.log` entries append `src_row` (currently supported for MySQL, PostgreSQL, and MongoDB). Example:

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

If you need to manually repair inconsistent data, enable SQL output in `[checker]`. In standalone
snapshot check, configure the checker target explicitly in `[checker]`; in inline snapshot check
and inline cdc check, the checker reuses the parsed `[sinker]` target:

```
[checker]
enable=true
output_revise_sql=true
# Optional: force WHERE clause to match the whole row
revise_match_full_row=true
```

After enabling, `INSERT` statements for missing records, `UPDATE` statements for differing records,
and `DELETE` statements for unresolved inline CDC delete events will be written to `sql.log`.
`diff.log` and `miss.log` remain JSON Lines and do not include SQL fields.

When `revise_match_full_row=true`, the entire row data is used to generate the WHERE condition even if the table has a primary key, so that the target row is located by matching all column values.

The generated SQL uses the real destination schema/table and can be executed directly at the target. When routing renames are configured, refer to `target_schema`/`target_tb` to determine the final target object.

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

## Summary Log (summary.log)

The summary log contains the overall results of the check, such as start_time, end_time, is_consistent, and the number of miss, diff, skipped rows (`skip_count`), and generated repair SQLs (`sql_count`).

`skip_count` records rows skipped by the checker, for example when the row key cannot be hashed.

`summary.log` includes `tables` when table-level counts were recorded. Table entries store
per-table checked/miss/diff/skip counts for all tables that contributed table-level counts, so clean
tables can appear with `miss_count=0`, `diff_count=0`, and `skip_count=0`. `tables` is omitted only
when no table-level counts were recorded. Table entries omit `target_schema`/`target_tb` when the
destination object is not explicitly different. If either target name differs, both fields are
included.

```json
{"start_time":"2023-09-01T12:00:00+08:00","end_time":"2023-09-01T12:00:01+08:00","is_consistent":false,"checked_count":30,"miss_count":1,"diff_count":2,"skip_count":1,"sql_count":3,"tables":[{"schema":"test_db_1","tb":"clean_table","checked_count":20,"miss_count":0,"diff_count":0,"skip_count":0},{"schema":"test_db_1","tb":"test_table","checked_count":10,"miss_count":1,"diff_count":2,"skip_count":1}]}
```

Route rename example:

```json
{"start_time":"2023-09-01T12:00:00+08:00","end_time":"2023-09-01T12:05:00+08:00","is_consistent":false,"miss_count":1,"diff_count":2,"skip_count":0,"tables":[{"schema":"test_db_1","tb":"orders","target_schema":"dst_db","target_tb":"dst_orders","checked_count":8,"miss_count":1,"diff_count":2,"skip_count":0}]}
```

# Reverse Check

Data check is source-driven and only verifies that source rows exist in the target. To detect extra
rows in the target that do not exist in the source, run a standalone snapshot check with the
source/target roles swapped:

```
# Original: source=A, target=B
# Reverse: source=B, target=A
[extractor]
db_type=<original checker db_type>
url=<original checker url>

[checker]
enable=true
db_type=<original extractor db_type>
url=<original extractor url>
```

# Configuration

See [config.md](../config.md) for the full `[checker]` configuration list and target selection rules.

## Retry Mechanism

When `max_retries > 0`, the checker automatically retries on inconsistency:
- No logs are written during retry attempts to reduce noise
- Detailed miss/diff logs are only written on the final check
- Useful when target data synchronization is not yet complete

This retry behavior is the main fit for standalone snapshot check and inline snapshot check. It is
designed for short-term convergence waiting after write, not for long-running reconciliation state.

> **Note:** Retries are not supported in inline cdc check. CDC events arrive as a stream, and
> subsequent DELETE events may remove data that was correctly written, causing false misses in the
> retry queue. Even if `max_retries` and `retry_interval_secs` are configured, they are forcibly
> ignored (set to 0) in CDC mode, and a warning is logged.

## Router

Data check supports `[router]`. See [config.md](../config.md) for details.

## Integration Test References

Refer to `task_config.ini` of each type of integration test:
- dt-tests/tests/mysql_to_mysql/check
- dt-tests/tests/pg_to_pg/check
- dt-tests/tests/mongo_to_mongo/check
