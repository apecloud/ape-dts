# Data Check

After data migration, you may want to compare the source and target data row by row and column by column. If the data volume is too large, you can perform a sampled check. Please ensure that the tables to be checked have primary keys/unique keys.

Supports comparison for MySQL, PostgreSQL, and MongoDB.

Snapshot and inline CDC checks support deterministic PK-hash checker-side sampling via `[checker].sample_rate`.
MySQL/PostgreSQL snapshot checks also support extractor-side sampling via
`[extractor].sample_interval`.

Data check is documented in three flows:

## Check Flows

### Standalone snapshot check

- Use `extract_type=snapshot`.
- Set `sink_type=dummy` or omit `[sinker]`.
- Configure the checker target explicitly in `[checker]`, and set `[checker].enable=true`.
- Use the merge-style parallelizer for the target type: `parallel_type=rdb_merge` for MySQL/PG,
  `parallel_type=mongo` for MongoDB.

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

These combinations fail fast with `ConfigError`:

- `[checker]` section is present but `enable` is missing.
- `[pipeline] pipeline_type` is not `basic`.
- `[extractor] extract_type=cdc` but `[sinker] sink_type` is not `write`.
- `[parallelizer] parallel_type` is not `rdb_merge`.
- `[sinker].db_type` is not `mysql` or `pg`.
- Any of `db_type`, `url`, `username`, or `password` is set in `[checker]`.
- `[resumer] resume_type` is missing or not `from_target` / `from_db`, so checker state cannot be persisted.

These settings remain effective or are forced in inline cdc check:

- `[checker].batch_size`: stays effective and does not fall back to `[sinker].batch_size`.
- `[checker].queue_size`: counts pending checker DML batches. If the queue is full, the oldest
  pending checker batch is dropped instead of blocking the write path. Control signals such as
  checkpoint and `refresh_meta` bypass this queue.
- `[checker].max_retries` and `[checker].retry_interval_secs`: always forced to `0`.

## Inline Snapshot Check vs Inline CDC Check

| Aspect | Inline snapshot check | Inline cdc check |
| :--- | :--- | :--- |
| Check timing | Write one batch, then check that batch | Write one event batch, then check that batch |
| First inconsistency handling | Retry first | Record into persistent inconsistency state/store |
| When miss/diff is logged | Only after retry budget is exhausted | May be logged from the current reconciliation state |
| Long-lived inconsistency tracking | No long-lived inconsistency store | Yes; checker state is coupled with checkpoint / state store |
| How later writes affect old inconsistencies | Retries are short-term waiting only | Later CDC events may cancel or reconcile older miss/diff records |
| Mental model | Write-after-check with short convergence waiting | Continuous reconciliation |

Operationally:

- **Inline snapshot check** is optimized for “write first, then wait briefly for convergence”.
  After a sink batch is written, the checker validates the same batch. If target visibility is
  slightly delayed, it retries first. Only after the retry budget is exhausted are `miss.log` and
  `diff.log` written. It does **not** maintain a long-lived inconsistency store.
- **Inline cdc check** is optimized for long-running reconciliation. After an event batch is
  applied, the checker validates the resulting target state. If an inconsistency is observed, it
  becomes part of persistent checker state/store instead of being treated as a short retry-only
  problem. Later CDC events may naturally offset or reconcile earlier miss/diff entries, so
  checkpoint/state persistence is more deeply coupled with the checker lifecycle.
  Runtime errors are handled per operation: the checker logs the error, keeps the main write path
  running, and continues processing subsequent checker messages. Checkpoint/meta-refresh delivery is
  also decoupled from checker batch backlog so the write path does not wait behind queued DML.

## Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md). The
templates now separate standalone snapshot check, inline snapshot check, and inline cdc check.

### Sampling Check

For snapshot and inline CDC checks, add `sample_rate` to the `[checker]` section. For example,
setting `sample_rate=25` checks rows/changes whose PK hash bucket falls in `[0, 25)`.
Candidate rows/changes still enter the checker queue. Before target fetch, the checker computes the
bucket as `row_key % 100` and fetches/compares only valid-key rows whose bucket is sampled in.

```
[checker]
enable=true
sample_rate=25
```

For MySQL/PostgreSQL snapshot check, you can also add `sample_interval` to the
`[extractor]` section. For example, setting `sample_interval=3` checks every 3rd extracted record.
If both sampling settings are configured, only rows passing both extractor-side sampling and
checker-side PK-hash sampling are checked.

```
[extractor]
sample_interval=3
```

## Limitations

- Data check is source-driven (validates Source ∈ Target) and cannot detect extra rows that exist
  only in the target. To catch such cases, consider setting up a
  [Reverse Check](#reverse-check) by swapping source/target roles.
- For MongoDB, `_id` should be a hashable type (for example ObjectId/String/Int32/Int64). Rows whose `_id` cannot be hashed are skipped and counted in `summary.log.skip_count`; if a fetched target row has an unhashable `_id`, the check fails.

## DELETE Event Check (inline cdc check)

In inline cdc check, the checker validates DELETE events: it queries the target by primary key,
and if the row still exists in the target, it is reported as an inconsistency in `diff.log` (with
empty `diff_col_values`). When `output_revise_sql=true`, a corresponding `DELETE` repair statement
is automatically generated in `sql.log`.

# Check Results

`diff.log`, `miss.log`, and `summary.log` are written in JSON format. `sql.log` contains generated repair SQL. By default, these logs are stored in `runtime.log_dir/check`; if `[checker].check_log_dir` is set, that directory is used instead.

## Difference Log (diff.log)

Difference logs include database (schema), table (tb), primary key/unique key (id_col_values), source and target values of difference columns (diff_col_values).

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

When the source and target types are different (such as Int32 vs Int64, or None vs Short), `src_type`/`dst_type` will appear under the corresponding column, clearly marking the type inconsistency. MongoDB also applies this rule, and the difference log will output the BSON type name.

Only when the router renames the schema or table will the log include `target_schema`/`target_tb` to identify the real destination table. `schema` and `tb` still represent the source, facilitating troubleshooting.

## Missing Log (miss.log)

Missing logs include database (schema), table (tb), and primary/unique key (id_col_values). Since missing records do not have difference columns, `diff_col_values` will not be output.

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
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

After enabling, `INSERT` statements for missing records and `UPDATE` statements for differing records will be written to `sql.log`.

When `revise_match_full_row=true`, the entire row data is used to generate the WHERE condition even if the table has a primary key, so that the target row is located by matching all column values.

If the router does not rename the schema or table, `target_schema`/`target_tb` will not appear in the log. These two fields are only needed to determine the destination table when routing renames are configured.

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

`skip_count` records rows skipped by the checker, for example when the row key cannot be hashed. When no rows are skipped, this field is omitted.

In inline cdc check, `summary.log` also includes `tables`, which stores per-table miss/diff
counts. This field is omitted for non-CDC tasks.

```json
{"start_time": "2023-09-01T12:00:00+08:00", "end_time": "2023-09-01T12:00:01+08:00", "is_consistent": false, "miss_count": 1, "diff_count": 2, "skip_count": 1, "sql_count": 3}
```

Inline cdc check example:

```json
{"start_time":"2023-09-01T12:00:00+08:00","end_time":"2023-09-01T12:05:00+08:00","is_consistent":false,"miss_count":1,"diff_count":2,"skip_count":1,"tables":{"test_db_1.test_tb":{"miss_count":1,"diff_count":2}}}
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
