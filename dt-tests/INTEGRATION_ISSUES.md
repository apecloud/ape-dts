# Integration Issues

This document tracks issues observed while running the local integration matrix with:

```bash
bash dt-tests/scripts/run-integration-tests.sh --suite all --all --down-each-suite --keep-going
```

Current run log directory:

```text
tmp/integration-logs/20260501-023820-82815
```

## Environment / Orchestration Issues

### `mysql_to_doris`

- Status: failing consistently
- Symptoms:
  - target-side connection aborted on first test startup
  - later cases fail with `pool timed out while waiting for an open connection`
- Evidence:
  - `mysql_to_doris/tests.log`
  - `mysql_to_doris/docker.log`
- Current assessment:
  - Doris is treated as ready too early
  - current wait logic only checks `running` for Doris, not SQL readiness
- Likely fix direction:
  - add a real Doris healthcheck or service-specific readiness probe
  - wait for MySQL-compatible port/login readiness before test start

### `pg_to_doris`

- Status: failing consistently
- Symptoms:
  - first CDC case waits a long time, then fails with:
    - `pool timed out while waiting for an open connection`
- Evidence:
  - `pg_to_doris/tests.log`
  - `pg_to_doris/docker.log`
- Current assessment:
  - same environment/readiness bug family as `mysql_to_doris`
  - runner marks Doris as `running`, but target SQL service is still not actually ready
- Likely fix direction:
  - solve Doris readiness at compose/runner layer once for all Doris suites
  - require successful MySQL-protocol connect/login before starting tests

### `mysql_to_kafka_to_mysql`

- Status: failing consistently
- Symptoms:
  - Kafka admin topic creation times out
  - error: `KafkaError (Admin operation error: OperationTimedOut (Local: Timed out))`
- Evidence:
  - `mysql_to_kafka_to_mysql/tests.log`
  - `mysql_to_kafka_to_mysql/docker.log`
- Current assessment:
  - Kafka container is only checked as `running`
  - broker is not actually ready for admin operations when tests start
- Likely fix direction:
  - add Kafka healthcheck / readiness logic
  - wait for broker metadata/admin readiness before test start

### `pg_to_kafka_to_pg`

- Status: failing consistently
- Symptoms:
  - CDC case runs a long time, then fails while creating Kafka topic
  - stack points to `RdbKafkaRdbTestRunner::create_topic`
- Evidence:
  - `pg_to_kafka_to_pg/tests.log`
  - `pg_to_kafka_to_pg/docker.log`
- Current assessment:
  - same environment/readiness bug family as `mysql_to_kafka_to_mysql`
  - Kafka is only treated as `running`, but admin/topic operations are not ready yet
- Likely fix direction:
  - add broker admin-readiness probing in the integration runner
  - wait for metadata/topic-create operations to succeed before launching tests

### `mysql_to_redis`

- Status: failing consistently
- Symptoms:
  - both CDC and snapshot tests fail almost immediately
  - target-side connection creation aborts before test logic proceeds:
    - `error communicating with database: connection aborted`
- Evidence:
  - `mysql_to_redis/tests.log`
  - `mysql_to_redis/docker.log`
- Current assessment:
  - likely environment/readiness or Redis target configuration issue
  - failure happens too early to look like business logic mismatch
- Likely fix direction:
  - inspect Redis sink connection settings and container startup mode
  - add or tighten Redis readiness probe before test start

### `mysql_to_tidb`

- Status: failing consistently
- Symptoms:
  - tests fail immediately during target connection pool creation
  - error: `error communicating with database: connection aborted`
- Evidence:
  - `mysql_to_tidb/tests.log`
  - `mysql_to_tidb/docker.log`
- Current assessment:
  - likely environment/readiness issue
  - current runner only marks TiDB as `running`, not SQL-ready
  - failure occurs before test logic starts, inside MySQL-compatible connection pool creation
- Likely fix direction:
  - add real TiDB readiness check, not just container running state
  - wait for port/login readiness on `4000` before launching tests

### `redis_to_redis_precheck`

- Status: failing consistently
- Symptoms:
  - suite starts with no external services
  - precheck test fails immediately on Redis connection:
    - `can not connect redis: [redis://:123456@127.0.0.1:6385]`
    - `Connection refused (os error 61)`
- Evidence:
  - `redis_to_redis_precheck/tests.log`
  - `redis_to_redis_precheck.log`
- Current assessment:
  - environment/orchestration issue
  - suite depends on a local Redis target on `6385`, but current integration matrix does not start any Redis service for this suite
- Likely fix direction:
  - bind `redis_to_redis_precheck` to the required Redis service set in the integration runner
  - or make the suite use a self-contained config that matches the no-service expectation

### `redis_to_redis_redisearch`

- Status: failing consistently
- Symptoms:
  - container starts successfully and healthcheck passes
  - `nextest` then reports:
    - `Starting 0 tests across 1 binary`
    - `error: no tests to run`
- Evidence:
  - `redis_to_redis_redisearch/tests.log`
  - `redis_to_redis_redisearch.log`
- Current assessment:
  - not a container readiness issue
  - suite-to-test filter mapping is wrong or the corresponding test module is currently absent/disabled
- Likely fix direction:
  - inspect integration runner filter mapping for `redis_to_redis_redisearch`
  - align suite name with the actual Rust test module/function names, or drop the suite from the active matrix until tests exist

## Code / Behavior Issues

### `mysql_to_mysql::cdc_ddl_meta_center_test`

- Status: failing
- Symptoms:
  - integration test fails after DDL-heavy setup and execution
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - not an environment startup problem
  - appears to be task logic or DDL/meta-center behavior regression
- Likely fix direction:
  - inspect failure stack and compare expected meta-center behavior with current implementation

### `mysql_to_mysql::cycle_star_test`

- Status: failing
- Symptoms:
  - cycle replication test fails during final data comparison
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - not a service readiness problem
  - likely cycle replication behavior or comparison logic bug
- Likely fix direction:
  - inspect `RdbCycleTestRunner` and comparison path
  - compare actual source/destination data snapshots from the run

### `mysql_to_mysql_lua::cdc_basic_test`

- Status: failing
- Symptoms:
  - final row comparison fails for transformed string columns
  - example mismatch:
    - expected `a中文😀`
    - actual `61e4b8ade69687f09f9880`
- Evidence:
  - `mysql_to_mysql_lua/tests.log`
- Current assessment:
  - not a startup issue
  - transformed target value looks like hex-encoded bytes rather than decoded text
  - likely Lua transform / sink encoding / comparison normalization bug on string path
- Likely fix direction:
  - inspect Lua path for string/blob type handling
  - verify whether non-ASCII strings are being routed through binary encoding unexpectedly

### `mysql_to_mysql_lua::cdc_do_nothing_test`

- Status: failing
- Symptoms:
  - fails in the same final data comparison path as `cdc_basic_test`
- Evidence:
  - `mysql_to_mysql_lua/tests.log`
- Current assessment:
  - likely same Lua/string handling root cause family
- Likely fix direction:
  - fix shared Lua value conversion path first, then rerun both cases

### `mysql_to_mysql::snapshot_parallel_test`

- Status: failing
- Symptoms:
  - snapshot setup panics before task body finishes
  - source-side SQL replay hits MySQL duplicate-key error:
    - `Duplicate entry ' ' for key 'tb_varchar_pk.PRIMARY'`
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - this is not a container readiness issue
  - likely test fixture / prepare SQL inserts duplicate blank-string PK values into `tb_varchar_pk`
  - failure appears in `RdbUtil::execute_sqls_mysql` during setup, before snapshot verification proper
- Likely fix direction:
  - inspect `mysql_to_mysql` snapshot parallel fixture data for `tb_varchar_pk`
  - verify whether duplicated source prepare SQL is intentional and should be deduplicated or whether PK definition / data generation is wrong

### `mysql_to_mysql::check_basic_struct_test`

- Status: failing
- Symptoms:
  - structure checker logs `Structure check passed - all structures are consistent`
  - test still panics in `check_util.rs` with `assertion left == right failed`, `left: 2`, `right: 0`
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - checker result accounting and test expectation are inconsistent
  - likely not a startup/environment issue
- Likely fix direction:
  - inspect struct-check result counting in checker/test utility
  - verify whether mismatches are being reported but not reflected in summary, or the assertion is stale

### `mysql_to_mysql::check_revise_struct_test`

- Status: failing
- Symptoms:
  - fails in the same struct-check / revise-check path as `check_basic_struct_test`
  - assertion mismatch surfaced through `check_util.rs`
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - likely same root cause family as struct check result counting / expectation handling
- Likely fix direction:
  - inspect revise-struct check output versus asserted mismatch count
  - debug `check_util.rs` expectations shared by struct-related check tests

### `mysql_to_mysql_case_sensitive::check_basic_struct_test`

- Status: failing
- Symptoms:
  - suite summary reports consistency success first
  - test still panics in `check_util.rs` with `left: 2`, `right: 0`
- Evidence:
  - `mysql_to_mysql_case_sensitive/tests.log`
- Current assessment:
  - same result-accounting / expectation bug family as `mysql_to_mysql::check_basic_struct_test`
- Likely fix direction:
  - fix shared check result counting path first, then rerun both normal and case-sensitive suites

### `mysql_to_mysql::struct_basic_test`

- Status: failing
- Symptoms:
  - struct comparison panics with explicit mismatch in DDL line-by-line comparison
  - observed mismatches include MySQL 8.0 specific normalized output such as:
    - `DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`
    - column collations like `utf8mb4_0900_ai_ci`
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - not a startup/environment issue
  - struct expectation matching is too strict for current MySQL 8.0 emitted DDL
  - same root-cause family likely affects other struct tests below
- Likely fix direction:
  - normalize charset/collation / trailing table options before comparison
  - or update expected DDL comparison to ignore non-semantic MySQL 8.0 formatting differences

### `mysql_to_mysql::struct_8_0_basic_test`

- Status: failing
- Symptoms:
  - DDL comparison mismatch on column definition:
    - `` `c` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT '' ``
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - same DDL normalization / comparison bug family as `struct_basic_test`
- Likely fix direction:
  - unify MySQL 8.0 collation-aware normalization in struct comparison path

### `mysql_to_mysql::struct_filter_test_2`

- Status: failing
- Symptoms:
  - DDL comparison mismatch includes trailing table option line:
    - `) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - same DDL normalization / comparison bug family as `struct_basic_test`
- Likely fix direction:
  - ignore or normalize engine/charset/collation table options before strict compare

### `mysql_to_mysql::struct_route_test`

- Status: failing
- Symptoms:
  - DDL comparison mismatch on text column collation line:
    - `` `longtext_col` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT 'longtext_col_comment' ``
- Evidence:
  - `mysql_to_mysql/tests.log`
- Current assessment:
  - same DDL normalization / comparison bug family as `struct_basic_test`
- Likely fix direction:
  - make struct DDL comparison robust to MySQL version-specific collation rendering

### `mysql_to_mysql_case_sensitive::struct_basic_test`

- Status: failing
- Symptoms:
  - DDL comparison mismatch on case-sensitive suite still trips over MySQL 8.0 collation-rendered lines such as:
    - `` `text_col` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT 'text_col_comment' ``
- Evidence:
  - `mysql_to_mysql_case_sensitive/tests.log`
- Current assessment:
  - same struct DDL normalization/comparison bug family as `mysql_to_mysql::struct_basic_test`
- Likely fix direction:
  - fix shared struct comparison normalization and verify both base and case-sensitive suites together

### `pg_to_pg::cdc_to_reverse_sql_test`

- Status: failing
- Symptoms:
  - test setup SQL for reverse-SQL CDC path fails before task body proceeds
  - PostgreSQL error:
    - `column "nan" does not exist`
- Evidence:
  - `pg_to_pg/tests.log`
- Current assessment:
  - not an environment issue
  - reverse-SQL fixture inserts `NaN`, `inf`, `-inf` as bare tokens in generated SQL
  - on the current execution path they are parsed as identifiers instead of valid numeric literals/casts
- Likely fix direction:
  - normalize the reverse-SQL fixture generation for PostgreSQL special float values
  - emit quoted/cast literals that PostgreSQL accepts consistently, e.g. `'NaN'::real`, `'Infinity'::double precision`

### `pg_to_pg::cdc_to_sql_test`

- Status: failing
- Symptoms:
  - test setup SQL fails before CDC verification starts
  - PostgreSQL error:
    - `column "nan" does not exist`
- Evidence:
  - `pg_to_pg/tests.log`
- Current assessment:
  - same root-cause family as `pg_to_pg::cdc_to_reverse_sql_test`
  - the SQL fixture path also emits bare `NaN/inf/-inf` tokens for PostgreSQL numeric test data
- Likely fix direction:
  - fix the shared PostgreSQL test SQL generation for special float literals
  - rerun both `cdc_to_sql_test` and `cdc_to_reverse_sql_test` together after the change

### `pg_to_pg::check_revise_struct_test`

- Status: failing
- Symptoms:
  - structure checker logs:
    - `Structure check passed - all structures are consistent`
  - summary also reports:
    - `"is_consistent": true`
  - test still panics in `check_util.rs` with:
    - `assertion left == right failed`
    - `left: 6`
    - `right: 0`
- Evidence:
  - `pg_to_pg/tests.log`
- Current assessment:
  - not an environment issue
  - same result-accounting / expectation mismatch family as `mysql_to_mysql::check_revise_struct_test`
  - PG struct-check path likely emits mismatch details that are not reflected in the final consistency summary
- Likely fix direction:
  - inspect shared `check_util.rs` and PG struct-check result aggregation
  - reconcile reported mismatch count with `is_consistent` summary output

### `pg_to_pg::check_struct_basic_test`

- Status: failing
- Symptoms:
  - structure checker summary reports:
    - `"is_consistent": true`
  - test still panics in `check_util.rs` with:
    - `assertion left == right failed`
    - `left: 6`
    - `right: 0`
- Evidence:
  - `pg_to_pg/tests.log`
- Current assessment:
  - same root-cause family as `pg_to_pg::check_revise_struct_test`
  - shared struct-check result counting / expectation handling bug
- Likely fix direction:
  - fix shared struct-check accounting once and rerun both PG and MySQL struct-check suites

### `pg_to_pg::struct_filter_test_1`

- Status: failing
- Symptoms:
  - struct/filter test logs:
    - `Structure check passed - all structures are consistent`
  - summary also reports:
    - `"is_consistent": true`
  - test still panics in `check_util.rs` with:
    - `assertion left == right failed`
    - `left: 8`
    - `right: 0`
- Evidence:
  - `pg_to_pg/tests.log`
- Current assessment:
  - not an environment issue
  - same struct-check/result-accounting family as `pg_to_pg::check_revise_struct_test` and `pg_to_pg::check_struct_basic_test`
  - this variant likely exercises the filter path, but the underlying inconsistency is still between emitted mismatch count and final summary
- Likely fix direction:
  - fix shared PG struct-check mismatch accounting first
  - then rerun all `pg_to_pg` struct/check variants together to verify `left` counts drop to expected values

## Notes

- These entries are only for issues confirmed during the ongoing run.
- Disabled suites such as Foxlake, StarRocks, and Redis 2.8 are intentionally not part of the current matrix.
