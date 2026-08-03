# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.26] - 2026-08-03

Data replication involves inherently complex and diverse workloads. Over the past several releases, we have continued to strengthen the foundation of Ape-DTS across three key areas: observability, through clearer metrics, error reporting, and runtime logs; controllability, through explicit resource configuration and the elimination of black-box behaviors that can unexpectedly amplify resource consumption; and usability, through streamlined configuration and CLI-based tooling. Alongside these foundational improvements, we have focused on making replication for four widely adopted open-source database engines—MySQL, PostgreSQL, MongoDB, and Redis—more stable and reliable across real-world workloads:

### Added

#### General and Platform

- [[471]](https://github.com/apecloud/ape-dts/pull/471) - add configurable RPS and bandwidth limits for extractors and sinkers, and memory limits for pipeline buffers
- [[460]](https://github.com/apecloud/ape-dts/pull/460) - add inline CDC consistency checking
- [[490]](https://github.com/apecloud/ape-dts/pull/490) - add sampled consistency checking for snapshot and CDC workloads
- [[500]](https://github.com/apecloud/ape-dts/pull/500) - add `dts-cli` for configuration validation and database connectivity diagnostics
- [[456]](https://github.com/apecloud/ape-dts/pull/456) - support separate username and password fields when credentials contain URL-sensitive characters
- [[544]](https://github.com/apecloud/ape-dts/pull/544) - add busy-sinker metrics for worker utilization and scheduling visibility
- [[525]](https://github.com/apecloud/ape-dts/pull/525) - add profiling and Tokio Console image builds
- [[530]](https://github.com/apecloud/ape-dts/pull/530) - add runtime tracing for task scheduling and resource usage diagnostics

#### MySQL

- [[450]](https://github.com/apecloud/ape-dts/pull/450) - support parallel snapshot extraction using automatically selected or user-defined partition columns
- [[483]](https://github.com/apecloud/ape-dts/pull/483) - support TLS/SSL connections through configuration or URL parameters; certificate verification is not yet supported

#### PostgreSQL

- [[450]](https://github.com/apecloud/ape-dts/pull/450) - support parallel snapshot extraction using integer, floating-point, string, decimal, and other partition column types
- [[475]](https://github.com/apecloud/ape-dts/pull/475) - support selected user-defined functions and types, including enums, ranges, domains, and composite types, in structure migration
- [[477]](https://github.com/apecloud/ape-dts/pull/477) - support unchanged TOAST columns in CDC updates
- [[483]](https://github.com/apecloud/ape-dts/pull/483) - support TLS/SSL connections through configuration or URL parameters; certificate verification is not yet supported

#### Redis

- [[508]](https://github.com/apecloud/ape-dts/pull/508) - add Redis Cluster extraction support
- [[511]](https://github.com/apecloud/ape-dts/pull/511) - add database routing from source databases to target databases
- [[520]](https://github.com/apecloud/ape-dts/pull/520) - add Redis Cluster support to precheck and `dts-cli`

#### MongoDB

- [[515]](https://github.com/apecloud/ape-dts/pull/515) - support MongoDB 6.0+ Change Stream DDL replication
- [[515]](https://github.com/apecloud/ape-dts/pull/515) - support structure and CDC replication for sharded collections and shard keys
- [[505]](https://github.com/apecloud/ape-dts/pull/505) - support all valid MongoDB `_id` field types
- [[520]](https://github.com/apecloud/ape-dts/pull/520) - add MongoDB sharded-cluster support to precheck and `dts-cli`

### Changed

#### General and Platform

- [[553]](https://github.com/apecloud/ape-dts/pull/553) - separate checker target connections, snapshot and CDC checks, and output settings into dedicated configuration sections
- [[439]](https://github.com/apecloud/ape-dts/pull/439) - improve checker difference logs, summary logs, and revision SQL output
- [[442]](https://github.com/apecloud/ape-dts/pull/442) - add consistency rechecking and improve review workflows
- [[488]](https://github.com/apecloud/ape-dts/pull/488) - replace snapshot multi-task fan-out with a single runtime task and internal scheduling
- [[497]](https://github.com/apecloud/ape-dts/pull/497) - add configurable snapshot chunk rebalancing based on row count or data size
- [[538]](https://github.com/apecloud/ape-dts/pull/538) - optimize sinker batch query construction and parameter binding
- [[486]](https://github.com/apecloud/ape-dts/pull/486) - expand integration and regression testing for supported databases and checker workflows

#### Redis

- [[519]](https://github.com/apecloud/ape-dts/pull/519) - automatically detect Redis Cluster mode instead of requiring explicit configuration

### Breaking Changes

- [[527]](https://github.com/apecloud/ape-dts/pull/527) - remove the HTTP client/consumer and unmaintained Foxlake integrations to clarify product scope

### Fixed

#### General and Platform

- [[459]](https://github.com/apecloud/ape-dts/pull/459) - fix failures when concurrent queues reach capacity
- [[496]](https://github.com/apecloud/ape-dts/pull/496) - fix a potential deadlock in task monitoring
- [[521]](https://github.com/apecloud/ape-dts/pull/521) - avoid duplicate pipeline capacity limiters
- [[464]](https://github.com/apecloud/ape-dts/pull/464) - fix DDL parsing when database or table names contain reserved keywords
- [[509]](https://github.com/apecloud/ape-dts/pull/509) - release metadata caches after table-level snapshot tasks and fix related routing issues
- [[503]](https://github.com/apecloud/ape-dts/pull/503) - fix RDB merge behavior when an update changes a unique key
- [[530]](https://github.com/apecloud/ape-dts/pull/530) - reduce high-frequency Tokio runtime polling and unnecessary wakeups
- [[492]](https://github.com/apecloud/ape-dts/pull/492) - fix validation failures caused by the default dummy-sinker configuration

#### MySQL

- [[489]](https://github.com/apecloud/ape-dts/pull/489) - fix large-packet transfers being restricted by the client's default packet limit
- [[502]](https://github.com/apecloud/ape-dts/pull/502) - fix compatibility with binary collations such as `utf8_bin`
- [[469]](https://github.com/apecloud/ape-dts/pull/469) - fix snapshot compatibility for certain time-related data types

#### PostgreSQL

- [[473]](https://github.com/apecloud/ape-dts/pull/473) - fix writes for custom types outside the search path or with duplicate names across schemas
- [[472]](https://github.com/apecloud/ape-dts/pull/472) - fix structure migration when comments contain single quotes or other special characters
- [[474]](https://github.com/apecloud/ape-dts/pull/474) - fix unique-index detection in metadata
- [[476]](https://github.com/apecloud/ape-dts/pull/476) - fix CDC pullback and concurrent merge behavior when primary or unique keys change
- [[478]](https://github.com/apecloud/ape-dts/pull/478) - fix CDC updates and deletes for tables without primary or unique keys
- [[481]](https://github.com/apecloud/ape-dts/pull/481) - fix parameter binding when an update condition contains `NULL` on a table without a primary key
- [[550]](https://github.com/apecloud/ape-dts/pull/550) - fix missing `TRUNCATE` events in CDC
- [[467]](https://github.com/apecloud/ape-dts/pull/467) - fix snapshot handling for `"char"` and other PostgreSQL data types

#### Redis

- [[468]](https://github.com/apecloud/ape-dts/pull/468) - fix PSYNC recovery positions not being restored correctly from logs
- [[522]](https://github.com/apecloud/ape-dts/pull/522) - fix handling of an empty `is_cluster` configuration value

#### MongoDB

- [[547]](https://github.com/apecloud/ape-dts/pull/547) - defer BSON parsing with `RawDocumentBuf` to handle invalid UTF-8 data and reduce extraction CPU usage

---

## [2.0.25.1] - 2026-07-14

### Fixed

- [[526]](https://github.com/apecloud/ape-dts/pull/526) - fix: avoid busy-yield in dt queue
- [[534]](https://github.com/apecloud/ape-dts/pull/534) - fix: reduce high CPU usage in idle pipelines

---

## [2.0.25] - 2025-12-11

### Added

- [[386]](https://github.com/apecloud/ape-dts/pull/386) - feat: [MySQL] add basic DCL support in CDC
- [[388]](https://github.com/apecloud/ape-dts/pull/388) - feat: [PG] add RBAC migration support in struct step
- [[391]](https://github.com/apecloud/ape-dts/pull/391) - feat: [Redis] add support for Redis 7.4 & 8.0 with restore method
- [[397]](https://github.com/apecloud/ape-dts/pull/397) - feat: add Prometheus-style metrics API for HTTP server
- [[411]](https://github.com/apecloud/ape-dts/pull/411) - feat: add raw regex string support for filters
- [[436]](https://github.com/apecloud/ape-dts/pull/436) - feat: add batch extraction for multi-PK/UK tables
- [[431]](https://github.com/apecloud/ape-dts/pull/431) - feat: [PG] && [MySQL] add resume from database support
- [[440]](https://github.com/apecloud/ape-dts/pull/440) - feat: [MySQL] add transaction isolation level setting for sinker

### Changed

- [[385]](https://github.com/apecloud/ape-dts/pull/385) - improve: [MySQL] aggregate B-tree indexes in ALTER TABLE statements
- [[393]](https://github.com/apecloud/ape-dts/pull/393) - improve: replace std crates with tokio crates in async functions
- [[389]](https://github.com/apecloud/ape-dts/pull/389) - chore: adjust project compilation configuration
- [[412]](https://github.com/apecloud/ape-dts/pull/412) - improve: add concurrency control options for struct tasks
- [[428]](https://github.com/apecloud/ape-dts/pull/428) - improve: enable connection pool sharing across multiple tasks
- [[438]](https://github.com/apecloud/ape-dts/pull/438) - improve: use yield instead of sleep

### Fixed

- [[372]](https://github.com/apecloud/ape-dts/pull/372) - fix: structure migration from MySQL to StarRocks/Doris
- [[375]](https://github.com/apecloud/ape-dts/pull/375) - fix: column case-sensitivity issue when migrating data from MySQL to Doris/StarRocks
- [[379]](https://github.com/apecloud/ape-dts/pull/379) - fix: char/varchar type length handling in MySQL table structure migration
- [[384]](https://github.com/apecloud/ape-dts/pull/384) - fix: [StarRocks] sinker connection closing issue
- [[384]](https://github.com/apecloud/ape-dts/pull/384) - fix: rdb_merger support for MySQL to StarRocks/Doris
- [[437]](https://github.com/apecloud/ape-dts/pull/437) - fix: [MySQL] binlog dump socket keepalive support
- [[413]](https://github.com/apecloud/ape-dts/pull/413) - fix: [PG] bytea handling for invalid UTF-8 characters
- [[420]](https://github.com/apecloud/ape-dts/pull/420) - fix: [MySQL] BLOB/TEXT B-tree index in table structure migration
- [[422]](https://github.com/apecloud/ape-dts/pull/422) - fix: [PG] index creation failure when using WHERE clause
- [[400]](https://github.com/apecloud/ape-dts/pull/400) - fix: [Redis] decode username and password fields in Redis client authentication
- [[403]](https://github.com/apecloud/ape-dts/pull/403) - fix: [MySQL] CURRENT_TIMESTAMP handling in table structure
- [[407]](https://github.com/apecloud/ape-dts/pull/407) - fix: [PG] OID fetch SQL handling for special characters in table names
- [[443]](https://github.com/apecloud/ape-dts/pull/443) - fix: [MySQL] parallel extractor infinite loop
