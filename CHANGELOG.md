# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- [[400]](https://github.com/apecloud/ape-dts/pull/400) - fix: decode username and password fields in Redis client authentication
- [[403]](https://github.com/apecloud/ape-dts/pull/403) - fix: [MySQL] CURRENT_TIMESTAMP handling in table structure
- [[407]](https://github.com/apecloud/ape-dts/pull/407) - fix: [PG] OID fetch SQL handling for special characters in table names
- [[443]](https://github.com/apecloud/ape-dts/pull/443) - fix: [MySQL] parallel extractor infinite loop
