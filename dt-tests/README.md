# English | [中文](README_ZH.md)

# Run tests
```rust
#[tokio::test]
#[serial]
async fn cdc_basic_test() {
    TestBase::run_cdc_test("mysql_to_mysql/cdc/basic_test", 3000, 2000).await;
}
```

```
cargo test --package dt-tests --test integration_test -- mysql_to_mysql::cdc_tests::test::cdc_basic_test --nocapture 
```

- A test contains: 
  - task_config.ini
  - src_prepare.sql
  - dst_prepare.sql
  - src_test.sql
  - dst_test.sql
  - Notes for `*.sql` test files:
    - Multi-statement SQL must be terminated with `;` (the runner splits statements by `;`).
    - Avoid `--` inside string/JSON literals (the runner strips inline `-- ...`).

- Steps for running a test: 
  - 1, execute src_prepare.sql in source database.
  - 2, execute dst_prepare.sql in target database.
  - 3, start data sync task.
  - 4, sleep some milliseconds for task initialization (start_millis, you may change it based on source/target performance).
  - 5, execute src_test.sql in source database.
  - 6, execute dst_test.sql (if exists) in target database.
  - 7, sleep some milliseconds for data sync (parse_millis, change it if needed).
  - 8, compare data of source and target.

# Config
- The full local test matrix is configured in `./tests/.env`.
- `./scripts/run-integration-tests.sh` uses `./tests/.env` plus `./docker-compose.integration.yml` by default.
- `./docker-compose.integration.yml` keeps fixed ports and aligns with the existing local env conventions.
- Local overrides can stay in `./tests/.env.local`.
- task_config.ini files reference these env keys directly.

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

## Local integration runner

```bash
# List available suites
./scripts/run-integration-tests.sh --list-suites

# Run one suite end-to-end
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all

# Start containers and keep them running for later steps
./scripts/run-integration-tests.sh --suite mysql_to_mysql --up --wait --keep-docker

# Check whether each suite's containers can start successfully
./scripts/run-integration-tests.sh --suite all --up --wait --down-each-suite --keep-going

# Run tests against already-started containers
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --runner nextest --keep-docker

# Show stdout/stderr for successful tests too
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --show-test-output

# Run one exact test case
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test -- --exact snapshot_tests::test::snapshot_basic_test

# Dump docker logs for the current suite
./scripts/run-integration-tests.sh --suite mysql_to_mysql --logs --keep-docker

# Stop all integration containers
./scripts/run-integration-tests.sh --suite mysql_to_mysql --down

# Dump docker logs automatically when a test step fails
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --logs-on-failure

# Write logs to a custom directory
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --log-dir /tmp/dt-it-logs

# Run multiple suites and continue after failures
./scripts/run-integration-tests.sh --suite mysql_to_mysql --suite pg_to_pg --keep-going --all
```

- The suite matrix lives at the top of `./scripts/run-integration-tests.sh`.
- Step flags map directly to CI-style phases: `--up`, `--wait`, `--test`, `--logs`, `--down`.
- The script requires `cargo nextest`.
- Test cases inside a suite are run serially.
- The script enables `RUST_BACKTRACE=1` and `RUST_LIB_BACKTRACE=1` by default unless you override them.
- Script flow logs are written per suite to `../tmp/integration-logs/<timestamp-pid>/<suite>.log`.
- Test runner output is written separately to `../tmp/integration-logs/<timestamp-pid>/<suite>/tests.log`.
- The script cleans up `docker compose` services on exit by default; use `--keep-docker` to skip that cleanup.
- Use `--down-each-suite` when you want to run many suites in one command but avoid service/port conflicts between suites.

# Init test env

- Examples work in docker. [prerequisites](/docs/en/tutorial/prerequisites.md)

# Postgres
[Prepare Postgres instances](/docs/en/tutorial/pg_to_pg.md)

## To run [Two-way data sync](/docs/en/cdc/two_way.md) tests
- pg_to_pg::cdc_tests::test::cycle_

- You need to create 3 Postgres instances, and set wal_level = logical for each one.


## To run [charset tests](../dt-tests/tests/pg_to_pg/snapshot/charset_euc_cn_test)
- Create database "postgres_euc_cn" in both source and target.

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

# MySQL
[Prepare MySQL instances](/docs/en/tutorial/mysql_to_mysql.md)

## To run [Two-way data sync](/docs/en/cdc/two_way.md) tests
- mysql_to_mysql::cdc_tests::test::cycle_

- You need to create 3 Postgres instances

# Mongo
[Prepare Mongo instances](/docs/en/tutorial/mongo_to_mongo.md)

# Kafka
[Prepare Kafka instances](/docs/en/tutorial/mysql_to_kafka_consumer.md)

# StarRocks
[Prepare StarRocks instances](/docs/en/tutorial/mysql_to_starrocks.md)

For old version: 2.5.4

```
docker run -itd --name some-starrocks-2.5.4 \
-p 9031:9030 \
-p 8031:8030 \
-p 8041:8040 \
starrocks/allin1-ubuntu:2.5.4
```

# Doris
[Prepare Doris instances](/docs/en/tutorial/mysql_to_doris.md)

# Redis
[Prepare Redis instances](/docs/en/tutorial/redis_to_redis.md)

## More versions
- Data format varies in different redis versions, we support 2.8 - 7.*, rebloom, rejson.
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8.22
- redislabs/rebloom:2.6.3
- redislabs/rejson:2.6.4
- Can not deploy 2.8,rebloom,rejson on mac, you may deploy them in EKS(amazon)/AKS(azure)/ACK(alibaba), refer to: dt-tests/k8s/redis.

### Source

```
docker run --name src-redis-7-0 \
    -p 6380:6379 \
    -d redis:7.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-6-2 \
    -p 6381:6379 \
    -d redis:6.2 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-6-0 \
    -p 6382:6379 \
    -d redis:6.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-5-0 \
    -p 6383:6379 \
    -d redis:5.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name src-redis-4-0 \
    -p 6384:6379 \
    -d redis:4.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```

### Target

```
docker run --name dst-redis-7-0 \
    -p 6390:6379 \
    -d redis:7.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-6-2 \
    -p 6391:6379 \
    -d redis:6.2 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-6-0 \
    -p 6392:6379 \
    -d redis:6.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-5-0 \
    -p 6393:6379 \
    -d redis:5.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning

docker run --name dst-redis-4-0 \
    -p 6394:6379 \
    -d redis:4.0 redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```
