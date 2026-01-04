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
- All database urls are configured in ./tests/.env file and referenced in task_config.ini of tests.

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

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
