# [English](README.md) | 中文

# 运行测试用例
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

- 测试用例包括：
  - task_config.ini
  - src_prepare.sql
  - dst_prepare.sql
  - src_test.sql
  - dst_test.sql
  - `*.sql` 测试文件说明：
    - 多语句必须用 `;` 结束（runner 依赖 `;` 分割语句）。
    - 避免在字符串/JSON 字面量中出现 `--`（runner 会按行截断 `-- ...` 内联注释）。

- 一个典型测试用例的步骤：
  - 1，对源库执行 src_prepare.sql。
  - 2，对目标库执行 dst_prepare.sql。
  - 3，启动 数据同步 任务的线程。
  - 4，停顿若干毫秒（start_millis，根据测试环境的性能和网络状况，你可修改测试用例的预设值），等待任务初始化。
  - 5，对源库执行 src_test.sql。
  - 6，对目标库执行 dst_test.sql（如果有）。
  - 7，停顿若干毫秒（parse_millis，同前，你可根据实际情况修改），等待数据同步完成。
  - 8，对比源和目标数据。

# 配置
- 所有数据库的 extractor url，sinker url 均配置在 ./tests/.env 文件，各测试用例的 task_config.ini 中引用。

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

# 测试环境搭建

- 本文均以 docker 搭建测试环境为例。[参考](/docs/en/tutorial/prerequisites.md)

# Postgres 环境搭建

[创建 Postgres](/docs/en/tutorial/pg_to_pg.md)

## 如要执行 [双向同步](/docs/zh/cdc/two_way.md) 相关测试
- pg_to_pg::cdc_tests::test::cycle_

- 总共需要创建 3 个 Postgres 示例，并按照 [创建 Postgres](/docs/en/tutorial/pg_to_pg.md) 为每个实例都设置 wal_level = logical。

## 如要执行 [charset 相关测试](../dt-tests/tests/pg_to_pg/snapshot/charset_euc_cn_test)
- 在源和目标分别预建数据库 postgres_euc_cn。

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

# MySQL 环境搭建
[创建 MySQL](/docs/en/tutorial/mysql_to_mysql.md)

## 如要执行 [双向同步](/docs/zh/cdc/two_way.md) 相关测试
- mysql_to_mysql::cdc_tests::test::cycle_

- 总共需要创建 3 个 MySQL 示例

# Mongo
[创建 Mongo](/docs/en/tutorial/mongo_to_mongo.md)

# Kafka
[创建 Kafka](/docs/en/tutorial/mysql_to_kafka_consumer.md)

# StarRocks
[创建 StarRocks](/docs/en/tutorial/mysql_to_starrocks.md)

创建老版本 StarRocks: 2.5.4

```
docker run -itd --name some-starrocks-2.5.4 \
-p 9031:9030 \
-p 8031:8030 \
-p 8041:8040 \
starrocks/allin1-ubuntu:2.5.4
```

# Doris
[创建 Doris](/docs/en/tutorial/mysql_to_doris.md)

# Redis
[创建 Redis](/docs/en/tutorial/redis_to_redis.md)

## 更多版本

- redis 不同版本的数据格式差距较大，我们支持 2.8 - 7.*，rebloom，rejson。
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8.22
- redislabs/rebloom:2.6.3
- redislabs/rejson:2.6.4
- mac 上无法部署 2.8，rebloom，rejson 镜像，可在 EKS(amazon)/AKS(azure)/ACK(alibaba) 上部署，参考目录：dt-tests/k8s/redis。

### 源

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

### 目标

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
