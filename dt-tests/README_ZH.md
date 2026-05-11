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
- 完整的本地测试矩阵配置在 `./tests/.env`。
- `./scripts/run-integration-tests.sh` 默认使用 `./tests/.env` 和 `./docker-compose.integration.yml`。
- `./docker-compose.integration.yml` 使用固定端口，并与现有本地 env 约定对齐。
- 本地覆盖仍可放在 `./tests/.env.local`。
- 各测试用例的 task_config.ini 直接引用这些 env key。

```
[extractor]
url={mysql_extractor_url}

[sinker]
url={mysql_sinker_url}
```

## 本地集成测试脚本

```bash
# 列出所有 suite
./scripts/run-integration-tests.sh --list-suites

# 跑完整 suite
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all

# 启动容器并保留，供后续步骤复用
./scripts/run-integration-tests.sh --suite mysql_to_mysql --up --wait --keep-docker

# 批量检查每个 suite 的容器是否能成功启动
./scripts/run-integration-tests.sh --suite all --up --wait --down-each-suite --keep-going

# 针对已启动容器执行测试
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --runner nextest --keep-docker

# 同时输出成功用例的 stdout/stderr
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test --show-test-output

# 只跑一个精确测试用例
./scripts/run-integration-tests.sh --suite mysql_to_mysql --test -- --exact snapshot_tests::test::snapshot_basic_test

# 导出当前 suite 的 docker 日志
./scripts/run-integration-tests.sh --suite mysql_to_mysql --logs --keep-docker

# 关闭所有集成测试容器
./scripts/run-integration-tests.sh --suite mysql_to_mysql --down

# 测试失败时自动导出 docker 日志
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --logs-on-failure

# 把日志写到自定义目录
./scripts/run-integration-tests.sh --suite mysql_to_mysql --all --log-dir /tmp/dt-it-logs

# 连跑多个 suite，失败后继续
./scripts/run-integration-tests.sh --suite mysql_to_mysql --suite pg_to_pg --keep-going --all
```

- 测试矩阵直接写在 `./scripts/run-integration-tests.sh` 顶部。
- step flag 和 CI phase 一一对应：`--up`、`--wait`、`--test`、`--logs`、`--down`。
- 脚本依赖 `cargo nextest`。
- 同一个 suite 内的测试用例会串行执行。
- 脚本默认会开启 `RUST_BACKTRACE=1` 和 `RUST_LIB_BACKTRACE=1`，除非你自行覆盖。
- 脚本流程日志会按 suite 分别落到 `../tmp/integration-logs/<timestamp-pid>/<suite>.log`。
- 测试进程输出会单独落到 `../tmp/integration-logs/<timestamp-pid>/<suite>/tests.log`。
- 脚本默认会在退出时清理 `docker compose` 服务；如需保留容器，传 `--keep-docker`。
- 如果想在一条命令里批量跑多个 suite，又避免服务/端口冲突，可使用 `--down-each-suite`。

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
