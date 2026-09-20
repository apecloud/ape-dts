# Benchmark

# MySQL -> MySQL

使用 [sysbench](https://github.com/akopytov/sysbench) 生成全量和增量数据。使用 [ape_dts](/docs/en/tutorial/mysql_to_mysql.md)执行迁移任务。

## 测试环境

源库，目标库，数据迁移任务分别位于同一网络的 3 台阿里云ecs。

## MySQL 规格

|      | 源库         | 目标库       |
| ---- | ------------ | ------------ |
| 版本 | mysql:8.4.11 | mysql:8.4.11 |
| 规格 | 8c16g        | 8c16g        |

## 全量迁移

使用 sysbench oltp_read_write 生成 8 张表，每张表 400万条数据。

### 结果

| 节点规格 | ape-dts 2.0.26.1 (行/秒) | ape-dts 2.0.25 (行/秒) | ape-dts 2.0.23 (行/秒) |
| -------- | ------------------------ | ---------------------- | ---------------------- |
| 1c2g     | 109067                   | 74576                  | 71428                  |
| 2c4g     | 131783                   | 129639                 | 99403                  |
| 4c8g     | 133283                   | 132781                 | 126582                 |

资源用量（CPU / RSS MiB）及耗时：

| 节点规格 | 指标                       | ape-dts 2.0.26.1 | ape-dts 2.0.25  | ape-dts 2.0.23 |
| -------- | -------------------------- | ---------------- | --------------- | -------------- |
| 1c2g     | 工具 CPU / RSS (MiB)       | 80.58% / 600     | 96.86% / 1005   | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 7.28% / 8725     | 5.15% / 8635    | 8.2% / 未记录  |
|          | 目标 MySQL CPU / RSS (MiB) | 112.59% / 14235  | 69.43% / 12672  | 211% / 未记录  |
|          | 耗时（秒）                 | 293.40           | 429.09          | 未记录         |
| 2c4g     | 工具 CPU / RSS (MiB)       | 102.80% / 1768   | 158.88% / 1212  | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 9.18% / 8636     | 9.29% / 8772    | 14.0% / 未记录 |
|          | 目标 MySQL CPU / RSS (MiB) | 156.01% / 14291  | 149.88% / 12486 | 359% / 未记录  |
|          | 耗时（秒）                 | 242.82           | 246.84          | 未记录         |
| 4c8g     | 工具 CPU / RSS (MiB)       | 96.30% / 1998    | 183.50% / 1596  | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 9.06% / 8634     | 8.83% / 8633    | 13.8% / 未记录 |
|          | 目标 MySQL CPU / RSS (MiB) | 172.36% / 14254  | 184.19% / 13775 | 552% / 未记录  |
|          | 耗时（秒）                 | 240.09           | 241.00          | 未记录         |

注：ape-dts 2.0.23 的内存用量按源库和目标库的 8c16g 规格，以 16 GiB（16384 MiB）× 原始内存百分比估算：源库 5.2% ≈ 852 MiB，目标库 5.1% ≈ 836 MiB。这些数值为 RSS 估算值，并非实测 RSS；工具资源用量及耗时未记录。

## 增量迁移

### 测试 8表 （100万基线数据 + 400万次UPDATE）

使用 sysbench oltp_update_index：8 表 × 100 万行基线、400 万次 UPDATE。

#### 结果

| 节点规格 | ape-dts 2.0.26.1 (UPDATE/s) | ape-dts 2.0.25 (UPDATE/s) | ape-dts 2.0.23 (UPDATE/s) |
| -------- | --------------------------- | ------------------------- | ------------------------- |
| 1c2g     | 23477                       | 20365                     | 11902                     |
| 2c4g     | 51171                       | 41161                     | 14240                     |
| 4c8g     | 52538                       | 48681                     | 19450                     |

资源用量（CPU / RSS MiB）及耗时：

| 节点规格 | 指标                       | ape-dts 2.0.26.1 | ape-dts 2.0.25 | ape-dts 2.0.23 |
| -------- | -------------------------- | ---------------- | -------------- | -------------- |
| 1c2g     | 工具 CPU / RSS (MiB)       | 91.76% / 568     | 89.56% / 287   | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 2.71% / 1989     | 2.38% / 1971   | 19.0% / 未记录 |
|          | 目标 MySQL CPU / RSS (MiB) | 69.38% / 6915    | 55.21% / 5049  | 479% / 未记录  |
|          | 耗时（秒）                 | 170.38           | 196.41         | 未记录         |
| 2c4g     | 工具 CPU / RSS (MiB)       | 141.33% / 988    | 140.01% / 470  | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 5.73% / 1947     | 4.57% / 1925   | 18.6% / 未记录 |
|          | 目标 MySQL CPU / RSS (MiB) | 191.92% / 5627   | 117.99% / 5775 | 623% / 未记录  |
|          | 耗时（秒）                 | 78.17            | 97.18          | 未记录         |
| 4c8g     | 工具 CPU / RSS (MiB)       | 154.92% / 589    | 161.90% / 795  | 未记录         |
|          | 源 MySQL CPU / RSS (MiB)   | 5.63% / 2061     | 5.24% / 1890   | 19.2% / 未记录 |
|          | 目标 MySQL CPU / RSS (MiB) | 193.22% / 6058   | 131.25% / 5386 | 689% / 未记录  |
|          | 耗时（秒）                 | 76.14            | 82.17          | 未记录         |

# 2.0.26.1 1c2g 任务配置参考

## snapshot

```toml
[metrics]
http_host=127.0.0.1
http_port=2000
workers=1

[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://ape_dts:PASSWORD_REDACTED@xx:3307/sbtest?ssl-mode=disabled
batch_size=16000
max_connections=10

[sinker]
db_type=mysql
sink_type=write
url=mysql://ape_dts:PASSWORD_REDACTED@xx:3308/sbtest?ssl-mode=disabled&statement-cache-capacity=4
batch_size=4000
max_connections=16

[parallelizer]
parallel_type=snapshot
parallel_size=16
rebalance_strategy=auto_split

[pipeline]
buffer_size=128000
checkpoint_interval_secs=1

[filter]
do_dbs=sbtest
do_tbs=sbtest.sbtest1,sbtest.sbtest2,sbtest.sbtest3,sbtest.sbtest4,sbtest.sbtest5,sbtest.sbtest6,sbtest.sbtest7,sbtest.sbtest8
do_events=insert

[runtime]
log_level=info
tb_parallel_size=4
log_dir=xx
log4rs_file=xx
```

## cdc

```toml
[metrics]
http_host=127.0.0.1
http_port=2000

[extractor]
db_type=mysql
extract_type=cdc
url=mysql://ape_dts:PASSWORD_REDACTED@xxx:3307/sbtest_cdc?ssl-mode=disabled
max_connections=10
binlog_filename=mysql-bin.000035
binlog_position=466727690
server_id=1170910737

[sinker]
db_type=mysql
sink_type=write
url=mysql://ape_dts:PASSWORD_REDACTED@xxx:3308/sbtest_cdc?ssl-mode=disabled&statement-cache-capacity=16
batch_size=4000
max_connections=16

[parallelizer]
parallel_type=rdb_merge
parallel_size=16

[pipeline]
buffer_size=128000
checkpoint_interval_secs=1

[filter]
do_dbs=sbtest_cdc
do_tbs=sbtest_cdc.sbtest1,sbtest_cdc.sbtest2,sbtest_cdc.sbtest3,sbtest_cdc.sbtest4,sbtest_cdc.sbtest5,sbtest_cdc.sbtest6,sbtest_cdc.sbtest7,sbtest_cdc.sbtest8,sbtest_cdc.ape_dts_bench_cdc_done
do_events=insert,update
do_ddls=alter_table

[runtime]
log_level=info
tb_parallel_size=1
log_dir=xx
log4rs_file=xx
```
