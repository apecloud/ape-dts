# Benchmark

# MySQL -> MySQL

Use [sysbench](https://github.com/akopytov/sysbench) to generate snapshot and incremental data. Use [ape_dts](/docs/en/tutorial/mysql_to_mysql.md) to run migration tasks.

## Test Environment

The source database, target database, and data migration task run on three separate Alibaba Cloud ECS instances within the same network.

## MySQL Specifications

|         | Source       | Target       |
| ------- | ------------ | ------------ |
| Version | mysql:8.4.11 | mysql:8.4.11 |
| Specs   | 8c16g        | 8c16g        |

## Snapshot migration

Use sysbench oltp_read_write to generate 8 tables, each with 4,000,000 rows.

### Results

| Node Specs | ape-dts 2.0.26.1 (Rows/s) | ape-dts 2.0.25 (Rows/s) | ape-dts 2.0.23 (Rows/s) |
| ---------- | ------------------------- | ----------------------- | ----------------------- |
| 1c2g       | 109067                    | 74576                   | 71428                   |
| 2c4g       | 131783                    | 129639                  | 99403                   |
| 4c8g       | 133283                    | 132781                  | 126582                  |

Resource usage (CPU / RSS MiB) and elapsed time:

| Node Specs | Metric                       | ape-dts 2.0.26.1 | ape-dts 2.0.25  | ape-dts 2.0.23       |
| ---------- | ---------------------------- | ---------------- | --------------- | -------------------- |
| 1c2g       | Tool CPU / RSS (MiB)         | 80.58% / 600     | 96.86% / 1005   | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 7.28% / 8725     | 5.15% / 8635    | 8.2% / Not recorded  |
|            | Target MySQL CPU / RSS (MiB) | 112.59% / 14235  | 69.43% / 12672  | 211% / Not recorded  |
|            | Elapsed (s)                  | 293.40           | 429.09          | Not recorded         |
| 2c4g       | Tool CPU / RSS (MiB)         | 102.80% / 1768   | 158.88% / 1212  | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 9.18% / 8636     | 9.29% / 8772    | 14.0% / Not recorded |
|            | Target MySQL CPU / RSS (MiB) | 156.01% / 14291  | 149.88% / 12486 | 359% / Not recorded  |
|            | Elapsed (s)                  | 242.82           | 246.84          | Not recorded         |
| 4c8g       | Tool CPU / RSS (MiB)         | 96.30% / 1998    | 183.50% / 1596  | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 9.06% / 8634     | 8.83% / 8633    | 13.8% / Not recorded |
|            | Target MySQL CPU / RSS (MiB) | 172.36% / 14254  | 184.19% / 13775 | 552% / Not recorded  |
|            | Elapsed (s)                  | 240.09           | 241.00          | Not recorded         |

Note: Memory usage for ape-dts 2.0.23 is estimated from the 8c16g source and target instances as 16 GiB (16384 MiB) × the original memory percentage: source 5.2% ≈ 852 MiB; target 5.1% ≈ 836 MiB. These are RSS estimates, not measured RSS values. Tool resource usage and elapsed time were not recorded.

## CDC synchronization

### Test: 8 tables with 1 million baseline rows each and 4 million UPDATEs

Use sysbench oltp_update_index with 8 tables of 1 million baseline rows each and 4 million UPDATEs in total.

#### Results

| Node Specs | ape-dts 2.0.26.1 (UPDATE/s) | ape-dts 2.0.25 (UPDATE/s) | ape-dts 2.0.23 (UPDATE/s) |
| ---------- | --------------------------- | ------------------------- | ------------------------- |
| 1c2g       | 23477                       | 20365                     | 11902                     |
| 2c4g       | 51171                       | 41161                     | 14240                     |
| 4c8g       | 52538                       | 48681                     | 19450                     |

Resource usage (CPU / RSS MiB) and elapsed time:

| Node Specs | Metric                       | ape-dts 2.0.26.1 | ape-dts 2.0.25 | ape-dts 2.0.23       |
| ---------- | ---------------------------- | ---------------- | -------------- | -------------------- |
| 1c2g       | Tool CPU / RSS (MiB)         | 91.76% / 568     | 89.56% / 287   | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 2.71% / 1989     | 2.38% / 1971   | 19.0% / Not recorded |
|            | Target MySQL CPU / RSS (MiB) | 69.38% / 6915    | 55.21% / 5049  | 479% / Not recorded  |
|            | Elapsed (s)                  | 170.38           | 196.41         | Not recorded         |
| 2c4g       | Tool CPU / RSS (MiB)         | 141.33% / 988    | 140.01% / 470  | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 5.73% / 1947     | 4.57% / 1925   | 18.6% / Not recorded |
|            | Target MySQL CPU / RSS (MiB) | 191.92% / 5627   | 117.99% / 5775 | 623% / Not recorded  |
|            | Elapsed (s)                  | 78.17            | 97.18          | Not recorded         |
| 4c8g       | Tool CPU / RSS (MiB)         | 154.92% / 589    | 161.90% / 795  | Not recorded         |
|            | Source MySQL CPU / RSS (MiB) | 5.63% / 2061     | 5.24% / 1890   | 19.2% / Not recorded |
|            | Target MySQL CPU / RSS (MiB) | 193.22% / 6058   | 131.25% / 5386 | 689% / Not recorded  |
|            | Elapsed (s)                  | 76.14            | 82.17          | Not recorded         |

# 2.0.26.1 1c2g Task Configuration Reference

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
