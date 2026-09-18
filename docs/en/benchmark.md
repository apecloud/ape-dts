# Benchmark

# MySQL -> MySQL

Use [sysbench](https://github.com/akopytov/sysbench) to generate snapshot data and binlogs.

Run tests with [ape_dts](/docs/en/tutorial/mysql_to_mysql.md) and [debezium](https://github.com/debezium/debezium).

## Test Environment

The source database, target database, and data migration task run on three separate Alibaba Cloud ECS instances within the same network.

## MySQL Specifications

| | Source | Target |
| :-------- | :-------- | :-------- |
| Version | mysql:8.4.11| mysql:8.4.11 |
| Specs | 8c16g| 8c16g |

## Snapshot migration

Use sysbench oltp_read_write to generate 8 tables, each with 4,000,000 rows.

### Results

| Node Specs | ape-dts 2.0.26.1 (Rows/s) | ape-dts 2.0.25 (Rows/s) | ape-dts 2.0.23 (Rows/s) |
| :--- | :--- | :--- | :--- |
| 1c2g | 109067 | 74576 | 71428 |
| 2c4g | 131783 | 129639 | 99403 |
| 4c8g | 133283 | 132781 | 126582 |

New test resource usage (CPU / RSS MiB) and elapsed time:

| Node Specs | Version | Elapsed (s) | Tool CPU / RSS (MiB) | Source MySQL CPU / RSS (MiB) | Target MySQL CPU / RSS (MiB) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1c2g | 2.0.26.1 | 293.40 | 80.58% / 600 | 7.28% / 8725 | 112.59% / 14235 |
| 1c2g | 2.0.25 | 429.09 | 96.86% / 1005 | 5.15% / 8635 | 69.43% / 12672 |
| 2c4g | 2.0.26.1 | 242.82 | 102.80% / 1768 | 9.18% / 8636 | 156.01% / 14291 |
| 2c4g | 2.0.25 | 246.84 | 158.88% / 1212 | 9.29% / 8772 | 149.88% / 12486 |
| 4c8g | 2.0.26.1 | 240.09 | 96.30% / 1998 | 9.06% / 8634 | 172.36% / 14254 |
| 4c8g | 2.0.25 | 241.00 | 183.50% / 1596 | 8.83% / 8633 | 184.19% / 13775 |

Historical resource usage (CPU / memory percentages, preserving the original units):

| Node Specs | ape-dts Version | Source MySQL CPU / Memory | Target MySQL CPU / Memory |
| :--- | :--- | :--- | :--- |
| 1c2g | 2.0.23 | 8.2% / 5.2% | 211% / 5.1% |
| 2c4g | 2.0.23 | 14.0% / 5.2% | 359% / 5.1% |
| 4c8g | 2.0.23 | 13.8% / 5.2% | 552% / 5.1% |

## CDC synchronization

### Test: 8 tables with 1 million baseline rows each and 4 million UPDATEs

Use sysbench oltp_update_index with 8 tables of 1 million baseline rows each and 4 million UPDATEs in total.

#### Results

| Node Specs | ape-dts 2.0.26.1 (UPDATE/s) | ape-dts 2.0.25 (UPDATE/s) | ape-dts 2.0.23 (provisional, UPDATE/s) |
| :--- | :--- | :--- | :--- |
| 1c2g | 23477 | 20365 | 11902 |
| 2c4g | 51171 | 41161 | 14240 |
| 4c8g | 52538 | 48681 | 19450 |

New test resource usage (CPU / RSS MiB) and elapsed time:

| Node Specs | Version | Elapsed (s) | Tool CPU / RSS (MiB) | Source MySQL CPU / RSS (MiB) | Target MySQL CPU / RSS (MiB) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1c2g | 2.0.26.1 | 170.38 | 91.76% / 568 | 2.71% / 1989 | 69.38% / 6915 |
| 1c2g | 2.0.25 | 196.41 | 89.56% / 287 | 2.38% / 1971 | 55.21% / 5049 |
| 2c4g | 2.0.26.1 | 78.17 | 141.33% / 988 | 5.73% / 1947 | 191.92% / 5627 |
| 2c4g | 2.0.25 | 97.18 | 140.01% / 470 | 4.57% / 1925 | 117.99% / 5775 |
| 4c8g | 2.0.26.1 | 76.14 | 154.92% / 589 | 5.63% / 2061 | 193.22% / 6058 |
| 4c8g | 2.0.25 | 82.17 | 161.90% / 795 | 5.24% / 1890 | 131.25% / 5386 |

Historical resource usage (CPU / memory percentages, preserving the original units; not directly comparable to RSS MiB):

| Node Specs | ape-dts Version | Source MySQL CPU / Memory | Target MySQL CPU / Memory |
| :--- | :--- | :--- | :--- |
| 1c2g | 2.0.23 | 19.0% / 5.2% | 479% / 5.1% |
| 2c4g | 2.0.23 | 18.6% / 5.2% | 623% / 5.1% |
| 4c8g | 2.0.23 | 19.2% / 5.2% | 689% / 5.1% |
