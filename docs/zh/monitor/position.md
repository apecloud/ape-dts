# 任务进度位点

任务进度记录在 position.log 中。刷新频率可在 [pipeline] checkpoint_interval_secs 中配置。

# 增量

对于增量任务，因为我们只保证目标库与源库的最终一致性，而不保证源库事务在目标库重放时的完整性，position.log 中记录 current_position 和 checkpoint_position 两份信息。

- current_position：已同步的数据的位点信息，可能处在源库某个大事务 binlog 的中间位置。
- checkpoint_position：已同步的完整的事务的位点信息。

如果任务中断，要使用 position.log 中的位点信息做断点续传，优先使用 checkpoint_position 作为新任务的起点，使用 current_position 可能导致 extractor 解析增量数据失败。

## MySQL

根据是否使用 gtid，分为两种，参考 [教程](../en/tutorial/mysql_to_mysql.md)

- 不使用 gtid，用 binlog_filename + next_event_position（即 binlog_position）做断点续传。

```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":44315,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

- 使用 gtid，用 gtid_set 做断点续传
```
2024-10-18 05:22:41.201208 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":50865,"gtid_set":"9663a096-8adc-11ef-b617-0242ac110002:1-3112","timestamp":"2024-10-18 05:22:41.000"}
```

## Postgres

使用 lsn 做断点续传。

```
2024-10-18 05:22:22.419787 | checkpoint_position | {"type":"PgCdc","lsn":"0/5D65CB0","timestamp":"2024-10-18 05:22:21.756"}
```

## Mongo

Mongo 增量任务没有记录 checkpoint_position，根据拉取数据方式不同，分为：

- 使用 operation_time 做断点续传（对应 [extractor] source=op_log）。
- 使用 resume_token 做断点续传（对应 [extractor] source=change_stream）。

```
2024-10-18 05:19:25.877182 | current_position | {"type":"MongoCdc","resume_token":"","operation_time":1729228763,"timestamp":"2024-10-18 05:19:23.000"}

```

```
2024-10-18 05:20:33.977700 | current_position | {"type":"MongoCdc","resume_token":"{\"_data\":\"826711F020000000042B022C0100296E5A10040E19213A975845EBAD0B8384EAE1DA1C46645F696400646711F01A88DC948E321DEC2A0004\"}","operation_time":1729228832,"timestamp":"2024-10-18 05:20:32.000"}
```


## Redis

位点信息如下，暂不支持断点续传。

```
2024-10-18 05:23:41.019195 | checkpoint_position | {"type":"Redis","repl_id":"1cd12b27acff56526106e343b9f4ff623b5e4c14","repl_port":10008,"repl_offset":2056,"now_db_id":0,"timestamp":""}
```

# 全量

全量任务如果包含多个库/多张表，则会按照 **先库后表** 排序，**依次同步** 各张表，有且只有一张表处于同步中。

如果表具有 **单一列构成的 主键/唯一键**，则 extractor 会以此列作为排序列，并按 `[extractor].batch_size` 从小到大分片拉取；启用 chunk 切分时，该值也作为目标 chunk 大小。如果表没有排序列，则 extractor 会流式拉取该表所有数据。

## default.log

某张表同步开始/完成，则在 default.log 中记录：

```
2024-02-28 10:07:35.531681 - INFO - [14778588] - start extracting data from `test_db_1`.`one_pk_no_uk` by slices
2024-02-28 10:07:35.735439 - INFO - [14778588] - end extracting data from `test_db_1`.`one_pk_no_uk`, all count: 9
```

## finished.log

某张表同步完成，则在 finished.log 中记录：

```
2024-10-10 04:04:07.803422 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"a"}
```

## position.log

如果表数据较多，则会将当前进度信息写入 position.log。

全量进度存储的是当前 **正在同步的表** 的 **最后一条已同步数据** 的排序列的值，该值可用于断点续传。

### MySQL

```
2024-10-10 04:04:08.152044 | current_position | {"type":"RdbSnapshot","db_type":"mysql","schema":"test_db","tb":"b","order_col":"id","value":"6"}
```

### Postgres

```
2024-10-10 04:04:09.223040 | current_position | {"type":"RdbSnapshot","db_type":"pg","schema":"test_db","tb":"b","order_col":"id","value":"6"}
```

### Mongo

```
2024-10-18 04:10:35.792078 | current_position | {"type":"RdbSnapshot","db_type":"mongo","schema":"test_db_2","tb":"tb_1","order_col":"_id","value":"6711dfb9643426296f0cb93d"}
```

### Redis
```
2024-10-18 05:24:47.932794 | current_position | {"type":"Redis","repl_id":"1cd12b27acff56526106e343b9f4ff623b5e4c14","repl_port":10008,"repl_offset":5103,"now_db_id":0,"timestamp":""}
```
