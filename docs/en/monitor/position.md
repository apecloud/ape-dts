# Task progress info

Task progress will be recorded periodically in position.log(configuration: [pipeline] checkpoint_interval_secs).

# CDC

For CDC tasks, we only guarantee eventual consistency between target and source, binlog/wal for a big transaction in source may be synced to target by multiple parts. Thus, we will record both current_position and checkpoint_position in position.log.

- current_position: position of synced data, may be in the middle of a large transaction binlog/wal.
- checkpoint_position: position of the last synced transaction binlog/wal.

If task interrupts, use checkpoint_position as the starting point for new task, refer to [CDC task resume](../cdc/resume.md), using current_position may cause errors when parsing binlog/wal.

## MySQL

Depends on gtid enabled or not, refer to [tutorial](./tutorial/mysql_to_mysql.md):

- Use binlog_filename + next_event_position as position if gtid disabled.

```
2024-10-18 05:21:45.207788 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":44315,"gtid_set":"","timestamp":"2024-10-18 05:21:44.000"}
```

- Use gtid_set as position if gtid enabled.
```
2024-10-18 05:22:41.201208 | checkpoint_position | {"type":"MysqlCdc","server_id":"","binlog_filename":"mysql-bin.000004","next_event_position":50865,"gtid_set":"9663a096-8adc-11ef-b617-0242ac110002:1-3112","timestamp":"2024-10-18 05:22:41.000"}
```

## Postgres

Use lsn as position.

```
2024-10-18 05:22:22.419787 | checkpoint_position | {"type":"PgCdc","lsn":"0/5D65CB0","timestamp":"2024-10-18 05:22:21.756"}
```

## Mongo

Only current_position for Mongo, depends on source types:

- Use operation_time as position ([extractor] source=op_log).

```
2024-10-18 05:19:25.877182 | current_position | {"type":"MongoCdc","resume_token":"","operation_time":1729228763,"timestamp":"2024-10-18 05:19:23.000"}
```

- Use resume_token as position ([extractor] source=change_stream).

```
2024-10-18 05:20:33.977700 | current_position | {"type":"MongoCdc","resume_token":"{\"_data\":\"826711F020000000042B022C0100296E5A10040E19213A975845EBAD0B8384EAE1DA1C46645F696400646711F01A88DC948E321DEC2A0004\"}","operation_time":1729228832,"timestamp":"2024-10-18 05:20:32.000"}
```


## Redis

Use repl_offset as position.

```
2024-10-18 05:23:41.019195 | checkpoint_position | {"type":"Redis","repl_id":"1cd12b27acff56526106e343b9f4ff623b5e4c14","repl_port":10008,"repl_offset":2056,"now_db_id":0,"timestamp":""}
```

# Snapshot

If the snapshot task contains multiple databases/tables, tables will be sorted **first by database name and then table name**, and they will be migrated to the target **one by one**.

If a table has a **single column** **primary key/unique key**, extractor will use it as sorting column and pull data in batches (configuration: `[extractor].batch_size`) from small to large. When chunk splitting is enabled, this value is also the target chunk size. Otherwise the table will be pulled in a stream.

## default.log

Once a table migrating starts/ends, in default.log:

```
2024-02-28 10:07:35.531681 - INFO - [14778588] - start extracting data from `test_db_1`.`one_pk_no_uk` by slices
2024-02-28 10:07:35.735439 - INFO - [14778588] - end extracting data from `test_db_1`.`one_pk_no_uk`, all count: 9
```

## finished.log

Once a table migrating ends, in finished.log:

```
2024-10-10 04:04:07.803422 | {"type":"RdbSnapshotFinished","db_type":"mysql","schema":"test_db","tb":"a"}
```

## position.log

The progress of migrating tables will be logged in position.log.
Use the sorting column's value of the **last migrated record** as position.

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
