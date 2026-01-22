# Execute snapshot and CDC tasks serially without data loss

In most data migration/import scenarios, you may want to migrate snapshot first, then subscribe to source cdc data and synchronize it to target.

This article tells you what to do before starting a snapshot task, and how to configure cdc task_config.ini to avoid data loss.

## CDC + Checker

If you want to validate CDC-applied data after the snapshot + CDC pipeline, enable `[checker]` in the CDC task and refer to [Data Check](../snapshot/check.md) for configuration and limitations.

# Source: MySQL

Refer to [mysql -> mysql](./mysql_to_mysql.md) or [mysql -> kafka](./mysql_to_kafka_consumer.md) to generate snapshot_task_config.ini & cdc_task_config.ini.

- Get binlog info of source MySQL before starting snapshot task

```
mysql -h127.0.0.1 -uroot -p123456 -P3307
show master status;

+------------------+----------+--------------+------------------+-------------------------------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                         |
+------------------+----------+--------------+------------------+-------------------------------------------+
| mysql-bin.000003 |     3009 |              |                  | 9663a096-8adc-11ef-b617-0242ac110002:1-17 |
+------------------+----------+--------------+------------------+-------------------------------------------+
```

- Update cdc_task_config.ini
```
[extractor]
binlog_position=3009
binlog_filename=mysql-bin.000003
```

- Start snapshot task
- Start cdc task once snapshot task finished, all changes made during snapshot task will be synchronized to target.

# Source: Postgres
Refer to [pg -> pg](./pg_to_pg.md) or [pg -> kafka](./pg_to_kafka_consumer.md) to generate snapshot_task_config.ini & cdc_task_config.ini.

- Check if replication slot exists in source Postgres
```
SELECT * FROM pg_catalog.pg_replication_slots WHERE slot_name = 'ape_test';
```

- Drop it if not used by others, or use another slot_name
```
SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
```

- Create slot and get starting lsn
```
SELECT * FROM pg_create_logical_replication_slot('ape_test', 'pgoutput');
```
```
 slot_name |    lsn    
-----------+-----------
 ape_test  | 0/3D583B0
```

- Check if publication exists
- By default, the pubname will be "[slot_name]_publication_for_all_tables", if you already have a publication for all tables, for example: my_some_publication, you can reuse it without creating a new one, just configure it in task_config.ini as described later in this article.
```
SELECT * FROM pg_catalog.pg_publication WHERE pubname = 'ape_dts_publication_for_all_tables';
```

- Create publication for all tables
```
CREATE PUBLICATION ape_dts_publication_for_all_tables FOR ALL TABLES;
```

- Update cdc_task_config.ini
```
[extractor]
start_lsn=0/3D583B0
pub_name=ape_dts_publication_for_all_tables
```

- Start snapshot task
- Start cdc task once snapshot task finished, all changes made during snapshot task will be synchronized to target.

# Source: Mongo
Refer to [mongo -> mongo](./mongo_to_mongo.md) to generate snapshot_task_config.ini & cdc_task_config.ini.

- Get current timestamp accurate to seconds from source Mongo
```
docker exec -it src-mongo mongosh --quiet

print(Math.floor(Date.now() / 1000));
```
```
1729070711
```

- Update cdc_task_config.ini
- This works for both source=change_stream and source=op_log
```
[extractor]
start_timestamp=1729070711
```

- Start snapshot task
- Start cdc task once snapshot task finished, all changes made during snapshot task will be synchronized to target.
