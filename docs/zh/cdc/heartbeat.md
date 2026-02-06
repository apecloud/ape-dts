# 开启源库心跳

增量任务通过位点来记录延迟情况，如 MySQL 增量任务将已同步的源库 binlog 位置作为位点。如果增量任务当前是追平状态，那么位点应该和源库一致，且位点的 timestamp（如有）应该和当前时间一致。

但是，如果源库本身长时间没有更新，或者有更新但更新的表不在任务的订阅范围，此时，增量任务的位点就不会朝前推进。因此，我们可以通过在源库预建心跳表，使增量任务定时更新该表，来推动任务位点前进。

# 配置

- 对于 MySQL/PG/Mongo，请参考：
    - dt-tests/tests/mysql_to_mysql/cdc/heartbeat_test
    - dt-tests/tests/pg_to_pg/cdc/heartbeat_test
    - dt-tests/tests/mongo_to_mongo/cdc/heartbeat_test

```
[extractor]
heartbeat_interval_secs=10
heartbeat_tb=test_db_1.ape_dts_heartbeat

[filter]
ignore_tbs=test_db_1.ape_dts_heartbeat
```

- 对于 Redis，请参考：dt-tests/tests/redis_to_redis/cdc/heartbeat_test
```
[extractor]
heartbeat_interval_secs=10
heartbeat_key=5.ape_dts_heartbeat_key
```

# 心跳表

- MySQL
```
CREATE TABLE IF NOT EXISTS `{}`.`{}`(
    server_id INT UNSIGNED,
    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    received_binlog_filename VARCHAR(255),
    received_next_event_position INT UNSIGNED,
    received_timestamp VARCHAR(255),
    flushed_binlog_filename VARCHAR(255),
    flushed_next_event_position INT UNSIGNED,
    flushed_timestamp VARCHAR(255),
    PRIMARY KEY(server_id)
)
```

- PG
```
CREATE TABLE IF NOT EXISTS "{}"."{}"(
    slot_name character varying(64) not null,
    update_timestamp timestamp without time zone default (now() at time zone 'utc'),
    received_lsn character varying(64),
    received_timestamp character varying(64),
    flushed_lsn character varying(64),
    flushed_timestamp character varying(64),
    primary key(slot_name)
)
```

请注意：
- 库名 & 表名 需和 task_config.ini 中 heartbeat_tb 一致。
- Mongo 和 Redis 不需要预建心跳表。
- 如果不需要任务触发心跳，则无需配置 heartbeat_tb。
- 如果配置了 heartbeat_tb，但用户并未手动预建心跳表，增量任务会尝试建表，但这需要 extractor 使用的账户拥有相应权限。
