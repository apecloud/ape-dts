# Enable heartbeat to source database

CDC tasks calculate delays by positions. For example, a MySQL CDC task uses the synced source binlog offset as the position. The position should be consistent with the source database if the CDC task catches up, and the timestamp of the position (if any) should follow the current time.

But if the source database has not been updated for a long time, or there are updates but the updated tables are not subscribed by the CDC task, then the position won't change, which will be considered as a delay. Therefore, we can create a heartbeat table in the source database and update the table periodically by CDC tasks to push the task position forward.

## CDC + Checker

If you want to validate CDC-applied data, enable `[checker]` in the CDC task config. See [Data Check](../snapshot/check.md) for configuration and limitations.
If both `[checker]` and `[sinker]` specify targets, `[checker]` takes precedence.

# Configurations

- For MySQL/PG/Mongo, refer to:
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

- For Redis，refer to: dt-tests/tests/redis_to_redis/cdc/heartbeat_test
```
[extractor]
heartbeat_interval_secs=10
heartbeat_key=5.ape_dts_heartbeat_key
```

# Heartbeat table

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

Note:
- The names of databases and tables should be the same with those of heartbeat_tb in task_config.ini.
- No need to create heartbeat tables for Mongo and Redis.
- Keep heartbeat_tb empty if not needed.
- If heartbeat_tb is configured but the table is NOT created, CDC task will try to create the table automatically. So, the extractor account needs to have corresponding permissions.
