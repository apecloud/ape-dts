# Resume from Checkpoint

When starting a task, you can retrieve the latest task position from the previous execution based on the configuration, allowing you to continue the task without starting from scratch.

## CDC + Checker

If you want to validate CDC-applied data, enable `[checker]` in the CDC task config. See [Data Check](../snapshot/check.md) for configuration and limitations.
If both `[checker]` and `[sinker]` specify targets, `[checker]` takes precedence.

## Supported Sources

- MySQL source
- Postgres source
- Mongo source

## Position Recording

CDC task progress is periodically recorded in position.log. The recording frequency of task progress depends on the `pipeline.checkpoint_interval_secs` configuration, with a default value of 10s.

Whether or not you enable resume from checkpoint, position information will be recorded as logs during task execution, located in the logs directory (`runtime.log_dir`) as position.log.

In addition, you can persist task positions in the `target database` or a `specified database` through configuration. Recording positions to the database will consume approximately `150 bytes` of storage.
To ensure position information is not affected by other tasks, it is recommended to specify the task_id through the configuration file: `global.task_id`
**This feature only supports MySQL or PG as the target database/specified database**
**The target account needs to have permissions to create MySQL database/PG schema and create tables**

## Position Reading

You can choose to read positions `from logs`, `from target database`, or `from a specified database` through configuration.

# Progress Logs

For detailed explanations, please refer to [Position Information](../monitor/position.md)

## MySQL position.log

# Configuration

## Resume from Target Database

```
[global]
//[Optional]
task_id=task1

[resumer]
resume_type=from_target
//[Optional] Default value is apecloud_metadata.apedts_task_position
table_full_name=apecloud_resumer_test.ape_task_position
max_connections=1
```

When the task starts, it will automatically ensure that the target has the `apecloud_resumer_test.ape_task_position` database table configured, and initialize a connection pool with a maximum of 1 connection for subsequent resume-related position recording and querying.

## Resume from Specified Database

```
[global]
//[Optional]
task_id=task1

[resumer]
resume_type=from_db
url=mysql://xxx:xxx@127.0.0.1:3306
db_type=mysql
//[Optional] Default value is apecloud_metadata.apedts_task_position
table_full_name=apecloud_resumer_test.ape_task_position
max_connections=1
```

When the task starts, it will initialize a connection pool with a maximum of 1 connection and automatically ensure that the configured database instance has the `apecloud_resumer_test.ape_task_position` database table for subsequent resume-related position recording and querying.

## Resume from Log

```
[runtime]
log_dir=/logs

[resumer]
resume_type=from_log
//[Optional] Uses runtime.log_dir by default
log_dir=/other_logs
```

Looks for position.log in `/other_logs` to resume from checkpoint.
