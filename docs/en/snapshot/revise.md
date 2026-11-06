# Revise data

Based on the check results, you can initiate a revision task.

The check results serve as a guide for specifying the scope for revision, and you still need to get the current data for each row from the source database, to fix the data.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

## Note

While this configuration is similar to that of snapshot migration, the only differences are:

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log
```

# Other configurations

- For routing, refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/revise
    - dt-tests/tests/pg_to_pg/revise
    - dt-tests/tests/mongo_to_mongo/revise
