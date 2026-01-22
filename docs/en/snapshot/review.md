# Review data

After data revision, you can review the data again based on the check results.

The check results serve as a guide for specifying the rows/scope to be reviewed, and you still need to get the current data for each row from the source database, to compare it with the target.

# Example: MySQL -> MySQL

Refer to [task templates](../../templates/mysql_to_mysql.md) and [tutorial](../tutorial/mysql_to_mysql.md)

## Note

While this configuration is similar to that of snapshot migration, the only differences are:

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log

[checker]
db_type=mysql
url=mysql://user:password@127.0.0.1:3308

[parallelizer]
parallel_type=rdb_check
```

# Other configurations

- For [router], refer to [config details](../config.md).
- Refer to task_config.ini in tests:
    - dt-tests/tests/mysql_to_mysql/review
    - dt-tests/tests/pg_to_pg/review
    - dt-tests/tests/mongo_to_mongo/review
