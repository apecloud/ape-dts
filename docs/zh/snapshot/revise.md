# 数据订正

基于校验结果，您可发起订正任务。

校验结果起到指定订正范围的作用。每条数据仍需回查源库以获取当前值，并基于当前值和目标库进行订正。

# 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)

## 说明

此配置和全量同步任务的基本一致，两者的不同之处是：

```
[extractor]
extract_type=check_log
check_log_dir=./dt-tests/tests/mysql_to_mysql/revise/basic_test/check_log
```

# 其他配置

- 支持名称路由，详情请参考 [配置详解](../config.md)。
- 参考各类型集成测试的 task_config.ini：
    - dt-tests/tests/mysql_to_mysql/revise
    - dt-tests/tests/pg_to_pg/revise
    - dt-tests/tests/mongo_to_mongo/revise
