# Generate sqls from CDC data

Subscribe to data changes in the source database and generate sqls / reverse sqls, stored in sql.log.

Supported databases:
- MySQL
- PG

Supported data changes:
- DML

## CDC + Checker

If you want to validate CDC-applied data, enable `[checker]` in the CDC task config. See [Data Check](../snapshot/check.md) for configuration and limitations.

# Examples

Refer to [task templates](../../templates/mysql_to_mysql.md)

## Example 1: generate sqls
```
[extractor]
db_type=mysql
extract_type=cdc
binlog_position=0
binlog_filename=
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_tbs=test_db.*
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=sql
```

execute sqls in source:
```
use test_db;
insert into test_tb values(1, 1);
update test_tb set value=2 where id=1;
delete from test_tb where id=1;
```

generated sql.log:
```
INSERT INTO `test_db`.`test_tb`(`id`,`value`) VALUES(1,1);
UPDATE `test_db`.`test_tb` SET `id`=1,`value`=2 WHERE `id` = 1;
DELETE FROM `test_db`.`test_tb` WHERE `id` = 1;
```

## Example 2: generate reverse sqls
Add configs based on example 1:

```
[sinker]
reverse=true
```

execute sqls in source:
```
use test_db;
insert into test_tb values(1, 1);
update test_tb set value=2 where id=1;
delete from test_tb where id=1;
```

generated sql.log:
```
DELETE FROM `test_db`.`test_tb` WHERE `id` = 1;
UPDATE `test_db`.`test_tb` SET `id`=1,`value`=1 WHERE `id` = 1;
INSERT INTO `test_db`.`test_tb`(`id`,`value`) VALUES(1,2);
```

## Set start and end time
If you need data changes within a certain period of time, add configs:

```
[extractor]
start_time_utc=2024-10-09 02:00:00
end_time_utc=2024-10-09 03:00:00
```

# Data rollback
If some sqls were executed incorrectly in the source and you want to roll back the data, you may:
- generate reverse sqls and execute them from the last to the first.
