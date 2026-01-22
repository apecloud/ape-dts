# Migrate data from MySQL to MySQL

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mysql_to_mysql.md) and [common configs](/docs/en/config.md) for more details.

# Prepare MySQL instances

## Source

```
docker run -d --name some-mysql-1 \
--platform linux/x86_64 \
-it \
-p 3307:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 "$MYSQL_IMAGE" --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --sql_mode=NO_ENGINE_SUBSTITUTION \
 --default_time_zone=+08:00
```

## Target

```
docker run -d --name some-mysql-2 \
--platform linux/x86_64 \
-it \
-p 3308:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 "$MYSQL_IMAGE" --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --sql_mode=NO_ENGINE_SUBSTITUTION \
 --default_time_zone=+07:00 
```

# Migrate structures

## Prepare data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task
```
rm -rf /tmp/ape_dts
mkdir -p /tmp/ape_dts

cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
sink_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[filter]
do_dbs=test_db

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SHOW TABLES IN test_db;
```

```
+-------------------+
| Tables_in_test_db |
+-------------------+
| tb_1              |
+-------------------+
```

# Migrate snapshot data
## Prepare data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SELECT * FROM test_db.tb_1;
```

```
+----+-------+
| id | value |
+----+-------+
|  1 |     1 |
|  2 |     2 |
|  3 |     3 |
|  4 |     4 |
+----+-------+
```

# Check data
- check the differences between target data and source data

## Prepare data
- change target table records
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=1 WHERE id=2;
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/:/logs/" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
- cat /tmp/ape_dts/check_data_task_log/check/miss.log
```
{"schema":"test_db","tb":"tb_1","id_col_values":{"id":"1"},"diff_col_values":{}}
```
- cat /tmp/ape_dts/check_data_task_log/check/diff.log
```
{"schema":"test_db","tb":"tb_1","id_col_values":{"id":"2"},"diff_col_values":{"value":{"src":"2","dst":"1"}}}
```

# Revise data
- revise target data based on "check data" task results

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./check_data_task_log

[sinker]
db_type=mysql
sink_type=write
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SELECT * FROM test_db.tb_1;
```

```
+----+-------+
| id | value |
+----+-------+
|  1 |     1 |
|  2 |     2 |
|  3 |     3 |
|  4 |     4 |
+----+-------+
```

# Review data
- check if target data revised based on "check data" task results

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=check_log
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
check_log_dir=./check_data_task_log

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
-v "/tmp/ape_dts/review_data_task_log/:/logs/" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
- /tmp/ape_dts/review_data_task_log/check/miss.log and /tmp/ape_dts/review_data_task_log/check/diff.log should be empty

# Cdc task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SELECT * FROM test_db.tb_1;
```

```
+----+---------+
| id | value   |
+----+---------+
|  2 | 2000000 |
|  3 |       3 |
|  4 |       4 |
|  5 |       5 |
+----+---------+
```

# Cdc task with gtid
## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled
gtid_enabled=true
gtid_set=

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

DELETE FROM test_db.tb_1 WHERE id=3;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=4;
INSERT INTO test_db.tb_1 VALUES(6,6);
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SELECT * FROM test_db.tb_1;
```

```
+----+---------+
| id | value   |
+----+---------+
|  2 | 2000000 |
|  4 | 2000000 |
|  5 |       5 |
|  6 |       6 |
+----+---------+
```

# CDC task with ddl capture

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[filter]
do_dbs=test_db
do_events=insert,update,delete
do_ddls=create_database,drop_database,alter_database,create_table,alter_table,drop_table,create_index,drop_index,truncate_table,rename_table

[sinker]
db_type=mysql
sink_type=write
batch_size=200
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Do ddls in source
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3307

CREATE TABLE test_db.tb_2(id int, value int, primary key(id));
INSERT INTO test_db.tb_2 VALUES(1,1);
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -uroot -P3308

SELECT * FROM test_db.tb_2;
```

```
+----+-------+
| id | value |
+----+-------+
|  1 |     1 |
+----+-------+
```