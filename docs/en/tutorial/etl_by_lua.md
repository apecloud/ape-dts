# Modify data by Lua
In the following types of tasks, you can modify data by a Lua script before they were written to target.

- mysql -> mysql
- mysql -> kafka
- pg -> pg
- pg -> kafka

For details, refer to [etl by lua](../etl/lua.md)

This is a tutorial on using Lua script to edit data for mysql -> mysql task.

# Prerequisites
- [prerequisites](./prerequisites.md)

# Prepare MySQL instances

Refer to [mysql to mysql](./mysql_to_mysql.md)

# Lua script
```
cat <<EOL > /tmp/ape_dts/etl.lua
if (schema == "test_db" and tb == "tb_1" and row_type == "insert")
then
    after["value"] = 10000
end
EOL
```

# Snapshot task

## Prepare data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

```
mysql -h127.0.0.1 -uroot -p123456 -P3308

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
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

[processor]
lua_code_file=/etl.lua

[source.db]
include=test_db

[source.event]
include=insert

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
-v "/tmp/ape_dts/etl.lua:/etl.lua" \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
mysql -h127.0.0.1 -uroot -p123456 -P3308

SELECT * FROM test_db.tb_1;
```
```
+----+-------+
| id | value |
+----+-------+
|  1 | 10000 |
|  2 | 10000 |
|  3 | 10000 |
|  4 | 10000 |
+----+-------+
```

# Cdc task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mysql
extract_type=cdc
server_id=2000
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[source.db]
include=test_db

[source.event]
include=insert,update,delete

[processor]
lua_code_file=/etl.lua

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
-v "/tmp/ape_dts/etl.lua:/etl.lua" \
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
mysql -h127.0.0.1 -uroot -p123456 -P3308

SELECT * FROM test_db.tb_1;
```
```
+----+---------+
| id | value   |
+----+---------+
|  2 | 2000000 |
|  3 |   10000 |
|  4 |   10000 |
|  5 |   10000 |
+----+---------+
```