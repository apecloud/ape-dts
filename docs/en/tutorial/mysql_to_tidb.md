# Migrate data from MySQL to TiDB

- TiDB's protocol is compatible with MySQL, ape_dts uses the same method for `MySQL -> MySQL` to achieve `MySQL -> TiDB`.

- Therefore, this article only provides a simple demo for structure migration. For other task types, please refer to [MySQL -> MySQL](./mysql_to_mysql.md)


# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mysql_to_mysql.md) and [common configs](/docs/en/config.md) for more details.

# Prepare MySQL instance
Refer to [MySQL -> MySQL](./mysql_to_mysql.md)

# Prepare TiDB instance

- Start instance
```
docker run --name some-tidb -d \
-v /tmp/tidb/data:/tmp/tidb \
-p 4000:4000 -p 10080:10080 \
pingcap/tidb:v7.1.6
```

- Create user
```
mysql -h 127.0.0.1 -P 4000 -u root -D test --prompt="tidb> "

CREATE DATABASE demo CHARACTER SET utf8 COLLATE utf8_general_ci;
CREATE USER 'demo'@'%' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON *.* TO 'demo'@'%';
FLUSH PRIVILEGES;
```

# Migrate structures
## Prepare source data
```
mysql -h127.0.0.1 -uroot -p123456 -P3307

CREATE DATABASE test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=mysql
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
url=mysql://demo:123456@127.0.0.1:4000?ssl-mode=disabled
sink_type=struct
db_type=tidb

[source.db]
include=test_db

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
mysql -h127.0.0.1 -udemo -p123456 -P4000

SHOW CREATE TABLE test_db.tb_1;
```

```
CREATE TABLE `tb_1` (
  `id` int(11) NOT NULL,
  `value` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
```

# Differences with `MySQL -> MySQL`
- For `MySQL -> TiDB` tasks, the only difference in config is:

```
[sinker]
db_type=tidb
```

- Please note that the charsets, collations, and data types supported by [TiDB](https://docs.pingcap.com/zh/tidb/stable/data-type-overview) are only a subset of those in MySQL. If you are migrating from MySQL to TiDB, make sure the data is within the supported range.