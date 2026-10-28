# Migrate data from Redis to Redis

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/redis_to_redis.md) and [common configs](/docs/en/config.md) for more details.

# Prepare Redis instances

## Source

```
docker run --name src-redis-7-0 \
    -p 6380:6379 \
    -d "$REDIS_IMAGE" redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```

## Target

```
docker run --name dst-redis-7-0 \
    -p 6390:6379 \
    -d "$REDIS_IMAGE" redis-server \
    --requirepass 123456 \
    --save 60 1 \
    --loglevel warning
```

# Migrate snapshot data
## Prepare data
```
telnet 127.0.0.1 6380
auth 123456

SELECT 0
SET key_1 val_1

SELECT 1
SET key_2 val_2
```

## Start task
```
rm -rf /tmp/ape_dts
mkdir -p /tmp/ape_dts

cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=redis
extract_type=snapshot
repl_port=10008
url=redis://:123456@127.0.0.1:6380

[source.db]
include=*

[sinker]
db_type=redis
sink_type=write
url=redis://:123456@127.0.0.1:6390

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1

[parallelizer]
parallel_type=redis
parallel_size=8
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Check results
```
telnet 127.0.0.1 6390
auth 123456

SELECT 0
+OK
GET key_1
$5
val_1

SELECT 1
+OK
GET key_2
$5
val_2
```

# Snapshot + Cdc task
- Currently we do not support synchronizing only cdc data, the cdc task will first migrate the snapshot data and then synchronize the cdc data.

## Clear target data
```
telnet 127.0.0.1 6390
auth 123456

flushall
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=redis
extract_type=snapshot_and_cdc
repl_id=
now_db_id=0
repl_port=10008
repl_offset=0
url=redis://:123456@127.0.0.1:6380

[source.db]
include=*

[source.cmd]
exclude=flushall,flushdb

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1

[parallelizer]
parallel_type=redis
parallel_size=8
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini 
```

## Change source data
```
telnet 127.0.0.1 6380
auth 123456

SELECT 0
SET key_3 val_3

SELECT 1
SET key_4 val_4
```

## Check results
```
telnet 127.0.0.1 6390
auth 123456

SELECT 0
+OK
GET key_1
$val_1
GET key_3
$5
val_3

SELECT 1
+OK
GET key_2
$5
val_2
GET key_4
$5
val_4
```