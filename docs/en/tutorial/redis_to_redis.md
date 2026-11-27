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

[sinker]
db_type=redis
sink_type=write
method=restore
url=redis://:123456@127.0.0.1:6390

[filter]
ignore_cmds=flushall,flushdb

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

# Check data after migration

Standalone Redis snapshot checks compare data without writing to the target:

- Missing source keys and extra target keys within the selected databases.
- Key types and logical values for string, hash, list, set, zset, and stream.
- Persistent/expiring state and expiration deadlines, using `[sinker].ttl_tolerance_ms`.

TTL is part of snapshot checking; no separate Redis structure check is needed.

## Configuration

```ini
[extractor]
db_type=redis
extract_type=snapshot
url=redis://source:6379
username=checker
password=your-password

[sinker]
db_type=redis
sink_type=check
url=redis://target:6379
username=checker
password=your-password
ttl_tolerance_ms=1000

[checker]
batch_size=200
recheck_count=2
recheck_interval_secs=1

[filter]
do_dbs=*

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=128

[runtime]
log_dir=./logs
```

Normal Redis authentication, TLS, and standalone/cluster connection options apply.
Database filters select source databases; database mappings must be one-to-one within that scope.

## Check flow

1. Read the source snapshot through the existing PSYNC/RDB extractor and compare each key against the target by type and logical content.
2. Scan the target and check source existence to find extra target keys.
3. Retry differences according to `recheck_count` and `recheck_interval_secs`, comparing the same source snapshot against the target.

Set `[checker].sample_percent` for sampled checking, or omit it for a full check.
Keep source/checker clocks synchronized for TTL comparison. Use a quiet write window when strict
consistency is required, since source and target reads do not share an atomic snapshot.

Module entries are skipped with a warning and counted in `skip_count`; supported types continue.
Stream consumer groups, consumers, PEL state, and hash-field expiration are not covered.
Inline checking, structure checking, check-log replay/revise, `output_full_row`, and
`output_revise_sql` are not supported.

## Results

- `miss.log`: source keys missing on the target.
- `diff.log`: extra target keys, type/value/TTL differences, and unstable reads.
- `summary.log`: check counts and overall consistency; skipped keys prevent a fully consistent result.

Keys are recorded in hexadecimal. Full values are omitted. See [Data check](../snapshot/check.md)
for common configuration and log formats.
