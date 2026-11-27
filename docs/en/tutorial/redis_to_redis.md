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

## Snapshot expiration with RESTORE

RDB entries retain their absolute expiration deadline. When using `method=restore`, DTS converts
that deadline to a remaining TTL immediately before encoding commands for sending. Entries whose
remaining TTL is zero or negative are skipped and logged with their key in hexadecimal and their
expiration deadline; DTS leaves any existing target value untouched for skipped entries. Permanent
keys use `RESTORE` TTL `0`. This uses relative TTL on all supported target versions, without `ABSTTL`;
command encoding, network transfer, and server execution can still introduce a small delay.

# Check data after migration

Redis snapshot checks compare the source and target without migrating or repairing data:

- Source keys missing on the target, and extra target keys, within the selected logical databases.
- Logical values of strings, hashes, lists, sets, sorted sets, and streams.
- Permanent versus expiring keys and expiration deadlines, with a configurable tolerance.

TTL is part of data checking; Redis does not need a separate structure-check task.

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

The ordinary source/target authentication and TLS options apply. Extractor type determines how
source data is read: `snapshot` uses the existing PSYNC/RDB extractor, including its replication
permissions and snapshot generation costs. The checker consumes the resulting `DtData::Redis` at
the sink stage; it does not substitute another extractor or refetch every source value initially.

The target needs the read commands below, `SELECT` and `INFO`. Source reads are needed for reverse
existence checks for extra target keys; initial differences and their retries keep the incoming
source snapshot and only reread the target. Cluster additionally requires `CLUSTER NODES`
and connectivity to the advertised master addresses. No data-write permissions, `DEBUG`, or scripts
are required by the checker. `is_cluster` may be supplied or detected through `INFO cluster`.

The source RDB pass and target reverse scan run in one task. The configured parallelizer is retained.
With `parallel_type=redis`, check sinkers expose target master IDs for the existing slot routing;
point reads route to the appropriate target/source master.
Cluster topology changes that return `MOVED`/`ASK` fail the task so a new complete scan can be run.

`[checker].sample_percent`, when specified, uses the same stable DB/key hash in both directions.
It samples comparisons, not source RDB extraction or the target reverse scan. Omit it for a full check. `batch_size` controls scan
and target value page sizes (capped at 1000); string comparison uses 64 KiB ranges.

Database filters are source-based. Database mappings must be one-to-one within the selected source
scope. For example, with `db_map=0:3`, select `do_dbs=0` if database 3 would otherwise also be selected
as an unrenamed source. Extra keys in unrelated target databases are outside that mapped scope.
Without a mapping, the reverse scan also finds target-only logical databases.

`output_full_row` and `output_revise_sql` are not supported and are rejected at startup. Inline
checking, Redis structure checking, and check-log replay/revise are outside this mode.

## Comparison

Type and length/cardinality are checked before contents. Initial source values come from the
extractor's RDB entries, including their absolute expiration deadlines. Target values are read in
pages; the checker borrows the decoded source value without cloning its payload. Retries preserve that source value and reread the target:

| Type | Read strategy |
| --- | --- |
| string | `STRLEN` and matching `GETRANGE` chunks |
| list | `LLEN` and matching `LRANGE` pages, preserving order |
| hash | `HLEN` and target `HMGET` for fields in the decoded source hash |
| set | `SCARD` and pipelined target `SISMEMBER` for decoded source members |
| zset | `ZCARD` and pipelined target `ZSCORE` for decoded source members, comparing numerical scores |
| stream | `XLEN`, matching `XRANGE` pages and `last-generated-id` from `XINFO STREAM` |

Streams preserve entry IDs and the sequence of field/value pairs, including repeated fields.
Consumer groups, consumers, PEL state, module values, and hash-field expiration are not covered.
Source module entries are skipped with a warning containing the database and hexadecimal key;
other supported types continue to be checked. Skipped module entries increment `summary.log.skip_count`
and are not counted as checked or equal, so the summary cannot report full consistency.
Other unsupported value types fail the task. Raw RDB bytes and DUMP checksums are not used as logical equality tests.

TTL comparison distinguishes missing, persistent, and expiring keys. The source uses the absolute RDB deadline (requiring source/checker clocks to be synchronized);
the target uses `PTTL`. Both are converted to the same monotonic check-time baseline. Request latency adds
sampling uncertainty to `[sinker].ttl_tolerance_ms`; the two remaining TTL numbers are not compared directly.
The `expires_at_check_elapsed_ms` values in difference logs use that check-relative baseline, not a
Unix timestamp.

The source RDB snapshot and target reads do not share an atomic snapshot. Target metadata is checked
before and after comparison. Changed type/expiration and `WRONGTYPE`/disappearing-stream races
are classified as unstable and retried when `recheck_count` is nonzero. With zero retries, the initial
result is retained, matching the other standalone checkers. Same-length concurrent writes cannot always be detected; use a
quiescent window when strict consistency is needed. After the source entry
reaches its recorded expiration deadline, an absent target resolves and a surviving target is reported
as an existence difference. This is checked on comparison/retry, not by a forced live-source reread.

## Results and resource limits

The task uses the existing `miss.log`, `diff.log`, `summary.log`, stdout result stream, and optional
standalone S3 upload. No separate result format or database-side state store is required.

`db` is the source logical database and `target_db` is included when mapped. Keys are losslessly
encoded as `id_col_values.key = "hex:<bytes>"`. Source/target full values are omitted. The keys in
`diff_col_values` identify `existence` (extra target key), `type`, `value`, `ttl`, or `unstable`.
A key can have both `value` and `ttl` differences. For content differences, `src`/`dst` are null
because contents were deliberately omitted; `src_type`/`dst_type` carry the Redis type.

`miss_count` counts unique final source keys missing on the target. `diff_count` counts unique final
keys with other differences; extra target keys are included there. Unresolved unstable keys count as
skipped, making the result inconsistent. Query/connection/spool failures fail the task.
`checked_count` counts source comparison observations emitted by the extractor. Database entries in `tables` provide the same count breakdown.

RDB extraction retains its existing whole-key decoding and raw-byte buffering. Paging target reads
does not impose a hard limit on the source key size or a single large target collection element.

Final difference identities and small result records are spooled under a task-specific `.redis-check-*`
directory inside the check-log directory, then externally sorted/deduplicated before output. Source
comparison results take precedence over reverse-scan results for the same identity.
`recheck_queue_size` and `recheck_queue_memory_mb` bound each in-memory sorting run, not total disk
usage. Retries retain only the current bounded input batch, sleep once per batch/round, and reread
the target against the same source payload. Source payloads are never serialized to spool files.
Spool files are removed after successful or failed finalization; abruptly terminated processes may
leave them behind.

Paging bounds normal working memory, but SCAN COUNT is a hint rather than a byte limit. A single
large hash field, list element, or stream entry can still produce a large response. Disk space for
candidate identities and instance-side query costs must also be considered for large checks.
