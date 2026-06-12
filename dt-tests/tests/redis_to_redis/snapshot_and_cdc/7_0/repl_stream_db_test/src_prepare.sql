flushall

SELECT 1
SET zzz_snapshot_db1 snapshot_db1
HSET zzz_snapshot_hash_db1 f1 v1 f2 v2
LPUSH zzz_snapshot_list_db1 v3 v2 v1
SADD zzz_snapshot_set_db1 v1 v2 v3
ZADD zzz_snapshot_zset_db1 1 v1 2 v2 3 v3
XADD zzz_snapshot_stream_db1 1-0 f1 v1 f2 v2

SELECT 0
SET aaa_snapshot_db0 snapshot_db0
HSET aaa_snapshot_hash_db0 f1 v1 f2 v2
LPUSH aaa_snapshot_list_db0 v3 v2 v1
SADD aaa_snapshot_set_db0 v1 v2 v3
ZADD aaa_snapshot_zset_db0 1 v1 2 v2 3 v3
XADD aaa_snapshot_stream_db0 1-0 f1 v1 f2 v2

SELECT 2
SET zzz_snapshot_db2 snapshot_db2
HSET zzz_snapshot_hash_db2 f1 v1 f2 v2
LPUSH zzz_snapshot_list_db2 v3 v2 v1
SADD zzz_snapshot_set_db2 v1 v2 v3
ZADD zzz_snapshot_zset_db2 1 v1 2 v2 3 v3
XADD zzz_snapshot_stream_db2 1-0 f1 v1 f2 v2

SELECT 0
