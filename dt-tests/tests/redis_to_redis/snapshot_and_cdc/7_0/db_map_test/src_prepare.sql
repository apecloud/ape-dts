flushall

SELECT 0
SET snapshot_db0 snapshot_db0
HSET snapshot_hash_db0 f1 v1 f2 v2
LPUSH snapshot_list_db0 v3 v2 v1
SADD snapshot_set_db0 v1 v2 v3
ZADD snapshot_zset_db0 1 v1 2 v2 3 v3
XADD snapshot_stream_db0 1-0 f1 v1 f2 v2

SELECT 2
SET snapshot_db2 snapshot_db2
HSET snapshot_hash_db2 f1 v1 f2 v2
LPUSH snapshot_list_db2 v3 v2 v1
SADD snapshot_set_db2 v1 v2 v3
ZADD snapshot_zset_db2 1 v1 2 v2 3 v3
XADD snapshot_stream_db2 1-0 f1 v1 f2 v2
