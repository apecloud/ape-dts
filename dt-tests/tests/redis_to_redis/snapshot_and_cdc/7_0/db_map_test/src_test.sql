SELECT 0
SET cdc_db0 cdc_db0
HSET cdc_hash_db0 f1 v1 f2 v2
LPUSH cdc_list_db0 v3 v2 v1
SADD cdc_set_db0 v1 v2 v3
ZADD cdc_zset_db0 1 v1 2 v2 3 v3
XADD cdc_stream_db0 1-0 f1 v1 f2 v2
HSET snapshot_hash_db0 f2 v2_updated f3 v3
LSET snapshot_list_db0 0 v1_updated
SREM snapshot_set_db0 v2
ZINCRBY snapshot_zset_db0 5 v1
XADD snapshot_stream_db0 2-0 f3 v3
DEL snapshot_db0

SELECT 2
SET cdc_db2 cdc_db2
HSET cdc_hash_db2 f1 v1 f2 v2
LPUSH cdc_list_db2 v3 v2 v1
SADD cdc_set_db2 v1 v2 v3
ZADD cdc_zset_db2 1 v1 2 v2 3 v3
XADD cdc_stream_db2 1-0 f1 v1 f2 v2
HDEL snapshot_hash_db2 f1
RPOP snapshot_list_db2
SADD snapshot_set_db2 v4
ZREM snapshot_zset_db2 v2
XADD snapshot_stream_db2 2-0 f3 v3
DEL snapshot_db2
