SET cdc_without_select_db0 cdc_db0
HSET cdc_without_select_hash_db0 f1 v1 f2 v2
LPUSH cdc_without_select_list_db0 v3 v2 v1
SADD cdc_without_select_set_db0 v1 v2 v3
ZADD cdc_without_select_zset_db0 1 v1 2 v2 3 v3
XADD cdc_without_select_stream_db0 1-0 f1 v1 f2 v2
HSET aaa_snapshot_hash_db0 f2 v2_updated f3 v3
LSET aaa_snapshot_list_db0 0 v1_updated
SREM aaa_snapshot_set_db0 v2
ZINCRBY aaa_snapshot_zset_db0 5 v1
XADD aaa_snapshot_stream_db0 2-0 f3 v3
DEL aaa_snapshot_db0

SELECT 1
SET cdc_db1 cdc_db1
HSET cdc_hash_db1 f1 v1 f2 v2
LPUSH cdc_list_db1 v3 v2 v1
SADD cdc_set_db1 v1 v2 v3
ZADD cdc_zset_db1 1 v1 2 v2 3 v3
XADD cdc_stream_db1 1-0 f1 v1 f2 v2
HDEL zzz_snapshot_hash_db1 f1
RPOP zzz_snapshot_list_db1
SADD zzz_snapshot_set_db1 v4
ZREM zzz_snapshot_zset_db1 v2
XADD zzz_snapshot_stream_db1 2-0 f3 v3

SELECT 2
SET cdc_db2 cdc_db2
HSET cdc_hash_db2 f1 v1 f2 v2
LPUSH cdc_list_db2 v3 v2 v1
SADD cdc_set_db2 v1 v2 v3
ZADD cdc_zset_db2 1 v1 2 v2 3 v3
XADD cdc_stream_db2 1-0 f1 v1 f2 v2
DEL zzz_snapshot_db2
