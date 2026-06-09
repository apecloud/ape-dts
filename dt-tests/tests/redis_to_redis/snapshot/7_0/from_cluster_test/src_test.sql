SET cluster_string_key value_1
HSET cluster_hash_key field_1 value_1 field_2 value_2
LPUSH cluster_list_key value_1 value_2 value_3
SADD cluster_set_key value_1 value_2 value_3
ZADD cluster_zset_key 1 value_1 2 value_2
XADD cluster_stream_key * field_1 value_1 field_2 value_2
SET cluster_expire_key expire_value
PEXPIRE cluster_expire_key 60000
