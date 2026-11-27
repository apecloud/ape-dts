SET persistent value
SET expiring_string value PX 60000
HSET expiring_hash field value
PEXPIRE expiring_hash 60000
RPUSH expiring_list a b
PEXPIRE expiring_list 60000
SET short_ttl_1 value PX 3000
SET short_ttl_2 value PX 3000
SET short_ttl_3 value PX 3000
