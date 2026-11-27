SET equal:string value
SET diff:string value
HSET equal:hash f value
HSET diff:hash f value
RPUSH equal:list a b c
RPUSH diff:list a b c
SADD equal:set a b c
SADD diff:set a b c
ZADD equal:zset 1 a 2 b
ZADD diff:zset 1 a 2 b
XADD equal:stream 1-0 f value
XADD diff:stream 1-0 f value
SET diff:type value
SET diff:ttl-state value
PEXPIREAT diff:ttl-state 4102444800000
SET diff:ttl-time value
PEXPIREAT diff:ttl-time 4102444800000
SET diff:value-and-ttl source
PEXPIREAT diff:value-and-ttl 4102444800000
SET missing value
XADD stream:metadata 1-0 f v
XSETID stream:metadata 9-0
XADD stream:empty 1-0 f v
XDEL stream:empty 1-0
