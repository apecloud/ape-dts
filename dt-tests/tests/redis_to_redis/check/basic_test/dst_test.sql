SET equal:string value
SET diff:string other
HSET equal:hash f value
HSET diff:hash f other
RPUSH equal:list a b c
RPUSH diff:list a x c
SADD equal:set a b c
SADD diff:set a b x
ZADD equal:zset 1 a 2 b
ZADD diff:zset 1 a 3 b
XADD equal:stream 1-0 f value
XADD diff:stream 1-0 f other
RPUSH diff:type value
SET diff:ttl-state value
SET diff:ttl-time value
PEXPIREAT diff:ttl-time 4102444810000
SET diff:value-and-ttl target
XADD stream:metadata 1-0 f v
XSETID stream:metadata 8-0
XADD stream:empty 1-0 f v
XDEL stream:empty 1-0
SET target-only value
