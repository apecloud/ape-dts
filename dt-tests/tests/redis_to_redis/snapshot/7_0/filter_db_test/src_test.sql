SET 0-0 0
SET 0-1 1

SELECT 1
SET 1-0 0
HSET 1-1 field1 "hello" field2 "world"
RPUSH 1-2 "Hello"
SADD 1-3 "Hello"
ZADD 1-4 0 a 1 b 2 c
XADD 1-5 1526919030474-55 message "Hello,"

SELECT 2
SET 2-0 0
SET 2-1 1

SELECT 3
SET 3-0 0
SET 3-1 1

SELECT 4
SET 4-0 0
SET 4-1 1