# Redis command support

## string

- [√] APPEND
- [√] COPY
- [√] DECR
- [√] DECRBY
- [√] DECRBYFLOAT
- [√] DEL
- [√] EXISTS
- [√] EXPIRE
- [√] EXPIREAT
- [√] EXPIRETIME
- [√] GET
- [√] GETDEL
- [√] GETEX
- [√] GETRANGE
- [√] GETSET
- [√] INCR
- [√] INCRBY
- [√] INCRBYFLOAT
- [√] MGET
- [√] MSET
- [√] PEXPIRE
- [√] PEXPIREAT
- [√] PEXPIRETIME
- [√] PTTL
- [√] PSETEX
- [√] RENAME
- [√] SET
- [√] SETEX
- [√] SETNX
- [√] SETRANGE
- [√] STRLEN
- [√] TYPE
- [√] TTL

## hash

- [√] HDEL
- [√] HEXISTS
- [√] HGET
- [√] HGETALL
- [√] HINCRBY
- [√] HINCRBYFLOAT
- [√] HKEYS
- [√] HLEN
- [√] HMGET
- [√] HMSET
- [√] HRANDFIELD
- [√] HSET
- [√] HSETNX
- [√] HSTRLEN
- [√] HVALS

## list

- [√] LINDEX
- [√] LINSERT
- [√] LLEN
- [√] LMOVE
- [√] LPOP
- [√] LPOS
- [√] LPUSH
- [√] LPUSHX
- [√] LRANGE
- [√] LREM
- [√] LSET
- [√] LTRIM
- [√] RPOPLPUSH
- [√] RPOP
- [√] RPUSH
- [√] RPUSHX

## set

- [√] SADD
- [√] SCARD
- [√] SDIFF
- [√] SDIFFSTORE
- [√] SINTER
- [√] SINTERCARD
- [√] SINTERSTORE
- [√] SISMEMBER
- [√] SMEMBERS
- [√] SMISMEMBER
- [√] SMOVE
- [√] SPOP
- [√] SRANDMEMBER
- [√] SREM
- [√] SUNION
- [√] SUNIONSTORE

## sorted set

- [√] ZADD
- [√] ZCARD
- [√] ZCOUNT
- [√] ZDIFF
- [√] ZDIFFSTORE
- [√] ZINCRBY
- [√] ZINTER
- [√] ZINTERCARD
- [√] ZINTERSTORE
- [√] ZLEXCOUNT
- [√] ZMSCORE
- [√] ZPOPMAX
- [√] ZPOPMIN
- [√] ZRANDMEMBER
- [√] ZRANGE
- [√] ZRANGEBYLEX
- [√] ZRANGEBYSCORE
- [√] ZRANGESTORE
- [√] ZRANK
- [√] ZREM
- [√] ZREMRANGEBYLEX
- [√] ZREMRANGEBYRANK
- [√] ZREMRANGEBYSCORE
- [√] ZREVRANGE
- [√] ZREVRANGEBYLEX
- [√] ZREVRANGEBYSCORE
- [√] ZREVRANK
- [√] ZSCORE
- [√] ZUNION
- [√] ZUNIONSTORE

TIPS:
Refer to class io.velo.command.AGroup - io.velo.command.ZGroup for all supported commands.
