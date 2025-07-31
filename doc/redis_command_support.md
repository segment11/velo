# Redis command support by category

## bitmap

- [√] BITCOUNT
- [x] BITFIELD
- [x] BITFIELD_RO
- [x] BITOP
- [√] BITPOS
- [√] GETBIT
- [√] SETBIT

## bloom filter

- [√] BF.ADD
- [√] BF.CARD
- [√] BF.EXISTS
- [√] BF.INFO
- [√] BF.INSERT
- [√] BF.LOADCHUNK
- [√] BF.MADD
- [√] BF.MEXISTS
- [√] BF.RESERVE
- [√] BF.SCANDUMP

## cluster management

- [√] CLUSTER ADDSLOTS
- [√] CLUSTER ADDSLOTSRANGE
- [x] CLUSTER BUMPEPOCH
- [x] CLUSTER COUNT-FAILURE-REPORTS
- [√] CLUSTER COUNTKEYSINSLOT
- [√] CLUSTER DELSLOTS
- [√] CLUSTER DELSLOTSRANGE
- [x] CLUSTER FAILOVER
- [x] CLUSTER FLUSHSLOTS
- [x] CLUSTER FORGET
- [√] CLUSTER GETKEYSINSLOT
- [√] CLUSTER INFO
- [√] CLUSTER KEYSLOT
- [x] CLUSTER LINKS
- [x] CLUSTER MEET
- [√] CLUSTER MYID
- [√] CLUSTER MYSHARDID
- [√] CLUSTER NODES
- [√] CLUSTER REPLICAS
- [x] CLUSTER REPLICATE
- [√] CLUSTER RESET
- [√] CLUSTER SAVECONFIG
- [x] CLUSTER SET-CONFIG-EPOCH
- [√] CLUSTER SETSLOT
- [√] CLUSTER SHARDS
- [√] CLUSTER SLAVES
- [√] CLUSTER SLOTS

## connection management

- [√] AUTH
- [√] CLIENT GETNAME
- [√] CLIENT ID
- [√] CLIENT REPLY
- [√] CLIENT SETNAME
- [√] ECHO
- [√] HELLO
- [√] PING
- [√] QUIT
- [x] RESET
- [x] SELECT

## generic

- [√] COPY
- [√] DEL
- [x] DUMP
- [√] EXISTS
- [√] EXPIRE
- [√] EXPIREAT
- [√] EXPIRETIME
- [√] KEYS
- [x] MIGRATE
- [x] MOVE
- [x] OBJECT
- [x] PERSIST
- [√] PEXPIRE
- [√] PEXPIREAT
- [√] PEXPIRETIME
- [√] PTTL
- [√] RANDOMKEY
- [√] RENAME
- [√] RENAMENX
- [x] RESTORE
- [√] SCAN
- [√] SORT
- [√] SORT_RO
- [√] TOUCH
- [√] TTL
- [√] TYPE
- [x] UNLINK
- [x] WAIT
- [x] WAITAOF

## geo

- [√] GEOADD
- [√] GEODIST
- [√] GEOHASH
- [√] GEOPOS
- [x] GEORADIUS
- [x] GEORADIUS_RO
- [x] GEORADIUSBYMEMBER
- [x] GEORADIUSBYMEMBER_RO
- [√] GEOSEARCH
- [√] GEOSEARCHSTORE

## hash

- [√] HDEL
- [√] HEXISTS
- [√] HEXPIRE
- [√] HEXPIREAT
- [√] HEXPIRETIME
- [√] HGET
- [√] HGETALL
- [√] HINCRBY
- [√] HINCRBYFLOAT
- [√] HKEYS
- [√] HLEN
- [√] HMGET
- [√] HMSET
- [√] HPERSIST
- [√] HPEXPIRE
- [√] HPEXPIREAT
- [√] HPEXPIRETIME
- [√] HPTTL
- [√] HRANDFIELD
- [√] HSET
- [√] HSETNX
- [√] HSTRLEN
- [√] HTTL
- [√] HVALS

## hyperloglog

- [√] PFADD
- [√] PFCOUNT
- [x] PFDEBUG
- [√] PFMERGE
- [x] PFSELFTEST

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

## pub/sub

- [√] PUBLISH
- [√] PUBSUB
- [√] SUBSCRIBE
- [√] UNSUBSCRIBE
- [√] PSUBSCRIBE
- [√] PUNSUBSCRIBE

## server management

- [√] ACL CAT
- [√] ACL DELUSER
- [√] ACL DRYRUN
- [√] ACL GETUSER
- [√] ACL LIST
- [√] ACL LOAD
- [√] ACL LOG
- [√] ACL SAVE
- [√] ACL SETUSER
- [√] ACL USERS
- [√] ACL WHOAMI
- [x] BGREWRITEAOF
- [√] BGSAVE
- [x] COMMAND
- [x] CONFIG
- [√] DBSIZE
- [√] FAILOVER
- [√] FLUSHALL
- [√] FLUSHDB
- [√] INFO
- [√] LASTSAVE
- [x] LATENCY
- [√] ROLE
- [√] SAVE
- [√] TIME

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
- [√] MSETNX
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

TIPS:
Velo cluster use fail over manager to manage fail over, use segment_kvrocks_controller to manage slots migration.
Refer to class io.velo.command.AGroup - io.velo.command.ZGroup for all supported commands.
