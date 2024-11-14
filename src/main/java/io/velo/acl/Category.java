package io.velo.acl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum Category {
    all, admin, bitmap, blocking, connection, dangerous,
    geo, hash, hyperloglog, fast, keyspace,
    list, pubsub, read, scripting,
    set, sortedset, slow, stream, string,
    transaction, write;

    private static final Map<Category, List<String>> CMD_LIST_BY_CATEGORY = new HashMap<>();
    private static final Map<String, List<Category>> CATEGORY_LIST_BY_CMD = new HashMap<>();

    static {
        List<String> adminCmdList = List.of(
                "acl",
                "bgrewriteaof",
                "bgsave",
                "client",
                "cluster",
                "config",
                "debug",
                "failover",
                "lastsave",
                "latency",
                "module",
                "monitor",
                "pfdebug",
                "pfselftest",
                "psync",
                "replconf",
                "replicaof",
                "save",
                "shutdown",
                "slaveof",
                "slowlog",
                "sync"
        );
        CMD_LIST_BY_CATEGORY.put(admin, adminCmdList);
        for (var cmd : adminCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(admin);
        }

        List<String> bitmapCmdList = List.of(
                "bitcount",
                "bitfield",
                "bitfield_ro",
                "bitop",
                "bitpos",
                "getbit",
                "setbit"
        );
        CMD_LIST_BY_CATEGORY.put(bitmap, bitmapCmdList);
        for (var cmd : bitmapCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(bitmap);
        }

        List<String> blockingCmdList = List.of(
                "blmove",
                "blpop",
                "brpop",
                "brpoplpush",
                "bzpopmax",
                "bzpopmin",
                "xread",
                "xreadgroup"
        );
        CMD_LIST_BY_CATEGORY.put(blocking, blockingCmdList);
        for (var cmd : blockingCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(blocking);
        }

        List<String> connectionCmdList = List.of(
                "auth",
                "client",
                "command",
                "echo",
                "hello",
                "ping",
                "reset"
        );
        CMD_LIST_BY_CATEGORY.put(connection, connectionCmdList);
        for (var cmd : connectionCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(connection);
        }

        List<String> dangerousCmdList = List.of(
                "acl",
                "bgrewriteaof",
                "bgsave",
                "client",
                "cluster",
                "config",
                "debug",
                "failover",
                "flushall",
                "flushdb",
                "info",
                "keys",
                "lastsave",
                "latency",
                "migrate",
                "module",
                "monitor",
                "pfdebug",
                "pfselftest",
                "psync",
                "replconf",
                "replicaof",
                "restore",
                "restore-asking",
                "role",
                "save",
                "shutdown",
                "slaveof",
                "slowlog",
                "sort",
                "swapdb",
                "sync"
        );
        CMD_LIST_BY_CATEGORY.put(dangerous, dangerousCmdList);
        for (var cmd : dangerousCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(dangerous);
        }

        List<String> geoCmdList = List.of(
                "geoadd",
                "geodist",
                "geohash",
                "geopos",
                "georadius",
                "georadius_ro",
                "georadiusbymember",
                "georadiusbymember_ro",
                "geosearch",
                "geosearchstore"
        );
        CMD_LIST_BY_CATEGORY.put(geo, geoCmdList);
        for (var cmd : geoCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(geo);
        }

        List<String> hashCmdList = List.of(
                "hdel",
                "hexists",
                "hget",
                "hgetall",
                "hincrby",
                "hincrbyfloat",
                "hkeys",
                "hlen",
                "hmget",
                "hmset",
                "hrandfield",
                "hscan",
                "hset",
                "hsetnx",
                "hstrlen",
                "hvals"
        );
        CMD_LIST_BY_CATEGORY.put(hash, hashCmdList);
        for (var cmd : hashCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(hash);
        }

        List<String> hyperloglogCmdList = List.of(
                "pfadd",
                "pfcount",
                "pfdebug",
                "pfmerge",
                "pfselftest"
        );
        CMD_LIST_BY_CATEGORY.put(hyperloglog, hyperloglogCmdList);
        for (var cmd : hyperloglogCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(hyperloglog);
        }

        List<String> fastCmdList = List.of(
                "append",
                "asking",
                "auth",
                "bitfield_ro",
                "bzpopmax",
                "bzpopmin",
                "dbsize",
                "decr",
                "decrby",
                "discard",
                "echo",
                "exists",
                "expire",
                "expireat",
                "get",
                "getbit",
                "getdel",
                "getex",
                "getset",
                "hdel",
                "hello",
                "hexists",
                "hget",
                "hincrby",
                "hincrbyfloat",
                "hlen",
                "hmget",
                "hmset",
                "hset",
                "hsetnx",
                "hstrlen",
                "incr",
                "incrby",
                "incrbyfloat",
                "lastsave",
                "llen",
                "lolwut",
                "lpop",
                "lpush",
                "lpushx",
                "mget",
                "move",
                "multi",
                "persist",
                "pexpire",
                "pexpireat",
                "pfadd",
                "ping",
                "pttl",
                "publish",
                "readonly",
                "readwrite",
                "renamenx",
                "reset",
                "role",
                "rpop",
                "rpush",
                "rpushx",
                "sadd",
                "scard",
                "select",
                "setnx",
                "sismember",
                "smismember",
                "smove",
                "spop",
                "srem",
                "strlen",
                "swapdb",
                "time",
                "touch",
                "ttl",
                "type",
                "unlink",
                "unwatch",
                "watch",
                "xack",
                "xadd",
                "xautoclaim",
                "xclaim",
                "xdel",
                "xlen",
                "xsetid",
                "zadd",
                "zcard",
                "zcount",
                "zincrby",
                "zlexcount",
                "zmscore",
                "zpopmax",
                "zpopmin",
                "zrank",
                "zrem",
                "zrevrank",
                "zscore"
        );
        CMD_LIST_BY_CATEGORY.put(fast, fastCmdList);
        for (var cmd : fastCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(fast);
        }

        List<String> keyspaceCmdList = List.of(
                "asking",
                "copy",
                "dbsize",
                "del",
                "dump",
                "exists",
                "expire",
                "expireat",
                "flushall",
                "flushdb",
                "keys",
                "migrate",
                "move",
                "object",
                "persist",
                "pexpire",
                "pexpireat",
                "pttl",
                "randomkey",
                "readonly",
                "readwrite",
                "rename",
                "renamenx",
                "restore",
                "restore-asking",
                "scan",
                "select",
                "swapdb",
                "touch",
                "ttl",
                "type",
                "unlink",
                "wait"
        );
        CMD_LIST_BY_CATEGORY.put(keyspace, keyspaceCmdList);
        for (var cmd : keyspaceCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(keyspace);
        }

        List<String> listCmdList = List.of(
                "blmove",
                "blpop",
                "brpop",
                "brpoplpush",
                "lindex",
                "linsert",
                "llen",
                "lmove",
                "lpop",
                "lpos",
                "lpush",
                "lpushx",
                "lrange",
                "lrem",
                "lset",
                "ltrim",
                "rpop",
                "rpoplpush",
                "rpush",
                "rpushx",
                "sort"
        );
        CMD_LIST_BY_CATEGORY.put(list, listCmdList);
        for (var cmd : listCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(list);
        }

        List<String> pubsubCmdList = List.of(
                "psubscribe",
                "publish",
                "pubsub",
                "punsubscribe",
                "subscribe",
                "unsubscribe"
        );
        CMD_LIST_BY_CATEGORY.put(pubsub, pubsubCmdList);
        for (var cmd : pubsubCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(pubsub);
        }

        List<String> readCmdList = List.of(
                "bitcount",
                "bitfield_ro",
                "bitpos",
                "dbsize",
                "dump",
                "exists",
                "geodist",
                "geohash",
                "geopos",
                "georadius_ro",
                "georadiusbymember_ro",
                "geosearch",
                "get",
                "getbit",
                "getrange",
                "hexists",
                "hget",
                "hgetall",
                "hkeys",
                "hlen",
                "hmget",
                "host:",
                "hrandfield",
                "hscan",
                "hstrlen",
                "hvals",
                "keys",
                "lindex",
                "llen",
                "lolwut",
                "lpos",
                "lrange",
                "memory",
                "mget",
                "object",
                "pfcount",
                "post",
                "pttl",
                "randomkey",
                "scan",
                "scard",
                "sdiff",
                "sinter",
                "sismember",
                "smembers",
                "smismember",
                "srandmember",
                "sscan",
                "stralgo",
                "strlen",
                "substr",
                "sunion",
                "touch",
                "ttl",
                "type",
                "xinfo",
                "xlen",
                "xpending",
                "xrange",
                "xread",
                "xrevrange",
                "zcard",
                "zcount",
                "zdiff",
                "zinter",
                "zlexcount",
                "zmscore",
                "zrandmember",
                "zrange",
                "zrangebylex",
                "zrangebyscore",
                "zrank",
                "zrevrange",
                "zrevrangebylex",
                "zrevrangebyscore",
                "zrevrank",
                "zscan",
                "zscore",
                "zunion"
        );
        CMD_LIST_BY_CATEGORY.put(read, readCmdList);
        for (var cmd : readCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(read);
        }

        List<String> scriptingCmdList = List.of(
                "eval",
                "evalsha",
                "script"
        );
        CMD_LIST_BY_CATEGORY.put(scripting, scriptingCmdList);
        for (var cmd : scriptingCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(scripting);
        }

        List<String> setCmdList = List.of(
                "sadd",
                "scard",
                "sdiff",
                "sdiffstore",
                "sinter",
                "sinterstore",
                "sismember",
                "smembers",
                "smismember",
                "smove",
                "sort",
                "spop",
                "srandmember",
                "srem",
                "sscan",
                "sunion",
                "sunionstore"
        );
        CMD_LIST_BY_CATEGORY.put(set, setCmdList);
        for (var cmd : setCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(set);
        }

        List<String> sortedsetCmdList = List.of(
                "bzpopmax",
                "bzpopmin",
                "sort",
                "zadd",
                "zcard",
                "zcount",
                "zdiff",
                "zdiffstore",
                "zincrby",
                "zinter",
                "zinterstore",
                "zlexcount",
                "zmscore",
                "zpopmax",
                "zpopmin",
                "zrandmember",
                "zrange",
                "zrangebylex",
                "zrangebyscore",
                "zrangestore",
                "zrank",
                "zrem",
                "zremrangebylex",
                "zremrangebyrank",
                "zremrangebyscore",
                "zrevrange",
                "zrevrangebylex",
                "zrevrangebyscore",
                "zrevrank",
                "zscan",
                "zscore",
                "zunion",
                "zunionstore"
        );
        CMD_LIST_BY_CATEGORY.put(sortedset, sortedsetCmdList);
        for (var cmd : sortedsetCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(sortedset);
        }

        List<String> slowCmdList = List.of(
                "acl",
                "bgrewriteaof",
                "bgsave",
                "bitcount",
                "bitfield",
                "bitop",
                "bitpos",
                "blmove",
                "blpop",
                "brpop",
                "brpoplpush",
                "client",
                "cluster",
                "command",
                "config",
                "copy",
                "debug",
                "del",
                "dump",
                "eval",
                "evalsha",
                "exec",
                "failover",
                "flushall",
                "flushdb",
                "geoadd",
                "geodist",
                "geohash",
                "geopos",
                "georadius",
                "georadius_ro",
                "georadiusbymember",
                "georadiusbymember_ro",
                "geosearch",
                "geosearchstore",
                "getrange",
                "hgetall",
                "hkeys",
                "host:",
                "hrandfield",
                "hscan",
                "hvals",
                "info",
                "keys",
                "latency",
                "lindex",
                "linsert",
                "lmove",
                "lpos",
                "lrange",
                "lrem",
                "lset",
                "ltrim",
                "memory",
                "migrate",
                "module",
                "monitor",
                "mset",
                "msetnx",
                "object",
                "pfcount",
                "pfdebug",
                "pfmerge",
                "pfselftest",
                "post",
                "psetex",
                "psubscribe",
                "psync",
                "pubsub",
                "punsubscribe",
                "randomkey",
                "rename",
                "replconf",
                "replicaof",
                "restore",
                "restore-asking",
                "rpoplpush",
                "save",
                "scan",
                "script",
                "sdiff",
                "sdiffstore",
                "set",
                "setbit",
                "setex",
                "setrange",
                "shutdown",
                "sinter",
                "sinterstore",
                "slaveof",
                "slowlog",
                "smembers",
                "sort",
                "srandmember",
                "sscan",
                "stralgo",
                "subscribe",
                "substr",
                "sunion",
                "sunionstore",
                "sync",
                "unsubscribe",
                "wait",
                "xgroup",
                "xinfo",
                "xpending",
                "xrange",
                "xread",
                "xreadgroup",
                "xrevrange",
                "xtrim",
                "zdiff",
                "zdiffstore",
                "zinter",
                "zinterstore",
                "zrandmember",
                "zrange",
                "zrangebylex",
                "zrangebyscore",
                "zrangestore",
                "zremrangebylex",
                "zremrangebyrank",
                "zremrangebyscore",
                "zrevrange",
                "zrevrangebylex",
                "zrevrangebyscore",
                "zscan",
                "zunion",
                "zunionstore"
        );
        CMD_LIST_BY_CATEGORY.put(slow, slowCmdList);
        for (var cmd : slowCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(slow);
        }

        List<String> streamCmdList = List.of(
                "xack",
                "xadd",
                "xautoclaim",
                "xclaim",
                "xdel",
                "xgroup",
                "xinfo",
                "xlen",
                "xpending",
                "xrange",
                "xread",
                "xreadgroup",
                "xrevrange",
                "xsetid",
                "xtrim"
        );
        CMD_LIST_BY_CATEGORY.put(stream, streamCmdList);
        for (var cmd : streamCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(stream);
        }

        List<String> stringCmdList = List.of(
                "append",
                "decr",
                "decrby",
                "get",
                "getdel",
                "getex",
                "getrange",
                "getset",
                "incr",
                "incrby",
                "incrbyfloat",
                "mget",
                "mset",
                "msetnx",
                "psetex",
                "set",
                "setex",
                "setnx",
                "setrange",
                "stralgo",
                "strlen",
                "substr"
        );
        CMD_LIST_BY_CATEGORY.put(string, stringCmdList);
        for (var cmd : stringCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(string);
        }

        List<String> transactionCmdList = List.of(
                "discard",
                "exec",
                "multi",
                "unwatch",
                "watch"
        );
        CMD_LIST_BY_CATEGORY.put(transaction, transactionCmdList);
        for (var cmd : transactionCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(transaction);
        }

        List<String> writeCmdList = List.of(
                "append",
                "bitfield",
                "bitop",
                "blmove",
                "blpop",
                "brpop",
                "brpoplpush",
                "bzpopmax",
                "bzpopmin",
                "copy",
                "decr",
                "decrby",
                "del",
                "expire",
                "expireat",
                "flushall",
                "flushdb",
                "geoadd",
                "georadius",
                "georadiusbymember",
                "geosearchstore",
                "getdel",
                "getex",
                "getset",
                "hdel",
                "hincrby",
                "hincrbyfloat",
                "hmset",
                "hset",
                "hsetnx",
                "incr",
                "incrby",
                "incrbyfloat",
                "linsert",
                "lmove",
                "lpop",
                "lpush",
                "lpushx",
                "lrem",
                "lset",
                "ltrim",
                "migrate",
                "move",
                "mset",
                "msetnx",
                "persist",
                "pexpire",
                "pexpireat",
                "pfadd",
                "pfdebug",
                "pfmerge",
                "psetex",
                "rename",
                "renamenx",
                "restore",
                "restore-asking",
                "rpop",
                "rpoplpush",
                "rpush",
                "rpushx",
                "sadd",
                "sdiffstore",
                "set",
                "setbit",
                "setex",
                "setnx",
                "setrange",
                "sinterstore",
                "smove",
                "sort",
                "spop",
                "srem",
                "sunionstore",
                "swapdb",
                "unlink",
                "xack",
                "xadd",
                "xautoclaim",
                "xclaim",
                "xdel",
                "xgroup",
                "xreadgroup",
                "xsetid",
                "xtrim",
                "zadd",
                "zdiffstore",
                "zincrby",
                "zinterstore",
                "zpopmax",
                "zpopmin",
                "zrangestore",
                "zrem",
                "zremrangebylex",
                "zremrangebyrank",
                "zremrangebyscore",
                "zunionstore"
        );
        CMD_LIST_BY_CATEGORY.put(write, writeCmdList);
        for (var cmd : writeCmdList) {
            var categories = CATEGORY_LIST_BY_CMD.computeIfAbsent(cmd, k -> new ArrayList<>());
            categories.add(write);
        }
    }

    public static List<String> getCmdListByCategory(Category category) {
        return CMD_LIST_BY_CATEGORY.get(category);
    }

    public static List<Category> getCategoryListByCmd(String cmd) {
        return CATEGORY_LIST_BY_CMD.get(cmd);
    }
}