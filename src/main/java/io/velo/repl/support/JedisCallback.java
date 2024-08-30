package io.velo.repl.support;

import redis.clients.jedis.Jedis;

public interface JedisCallback<R> {
    R call(Jedis jedis);
}
