package io.velo.repl.support;

import redis.clients.jedis.Jedis;

/**
 * Callback interface for Jedis operations.
 *
 * @param <R> the result type
 */
public interface JedisCallback<R> {
    /**
     * @param jedis the Jedis client
     * @return the result of the operation
     */
    R call(Jedis jedis);
}