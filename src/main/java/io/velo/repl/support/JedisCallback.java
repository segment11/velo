package io.velo.repl.support;

import redis.clients.jedis.Jedis;

/**
 * Represents a callback interface for operations that need to be performed
 * using a Jedis client. Implementations of this interface define a method
 * that takes a Jedis instance and returns a result of type <code>R</code>.
 *
 * @param <R> the type of the result that the callback returns
 * @since 1.0.0
 */
public interface JedisCallback<R> {
    /**
     * Executes the operation using the provided Jedis client.
     *
     * @param jedis the Jedis client instance to be used for the operation
     * @return the result of the operation of type <code>R</code>
     * @throws RuntimeException Any exceptions thrown during the execution of the operation.
     * @since 1.0.0
     */
    R call(Jedis jedis);
}