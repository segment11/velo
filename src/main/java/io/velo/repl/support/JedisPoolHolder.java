package io.velo.repl.support;

import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * A singleton class that manages and caches Jedis pools. This class provides methods to create and retrieve
 * Jedis pools for a given host and port, and ensures that resources are properly cleaned up.
 *
 * @since 1.0.0
 */
public class JedisPoolHolder implements NeedCleanUp {
    // Singleton instance
    private JedisPoolHolder() {
    }

    private static final JedisPoolHolder instance = new JedisPoolHolder();

    /**
     * Returns the singleton instance of JedisPoolHolder.
     *
     * @return the singleton instance of JedisPoolHolder
     * @since 1.0.0
     */
    public static JedisPoolHolder getInstance() {
        return instance;
    }

    private final Map<String, JedisPool> cached = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(JedisPoolHolder.class);

    /**
     * Creates a Jedis pool for the specified host and port. If a pool already exists for the given host and port,
     * it returns the existing pool.
     *
     * @param host the Redis server host
     * @param port the Redis server port
     * @return the Jedis pool for the specified host and port
     * @since 1.0.0
     */
    public synchronized JedisPool createIfNotCached(String host, int port) {
        var key = host + ":" + port;
        var client = cached.get(key);
        if (client != null) {
            return client;
        }

        var conf = new JedisPoolConfig();
        conf.setMaxTotal(ConfForGlobal.jedisPoolMaxTotal);
        conf.setMaxIdle(ConfForGlobal.jedisPoolMaxIdle);
        conf.setMaxWait(Duration.ofMillis(ConfForGlobal.jedisPoolMaxWaitMillis));

        conf.setTestOnBorrow(true);
        conf.setTestOnCreate(true);
        conf.setTestOnReturn(true);
        conf.setTestWhileIdle(true);

        var one = new JedisPool(conf, host, port, ConfForGlobal.JEDIS_POOL_CONNECT_TIMEOUT_MILLIS, ConfForGlobal.PASSWORD);
        log.info("Create jedis pool for {}:{}", host, port);
        cached.put(key, one);
        return one;
    }

    /**
     * Cleans up all cached Jedis pools by closing them and clearing the cache.
     *
     * @since 1.0.0
     */
    @Override
    public void cleanUp() {
        for (var pool : cached.values()) {
            pool.close();
            log.info("Close jedis pool");
        }
        cached.clear();
    }

    /**
     * Executes a callback using the provided Jedis pool. This method ensures that the Jedis resource is properly
     * closed after the callback execution.
     *
     * @param jedisPool the Jedis pool to use
     * @param callback  the callback to execute
     * @param <R>       the type of result returned by the callback
     * @return the result of the callback execution
     * @since 1.0.0
     */
    public static <R> R exe(JedisPool jedisPool, JedisCallback<R> callback) {
        try (Jedis jedis = jedisPool.getResource()) {
            return callback.call(jedis);
        }
    }
}