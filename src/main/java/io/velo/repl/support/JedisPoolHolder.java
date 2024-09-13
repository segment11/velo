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

public class JedisPoolHolder implements NeedCleanUp {
    // singleton
    private JedisPoolHolder() {
    }

    private static final JedisPoolHolder instance = new JedisPoolHolder();

    public static JedisPoolHolder getInstance() {
        return instance;
    }

    private final Map<String, JedisPool> cached = new HashMap<>();

    private static final Logger log = LoggerFactory.getLogger(JedisPoolHolder.class);

    public synchronized JedisPool create(String host, int port) {
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

    @Override
    public void cleanUp() {
        for (var pool : cached.values()) {
            pool.close();
            System.out.println("Close jedis pool");
        }
        cached.clear();
    }

    public static <R> R exe(JedisPool jedisPool, JedisCallback<R> callback) {
        Jedis jedis = jedisPool.getResource();
        try {
            return callback.call(jedis);
        } finally {
            jedis.close();
        }
    }
}
