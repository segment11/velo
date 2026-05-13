package io.velo.command;

import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

/**
 * Callback interface for RDB import events.
 */
public interface RDBCallback {
    /**
     * Called when a string value is imported.
     *
     * @param valueBytes the string value bytes
     */
    void onString(byte[] valueBytes);

    /**
     * Called when a list value is imported.
     *
     * @param rl the Redis list
     */
    void onList(RedisList rl);

    /**
     * Called when a set value is imported.
     *
     * @param rhk the Redis hash keys (set)
     */
    void onSet(RedisHashKeys rhk);

    /**
     * Called when a sorted set value is imported.
     *
     * @param rz the Redis sorted set
     */
    void onZSet(RedisZSet rz);

    /**
     * Called when a hash value is imported.
     *
     * @param rhh the Redis hash
     */
    void onHash(RedisHH rhh);
}
