package io.velo.command;

import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

public interface RDBCallback {
    void onInteger(Integer value);

    void onString(byte[] valueBytes);

    void onList(RedisList rl);

    void onSet(RedisHashKeys rhk);

    void onZSet(RedisZSet rz);

    void onHash(RedisHH rhh);
}
