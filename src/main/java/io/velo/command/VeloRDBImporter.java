package io.velo.command;

import io.netty.buffer.ByteBuf;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

public class VeloRDBImporter implements RDBImporter {
    public static boolean DEBUG = false;

    @Override
    public void restore(ByteBuf buf, RDBCallback callback) {
        if (DEBUG) {
            callback.onInteger(123);
            callback.onString("test_value".getBytes());

            var rhh = new RedisHH();
            rhh.put("test_key", "test_value".getBytes());
            callback.onHash(rhh);

            var rl = new RedisList();
            rl.addLast("abc".getBytes());
            rl.addLast("xyz".getBytes());
            callback.onList(rl);

            var rhk = new RedisHashKeys();
            rhk.add("member0");
            rhk.add("member1");
            callback.onSet(rhk);

            var rz = new RedisZSet();
            rz.add(1.0, "member0");
            rz.add(2.0, "member1");
            callback.onZSet(rz);

            return;
        }

        // decode robj encoded todo
    }
}
