package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.StandardCharsets;

public class CharsetCompatibilityMain {
    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }

    public static void main(String[] args) {
        String member = "你好-member";

        var rhk = new RedisHashKeys();
        rhk.add(member);
        var rhkDecoded = RedisHashKeys.decode(rhk.encodeButDoNotCompress(), false);
        if (!rhkDecoded.contains(member)) {
            fail("RedisHashKeys decode lost UTF-8 member");
        }

        var rz = new RedisZSet();
        rz.add(1.0d, member);
        var rzDecoded = RedisZSet.decode(rz.encodeButDoNotCompress(), false);
        if (!rzDecoded.contains(member)) {
            fail("RedisZSet decode lost UTF-8 member");
        }

        var rg = new RedisGeo();
        rg.add(member, 13.0d, 38.0d);
        var rgDecoded = RedisGeo.decode(rg.encode(), false);
        if (!rgDecoded.contains(member)) {
            fail("RedisGeo decode lost UTF-8 member");
        }

        BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 1000, 0.01d);
        filter.put(member);
        var bytes = BFSerializer.toBytes(filter);
        var filterDecoded = BFSerializer.fromBytes(bytes, 0, bytes.length);
        if (!filterDecoded.mightContain(member)) {
            fail("BFSerializer decode lost UTF-8 member");
        }
    }
}
