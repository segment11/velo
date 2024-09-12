package io.velo;

import org.apache.commons.io.FileUtils;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

public class TestCRC16DataSkew {
    public static void main(String[] args) throws IOException {
        int number = 100000000;

        TreeMap<Integer, Integer> map = new TreeMap<>();
        for (int i = 0; i < number; i++) {
            // like redis-benchmark key generator
            var key = "key:" + Utils.leftPad(String.valueOf(i), "0", 12);
            var keyBytes = key.getBytes();

            var crc16 = JedisClusterCRC16.getCRC16(keyBytes);
            map.putIfAbsent(crc16, 0);
            map.computeIfPresent(crc16, (k, v) -> v + 1);
        }

        var file = new File("crc16KeyCount.txt");
        FileUtils.touch(file);
        var writer = new FileWriter(file);
        writer.write("--- crc16 key count ---\n");

        for (var entry : map.entrySet()) {
            writer.write("crc16=" + entry.getKey() + " count=" + entry.getValue() + "\n");
        }

        writer.close();
    }
}
