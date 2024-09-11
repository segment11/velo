package io.velo.tools;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;

import java.util.HashMap;
import java.util.Map;

public class ScanAndFlattenAndThenSetToAnotherRedis {
    public static void main(String[] args) {
        for (int i = 0; i < 16; i++) {
            System.out.println("Scanning DB " + i);
            scanAndLoad(i, 0);
            System.out.println("Scanning done DB " + i);
        }
    }

    private static void scanAndLoad(int selectDbIndex, int maxCount) {
        var jedis = new Jedis("localhost", 6379);
        jedis.select(selectDbIndex);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 6380, jedisClientConfig);

        var scanParams = new ScanParams().count(1000);
        var result = jedis.scan("0", scanParams);

        long count = 0;

        String cursor = "";
        boolean finished = false;
        while (!finished) {
            var list = result.getResult();
            if (list == null || list.isEmpty()) {
                finished = true;
            }

            for (var key : list) {
                var keyType = jedis.type(key);
                Object rawValue = null;
                try {
                    if (keyType.equals("string")) {
                        var value = jedis.get(key);
                        rawValue = value;

                        if (value.getBytes().length > Short.MAX_VALUE) {
                            System.out.println("Type string, Key=" + key + " Value=" + value + ", Error=" + "Value is too long");
                            continue;
                        }

                        jedisTo.set(key, value);
                    } else if (keyType.equals("list")) {
                        var listValue = jedis.lrange(key, 0, -1);
                        rawValue = listValue;

                        // flatten list
                        for (int i = 0; i < listValue.size(); i++) {
                            jedisTo.set(key + "_" + i, listValue.get(i));
                        }
                    } else if (keyType.equals("set")) {
                        var setValue = jedis.smembers(key);
                        rawValue = setValue;

                        // flatten set
                        int i = 0;
                        for (var value : setValue) {
                            jedisTo.set(key + "_" + i, value);
                            i++;
                        }
                    } else if (keyType.equals("zset")) {
                        var zsetValue = jedis.zrangeWithScores(key, 0, -1);
                        rawValue = zsetValue;

                        Map<String, Double> scoreMembers = new HashMap<>();
                        for (var value : zsetValue) {
                            scoreMembers.put(value.getElement(), value.getScore());
                        }
                        jedisTo.zadd(key, scoreMembers);
                    } else if (keyType.equals("hash")) {
                        var hashValue = jedis.hgetAll(key);
                        rawValue = hashValue;

                        // flatten hash
                        for (var entry : hashValue.entrySet()) {
                            jedisTo.set(key + "_" + entry.getKey(), entry.getValue());
                        }
                    } else {
                        System.out.println("Key=" + key + " Value=" + "Unknown type=" + keyType);
                    }
                    count++;

                    if (count % 100 == 0) {
                        System.out.println("Processed " + count + " keys");
                    }

                    if (maxCount > 0 && count >= maxCount) {
                        finished = true;
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Key=" + key + " Value=" + rawValue + ", Error=" + e.getMessage());
                    throw e;
                }
            }

            cursor = result.getCursor();
            if (cursor.equalsIgnoreCase("0")) {
                finished = true;
            }
            result = jedis.scan(cursor);
        }

        System.out.println("Processed " + count + " keys");
    }
}
