package io.velo.tools;

import io.velo.CompressedValue;
import io.velo.DictMap;
import io.velo.repl.support.ExtendProtocolCommand;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ScanAndLoad {
    public static void main(String[] args) {
//        setOneKeyValue("car:tboxsimTocarid:", -1);

        try {
            scanAndLoad(false, -1);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Last visited key=" + lastVisitedKey + ", type=" + lastVisitedKeyType + ", value=" + lastVisitedValue);
        }

//        scanAndTrainHashFieldDicts();
    }

    private static void setOneKeyValue(String givenKey, int selectDb) throws IOException {
        final var writer = new FileWriter("set_one_key_value_output.txt", true);

        var jedis = new Jedis("localhost", 6379);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 7379, jedisClientConfig);

        if (selectDb != -1) {
            jedis.select(selectDb);
            var keyType = jedis.type(givenKey);
            setOne(keyType, givenKey, jedis, jedisTo, false, writer);
        } else {
            for (int i = 0; i < 16; i++) {
                jedis.select(i);
                if (jedis.exists(givenKey)) {
                    System.out.println("Key exists in db " + i);
                    var keyType = jedis.type(givenKey);
                    setOne(keyType, givenKey, jedis, jedisTo, false, writer);
                }
            }
        }

        jedis.close();
        jedisTo.close();

        writer.close();
    }

    private static void scanAndTrainHashFieldDicts() {
        var jedis = new Jedis("localhost", 6379);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 7379, jedisClientConfig);

        var scanParams = new ScanParams().count(1000);
        var result = jedis.scan("0", scanParams);

        HashMap<String, ArrayList<String>> fieldListByField = new HashMap<>();

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
                    if (keyType.equals("hash")) {
                        var hashValue = jedis.hgetAll(key);
                        rawValue = hashValue;

                        for (var entry : hashValue.entrySet()) {
                            var field = entry.getKey();
                            var value = entry.getValue();
                            if (value.length() >= DictMap.TO_COMPRESS_MIN_DATA_LENGTH) {
                                var fieldList = fieldListByField.computeIfAbsent(field, k -> new ArrayList<>());
                                fieldList.add(value);

                                if (fieldList.size() == 100) {
                                    var params = new String[fieldList.size() + 3];
                                    params[0] = "dict";
                                    params[1] = "train-new-dict";
                                    // key prefix
                                    params[2] = field;
                                    // sample values
                                    for (int i = 0; i < fieldList.size(); i++) {
                                        params[i + 3] = fieldList.get(i);
                                    }
                                    var trainDictResult = jedisTo.sendCommand(new ExtendProtocolCommand("manage"), params);
                                    System.out.println("Train dict result=" + trainDictResult);
                                    fieldList.clear();
                                }
                            }
                        }
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

        jedis.close();
        jedisTo.close();
    }

    private static String lastVisitedKey = null;
    private static String lastVisitedKeyType = null;
    private static Object lastVisitedValue = null;

    private static void scanAndLoad(boolean doCompare, int maxCount) throws IOException {
        var jedis = new Jedis("localhost", 6379);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 7379, jedisClientConfig);

        for (int i = 0; i < 16; i++) {
            scanAndLoad(i, jedis, jedisTo, doCompare, maxCount);
        }

        jedis.close();
        jedisTo.close();
    }

    private static long notEqualCount = 0;

    private static void scanAndLoad(int selectDb, Jedis jedis, Jedis jedisTo, boolean doCompare, int maxCount) throws IOException {
        final var writer = new FileWriter("scan_and_load_output.txt", true);

        jedis.select(selectDb);

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
                lastVisitedKey = key;
                var keyType = jedis.type(key);
                lastVisitedKeyType = keyType;

                Object rawValue = null;
                try {
                    rawValue = setOne(keyType, key, jedis, jedisTo, doCompare, writer);
                    count++;

                    if (count % 100 == 0) {
                        writer.write("Processed " + count + " keys\n");
                    }

                    if (maxCount > 0 && count >= maxCount) {
                        finished = true;
                        break;
                    }
                } catch (Exception e) {
                    writer.write("Key=" + key + " Value=" + rawValue + ", Error=" + e.getMessage() + "\n");
                    throw e;
                }
            }

            cursor = result.getCursor();
            if (cursor.equalsIgnoreCase("0")) {
                finished = true;
            }
            result = jedis.scan(cursor);
        }

        writer.write("Processed " + count + " keys, not equal count=" + notEqualCount + ", select db=" + selectDb + "\n");
        writer.close();
    }

    private static Object setOne(String keyType, String key, Jedis jedis, Jedis jedisTo, boolean doCompare, Writer writer) throws IOException {
        Object rawValue = null;
        if (keyType.equals("string")) {
            var value = jedis.get(key);
            rawValue = value;
            lastVisitedValue = value;

            if (key.length() > CompressedValue.KEY_MAX_LENGTH) {
                writer.write("Key=" + key + " Value=" + value + ", Error=" + "Key is too long\n");
                return rawValue;
            }

            if (value.getBytes().length > CompressedValue.VALUE_MAX_LENGTH) {
                writer.write("Type string, Key=" + key + " Value=" + value + ", Error=" + "Value is too long\n");
                return rawValue;
            }

            if (!doCompare) {
                jedisTo.set(key, value);
            } else {
                var value2 = jedisTo.get(key);
                if (!value.equals(value2)) {
                    writer.write("Type string, Key=" + key + " Value=" + value + ", Error=" + "Value not equal\n");
                    notEqualCount++;
                }
            }
        } else if (keyType.equals("list")) {
            var listLength = jedis.llen(key);
            if (listLength > RedisList.LIST_MAX_SIZE) {
                writer.write("Key=" + key + " Value=" + listLength + ", Error=" + "List size is too big\n");
                return rawValue;
            }

            var listValue = jedis.lrange(key, 0, -1);
            rawValue = listValue;
            lastVisitedValue = listValue;

            if (!doCompare) {
                jedisTo.del(key);

                final int batchSize = 1000;
                for (int i = 0; i < listValue.size(); i += batchSize) {
                    var batch = listValue.subList(i, Math.min(i + batchSize, listValue.size()));
                    jedisTo.lpush(key, batch.toArray(new String[0]));

                    if (batch.size() == batchSize) {
                        writer.write("Pushed " + batch.size() + " elements while total size=" + listValue.size() + " to list " + key + "\n");
                    }
                }
            } else {
                var listValue2 = jedisTo.lrange(key, 0, -1);
                if (listValue.size() != listValue2.size()) {
                    writer.write("Type list, Key=" + key + " Value=" + listValue + ", Error=" + "List size not equal\n");
                    notEqualCount++;
                } else {
                    for (int i = 0; i < listValue.size(); i++) {
                        if (!listValue.get(i).equals(listValue2.get(i))) {
                            writer.write("Type list, Key=" + key + " Value=" + listValue + ", Error=" + "List value not equal\n");
                            notEqualCount++;
                            break;
                        }
                    }
                }
            }
        } else if (keyType.equals("set")) {
            var setLength = jedis.scard(key);
            if (setLength > RedisHashKeys.SET_MEMBER_MAX_LENGTH) {
                writer.write("Key=" + key + " Value=" + setLength + ", Error=" + "Set size is too big\n");
                return rawValue;
            }

            var setValue = jedis.smembers(key);
            rawValue = setValue;
            lastVisitedValue = setValue;

            if (!doCompare) {
                jedisTo.del(key);

                var setArray = setValue.toArray();
                String[] values = new String[setValue.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = setArray[i].toString();
                }

                final int batchSize = 1000;
                for (int i = 0; i < values.length; i += batchSize) {
                    var batch = Arrays.copyOfRange(values, i, Math.min(i + batchSize, values.length));
                    jedisTo.sadd(key, batch);

                    if (batch.length == batchSize) {
                        writer.write("Pushed " + batch.length + " elements while total size=" + values.length + " to set " + key + "\n");
                    }
                }
            } else {
                var setValue2 = jedisTo.smembers(key);
                if (setValue.size() != setValue2.size()) {
                    writer.write("Type set, Key=" + key + " Value=" + setValue + ", Error=" + "Set size not equal\n");
                    notEqualCount++;
                } else {
                    for (var value : setValue) {
                        if (!setValue2.contains(value)) {
                            writer.write("Type set, Key=" + key + " Value=" + setValue + ", Error=" + "Set value not equal\n");
                            notEqualCount++;
                            break;
                        }
                    }
                }
            }
        } else if (keyType.equals("zset")) {
            var zsetLength = jedis.zcard(key);
            if (zsetLength > RedisZSet.ZSET_MAX_SIZE) {
                writer.write("Key=" + key + " Value=" + zsetLength + ", Error=" + "Zset size is too big\n");
                return rawValue;
            }

            var zsetValue = jedis.zrangeWithScores(key, 0, -1);
            rawValue = zsetValue;
            lastVisitedValue = zsetValue;

            if (!doCompare) {
                jedisTo.del(key);

                Map<String, Double> scoreMembers = new HashMap<>();
                for (var value : zsetValue) {
                    scoreMembers.put(value.getElement(), value.getScore());
                }

                final int batchSize = 1000;
                for (int i = 0; i < scoreMembers.size(); i += batchSize) {
                    var batch = scoreMembers.entrySet().stream().skip(i).limit(batchSize).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    jedisTo.zadd(key, batch);

                    if (batch.size() == batchSize) {
                        writer.write("Pushed " + batch.size() + " elements while total size=" + scoreMembers.size() + " to zset " + key + "\n");
                    }
                }
            } else {
                var zsetValue2 = jedisTo.zrangeWithScores(key, 0, -1);
                if (zsetValue.size() != zsetValue2.size()) {
                    writer.write("Type zset, Key=" + key + " Value=" + zsetValue + ", Error=" + "ZSet size not equal\n");
                    notEqualCount++;
                } else {
                    for (int i = 0; i < zsetValue.size(); i++) {
                        if (!zsetValue.get(i).getElement().equals(zsetValue2.get(i).getElement()) ||
                                zsetValue.get(i).getScore() != zsetValue2.get(i).getScore()) {
                            writer.write("Type zset, Key=" + key + " Value=" + zsetValue + ", Error=" + "ZSet value not equal\n");
                            notEqualCount++;
                            break;
                        }
                    }
                }
            }
        } else if (keyType.equals("hash")) {
            var hashLength = jedis.hlen(key);
            if (hashLength > RedisHashKeys.HASH_MAX_SIZE) {
                writer.write("Hash key=" + key + " length=" + hashLength + " is too long, skip it\n");
                return rawValue;
            }

            var hashValue = jedis.hgetAll(key);
            rawValue = hashValue;
            lastVisitedValue = hashValue;

            if (!doCompare) {
                jedisTo.del(key);

                var filterMap = new HashMap<String, String>();
                for (var entry : hashValue.entrySet()) {
                    if (!StringUtils.isEmpty(entry.getValue())) {
                        if (entry.getKey().length() > CompressedValue.KEY_MAX_LENGTH) {
                            writer.write("Hash key=" + key + " field=" + entry.getKey() + " is too long, skip it\n");
                            continue;
                        }

                        if (entry.getValue().length() > CompressedValue.VALUE_MAX_LENGTH) {
                            writer.write("Hash key=" + key + " field=" + entry.getKey() + " value=" + entry.getValue() + " is too long, skip it\n");
                            continue;
                        }

                        filterMap.put(entry.getKey(), entry.getValue());
                    }
                }

                final int batchSize = 1000;
                for (int i = 0; i < filterMap.size(); i += batchSize) {
                    var batch = filterMap.entrySet().stream().skip(i).limit(batchSize).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    jedisTo.hmset(key, batch);

                    if (batch.size() == batchSize) {
                        writer.write("Pushed " + batch.size() + " elements while total size=" + filterMap.size() + " to hash " + key + "\n");
                    }
                }
            } else {
                var hashValue2 = jedisTo.hgetAll(key);
                if (hashValue.size() != hashValue2.size()) {
                    writer.write("Type: hash, Key=" + key + " Value=" + hashValue + ", Error=" + "Hash size not equal\n");
                    notEqualCount++;
                } else {
                    for (var entry : hashValue.entrySet()) {
                        if (!entry.getValue().equals(hashValue2.get(entry.getKey()))) {
                            writer.write("Type hash, Key=" + key + " Value=" + hashValue + ", Error=" + "Hash value not equal\n");
                            notEqualCount++;
                            break;
                        }
                    }
                }
            }
        } else {
            writer.write("Key=" + key + " Value=" + "Unknown type=" + keyType + "\n");
        }
        return rawValue;
    }
}
