package io.velo.tools;

import io.velo.DictMap;
import io.velo.repl.support.ExtendProtocolCommand;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ScanAndLoad {
    public static void main(String[] args) {
//        setOneKeyValue();
//        scanAndLoad(false, 0);

        try {
            scanAndLoad(false, -1);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Last visited key=" + lastVisitedKey + ", type=" + lastVisitedKeyType + ", value=" + lastVisitedValue);
        }
//        scanAndTrainHashFieldDicts();
    }

    private static void setOneKeyValue() {
        var jedis = new Jedis("localhost", 6379);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 7379, jedisClientConfig);

        final String key = "system:configParamsName:long_rent";
        jedisTo.set(key, jedis.get(key));

        jedis.close();
        jedisTo.close();
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

    private static void scanAndLoad(boolean doCompare, int maxCount) {
        var jedis = new Jedis("localhost", 6379);

        var jedisClientConfig = DefaultJedisClientConfig.builder().timeoutMillis(20000).build();
        var jedisTo = new Jedis("localhost", 7379, jedisClientConfig);

        var scanParams = new ScanParams().count(1000);
        var result = jedis.scan("0", scanParams);

        long count = 0;
        long notEqualCount = 0;

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
                    if (keyType.equals("string")) {
                        var value = jedis.get(key);
                        rawValue = value;
                        lastVisitedValue = value;

                        if (value.getBytes().length > Short.MAX_VALUE) {
                            System.out.println("Type string, Key=" + key + " Value=" + value + ", Error=" + "Value is too long");
                            continue;
                        }

                        if (!doCompare) {
                            jedisTo.set(key, value);
                        } else {
                            var value2 = jedisTo.get(key);
                            if (!value.equals(value2)) {
                                System.out.println("Type string, Key=" + key + " Value=" + value + ", Error=" + "Value not equal");
                                notEqualCount++;
                            }
                        }
                    } else if (keyType.equals("list")) {
                        var listValue = jedis.lrange(key, 0, -1);
                        rawValue = listValue;
                        lastVisitedValue = listValue;

                        if (!doCompare) {
                            jedisTo.lpush(key, listValue.toArray(new String[0]));
                        } else {
                            var listValue2 = jedisTo.lrange(key, 0, -1);
                            if (listValue.size() != listValue2.size()) {
                                System.out.println("Type list, Key=" + key + " Value=" + listValue + ", Error=" + "List size not equal");
                                notEqualCount++;
                            } else {
                                for (int i = 0; i < listValue.size(); i++) {
                                    if (!listValue.get(i).equals(listValue2.get(i))) {
                                        System.out.println("Type list, Key=" + key + " Value=" + listValue + ", Error=" + "List value not equal");
                                        notEqualCount++;
                                        break;
                                    }
                                }
                            }
                        }
                    } else if (keyType.equals("set")) {
                        var setValue = jedis.smembers(key);
                        rawValue = setValue;
                        lastVisitedValue = setValue;

                        if (!doCompare) {
                            var setArray = setValue.toArray();
                            String[] values = new String[setValue.size()];
                            for (int i = 0; i < values.length; i++) {
                                values[i] = setArray[i].toString();
                            }
                            jedisTo.sadd(key, values);
                        } else {
                            var setValue2 = jedisTo.smembers(key);
                            if (setValue.size() != setValue2.size()) {
                                System.out.println("Type set, Key=" + key + " Value=" + setValue + ", Error=" + "Set size not equal");
                                notEqualCount++;
                            } else {
                                for (var value : setValue) {
                                    if (!setValue2.contains(value)) {
                                        System.out.println("Type set, Key=" + key + " Value=" + setValue + ", Error=" + "Set value not equal");
                                        notEqualCount++;
                                        break;
                                    }
                                }
                            }
                        }
                    } else if (keyType.equals("zset")) {
                        var zsetValue = jedis.zrangeWithScores(key, 0, -1);
                        rawValue = zsetValue;
                        lastVisitedValue = zsetValue;

                        if (!doCompare) {
                            Map<String, Double> scoreMembers = new HashMap<>();
                            for (var value : zsetValue) {
                                scoreMembers.put(value.getElement(), value.getScore());
                            }
                            jedisTo.zadd(key, scoreMembers);
                        } else {
                            var zsetValue2 = jedisTo.zrangeWithScores(key, 0, -1);
                            if (zsetValue.size() != zsetValue2.size()) {
                                System.out.println("Type zset, Key=" + key + " Value=" + zsetValue + ", Error=" + "ZSet size not equal");
                                notEqualCount++;
                            } else {
                                for (int i = 0; i < zsetValue.size(); i++) {
                                    if (!zsetValue.get(i).getElement().equals(zsetValue2.get(i).getElement()) ||
                                            zsetValue.get(i).getScore() != zsetValue2.get(i).getScore()) {
                                        System.out.println("Type zset, Key=" + key + " Value=" + zsetValue + ", Error=" + "ZSet value not equal");
                                        notEqualCount++;
                                        break;
                                    }
                                }
                            }
                        }
                    } else if (keyType.equals("hash")) {
                        var hashValue = jedis.hgetAll(key);
                        rawValue = hashValue;
                        lastVisitedValue = hashValue;

                        if (!doCompare) {
                            var filterMap = new HashMap<String, String>();
                            for (var entry : hashValue.entrySet()) {
                                if (!StringUtils.isEmpty(entry.getValue())) {
                                    filterMap.put(entry.getKey(), entry.getValue());
                                }
                            }
                            jedisTo.hmset(key, filterMap);
                        } else {
                            var hashValue2 = jedisTo.hgetAll(key);
                            if (hashValue.size() != hashValue2.size()) {
                                System.out.println("Type: hash, Key=" + key + " Value=" + hashValue + ", Error=" + "Hash size not equal");
                                notEqualCount++;
                            } else {
                                for (var entry : hashValue.entrySet()) {
                                    if (!entry.getValue().equals(hashValue2.get(entry.getKey()))) {
                                        System.out.println("Type hash, Key=" + key + " Value=" + hashValue + ", Error=" + "Hash value not equal");
                                        notEqualCount++;
                                        break;
                                    }
                                }
                            }
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

        jedis.close();
        jedisTo.close();

        System.out.println("Processed " + count + " keys, not equal count=" + notEqualCount);
    }
}
