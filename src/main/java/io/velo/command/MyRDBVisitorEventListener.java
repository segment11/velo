package io.velo.command;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.rdb.iterable.datatype.*;
import io.velo.DictMap;
import io.velo.persist.LocalPersist;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;

public class MyRDBVisitorEventListener implements EventListener {
    private final LGroup lGroup;
    private final HGroup hGroup;

    int updateKeyCount;
    int skipKeyCount;

    public MyRDBVisitorEventListener(LGroup lGroup) {
        this.lGroup = lGroup;
        this.hGroup = new HGroup(null, null, null);
        this.hGroup.from(lGroup);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();
    private final DictMap dictMap = DictMap.getInstance();

    @Override
    public void onEvent(Replicator replicator, Event event) {
        if (event instanceof BatchedKeyValuePair<?, ?> kv) {
            long ttl;
            if (kv.getExpiredType() != ExpiredType.NONE) {
                ttl = kv.getExpiredMs();
            } else {
                ttl = 0;
            }

            var keyBytes = (byte[]) kv.getKey();
            var key = new String(keyBytes);
            var slotWithKeyHash = lGroup.slot(key);
            var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());

            var isSkip = false;
            if (event instanceof BatchedKeyStringValueString s) {
                oneSlot.asyncExecute(() -> {
                    lGroup.set(s.getValue(), slotWithKeyHash, 0, ttl);
                });
            } else if (event instanceof BatchedKeyStringValueList l) {
                var rl = new RedisList();
                var list = l.getValue();
                for (var elementBytes : list) {
                    rl.addLast(elementBytes);
                }
                LGroup.saveRedisList(rl, slotWithKeyHash, lGroup, dictMap);
            } else if (event instanceof BatchedKeyStringValueHash h) {
                if (hGroup.isUseHH(keyBytes)) {
                    var rhh = new RedisHH();
                    for (var entry : h.getValue().entrySet()) {
                        rhh.put(new String(entry.getKey()), entry.getValue());
                    }

                    oneSlot.asyncExecute(() -> {
                        hGroup.saveRedisHH(rhh, slotWithKeyHash);
                    });
                } else {
                    var rhk = new RedisHashKeys();
                    for (var entry : h.getValue().entrySet()) {
                        var field = new String(entry.getKey());
                        var fieldKey = RedisHashKeys.fieldKey(key, field);

                        var slotWithKeyHashThisField = lGroup.slot(fieldKey);
                        oneSlot.asyncExecute(() -> {
                            hGroup.set(entry.getValue(), slotWithKeyHashThisField, 0, 0);
                        });
                    }
                    oneSlot.asyncExecute(() -> {
                        hGroup.saveRedisHashKeys(rhk, key);
                    });
                }
            } else if (event instanceof BatchedKeyStringValueTTLHash h) {
                if (hGroup.isUseHH(keyBytes)) {
                    var rhh = new RedisHH();
                    for (var entry : h.getValue().entrySet()) {
                        var ttlValue = entry.getValue();
                        rhh.put(new String(entry.getKey()), ttlValue.getValue(), ttlValue.getExpires());
                    }

                    oneSlot.asyncExecute(() -> {
                        hGroup.saveRedisHH(rhh, slotWithKeyHash);
                    });
                } else {
                    var rhk = new RedisHashKeys();
                    for (var entry : h.getValue().entrySet()) {
                        var field = new String(entry.getKey());
                        var fieldKey = RedisHashKeys.fieldKey(key, field);

                        var slotWithKeyHashThisField = lGroup.slot(fieldKey);
                        oneSlot.asyncExecute(() -> {
                            var ttlValue = entry.getValue();
                            hGroup.set(ttlValue.getValue(), slotWithKeyHashThisField, 0, ttlValue.getExpires());
                        });
                    }
                    oneSlot.asyncRun(() -> {
                        hGroup.saveRedisHashKeys(rhk, key);
                    });
                }
            } else if (event instanceof BatchedKeyStringValueSet s) {
                var rhk = new RedisHashKeys();
                for (var memberBytes : s.getValue()) {
                    rhk.add(new String(memberBytes));
                }
                oneSlot.asyncExecute(() -> {
                    SGroup.saveRedisSet(rhk, slotWithKeyHash, lGroup, dictMap);
                });
            } else if (event instanceof BatchedKeyStringValueZSet z) {
                var rz = new RedisZSet();
                for (var entry : z.getValue()) {
                    rz.add(entry.getScore(), new String(entry.getElement()));
                }
                oneSlot.asyncRun(() -> {
                    ZGroup.saveRedisZSet(rz, slotWithKeyHash, lGroup, dictMap);
                });
            } else {
                isSkip = true;
            }

            if (isSkip) {
                skipKeyCount++;
            } else {
                updateKeyCount++;
            }
        } else {
            skipKeyCount++;
        }
    }
}
