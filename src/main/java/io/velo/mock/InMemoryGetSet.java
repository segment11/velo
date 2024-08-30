package io.velo.mock;

import io.velo.CompressedValue;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;

@TestOnly
public class InMemoryGetSet implements ByPassGetSet {
    private final HashMap<String, CompressedValue> map = new HashMap<>();

    @Override
    public void put(short slot, String key, int bucketIndex, CompressedValue cv) {
        map.put(key, cv);
    }

    @Override
    public boolean remove(short slot, String key) {
        return map.remove(key) != null;
    }

    @Override
    public OneSlot.BufOrCompressedValue getBuf(short slot, byte[] keyBytes, int bucketIndex, long keyHash) {
        var cv = map.get(new String(keyBytes));
        if (cv == null) {
            return null;
        }

        return new OneSlot.BufOrCompressedValue(null, cv);
    }
}
