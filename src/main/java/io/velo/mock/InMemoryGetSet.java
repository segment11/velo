package io.velo.mock;

import io.velo.CompressedValue;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;

/**
 * In-memory implementation of {@link ByPassGetSet} for testing.
 * Uses {@link HashMap} to store {@link CompressedValue} objects.
 */
@TestOnly
public class InMemoryGetSet implements ByPassGetSet {
    /** Storage map for compressed values. */
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
    public OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash) {
        var cv = map.get(key);
        if (cv == null) {
            return null;
        }

        return new OneSlot.BufOrCompressedValue(null, cv);
    }
}