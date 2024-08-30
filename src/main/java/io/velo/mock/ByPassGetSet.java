package io.velo.mock;

import io.velo.CompressedValue;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.TestOnly;

@TestOnly
public interface ByPassGetSet {
    void put(short slot, String key, int bucketIndex, CompressedValue cv);

    boolean remove(short slot, String key);

    OneSlot.BufOrCompressedValue getBuf(short slot, byte[] keyBytes, int bucketIndex, long keyHash);
}
