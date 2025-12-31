package io.velo.mock;

import io.velo.CompressedValue;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.TestOnly;

import java.util.HashMap;

/**
 * An in-memory implementation of the {@link ByPassGetSet} interface for testing purposes.
 * This class uses a {@link HashMap} to store and retrieve {@link CompressedValue} objects.
 *
 * @author Your Name
 * @see ByPassGetSet
 * @see CompressedValue
 * @see OneSlot.BufOrCompressedValue
 * @since 1.0.0
 */
@TestOnly
public class InMemoryGetSet implements ByPassGetSet {
    private final HashMap<String, CompressedValue> map = new HashMap<>();

    /**
     * Stores a {@link CompressedValue} in the in-memory map.
     *
     * @param slot        the slot number (not used in this implementation)
     * @param key         the key associated with the value
     * @param bucketIndex the bucket index (not used in this implementation)
     * @param cv          the {@link CompressedValue} to store
     */
    @Override
    public void put(short slot, String key, int bucketIndex, CompressedValue cv) {
        map.put(key, cv);
    }

    /**
     * Removes a {@link CompressedValue} from the in-memory map.
     *
     * @param slot the slot number (not used in this implementation)
     * @param key  the key associated with the value to be removed
     * @return true if the value was successfully removed, false otherwise
     */
    @Override
    public boolean remove(short slot, String key) {
        return map.remove(key) != null;
    }

    /**
     * Retrieves a {@link CompressedValue} from the in-memory map.
     *
     * @param slot        the slot number (not used in this implementation)
     * @param key         the key to retrieve the value for
     * @param bucketIndex the bucket index (not used in this implementation)
     * @param keyHash     the hash of the key (not used in this implementation)
     * @return a {@link OneSlot.BufOrCompressedValue} containing the retrieved value, or null if the key is not found
     */
    @Override
    public OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash) {
        var cv = map.get(key);
        if (cv == null) {
            return null;
        }

        return new OneSlot.BufOrCompressedValue(null, cv);
    }
}