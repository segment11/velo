package io.velo.mock;

import io.velo.CompressedValue;
import io.velo.persist.OneSlot;
import org.jetbrains.annotations.TestOnly;

/**
 * This interface provides methods to manipulate data in a storage system bypassing
 * the usual get and set mechanisms. It is intended for use in testing scenarios.
 *
 * @see TestOnly
 */
@TestOnly
public interface ByPassGetSet {

    /**
     * Puts a compressed value into the storage system.
     *
     * @param slot        the slot number where the value should be stored.
     * @param key         the key associated with the value.
     * @param bucketIndex the index of the bucket in the slot where the value should be stored.
     * @param cv          the compressed value to be stored.
     */
    void put(short slot, String key, int bucketIndex, CompressedValue cv);

    /**
     * Puts a compressed value into the storage system.
     *
     * @param key the key associated with the value.
     * @param cv  the compressed value to be stored.
     */
    default void put(String key, CompressedValue cv) {
        put((short) 0, key, 0, cv);
    }

    /**
     * Removes a value from the storage system associated with a given key.
     *
     * @param slot the slot number where the value is stored.
     * @param key  the key associated with the value to be removed.
     * @return true if the value was successfully removed, false otherwise.
     */
    boolean remove(short slot, String key);

    /**
     * Retrieves a value from the storage system based on the provided key and bucket index.
     *
     * @param slot        the slot number where the value is stored.
     * @param key         the key with the value to be retrieved.
     * @param bucketIndex the index of the bucket in the slot where the value is stored.
     * @param keyHash     the hash of the key.
     * @return the value stored, either as a buffer or a compressed value.
     */
    OneSlot.BufOrCompressedValue getBuf(short slot, String key, int bucketIndex, long keyHash);
}