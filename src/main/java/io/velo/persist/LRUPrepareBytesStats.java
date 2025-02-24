package io.velo.persist;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * A utility class to manage and track memory usage statistics for different types of data structures
 * in the Least Recently Used (LRU) cache system.
 */
public class LRUPrepareBytesStats {

    // Private constructor to prevent instantiation of utility class
    private LRUPrepareBytesStats() {
    }

    /**
     * Enum representing different types of data structures in the cache.
     */
    enum Type {
        fd_key_bucket,       // File descriptor key bucket
        fd_chunk_data,       // File descriptor chunk data
        big_string,          // Big string data
        kv_read_group_by_wal_group, // Key-value read group by write-ahead log group
        kv_write_in_wal,     // Key-value write in write-ahead log
        chunk_segment_merged_cv_buffer // Chunk segment merged content value buffer
    }

    /**
     * Record representing an entry in the memory usage statistics list.
     */
    private record One(Type type, String key, int lruMemoryRequireMB, boolean isExact) {
    }

    /**
     * List to store the memory usage statistics records.
     */
    static ArrayList<One> list = new ArrayList<>();

    /**
     * Adds a new memory usage statistics entry to the list.
     *
     * @param type               The type of data structure (key bucket, chunk data, etc.).
     * @param key                The key associated with the data structure.
     * @param lruMemoryRequireMB The amount of memory required by the data structure in MB.
     * @param isExact            Indicates if the memory requirement is exact.
     */
    static void add(@NotNull Type type, @NotNull String key, int lruMemoryRequireMB, boolean isExact) {
        list.add(new One(type, key, lruMemoryRequireMB, isExact));
    }

    /**
     * Removes a memory usage statistics entry from the list based on its type and key.
     *
     * @param type The type of data structure.
     * @param key  The key associated with the data structure.
     */
    static void removeOne(@NotNull Type type, @NotNull String key) {
        list.removeIf(one -> one.type == type && one.key.equals(key));
    }

    /**
     * Computes the total memory required by all entries in the list.
     *
     * @return The total memory required in MB.
     */
    static int sum() {
        return list.stream().mapToInt(one -> one.lruMemoryRequireMB).sum();
    }

    /**
     * Computes the total memory required by entries of a specific type in the list.
     *
     * @param type The type of data structure.
     * @return The total memory required for the specified type in MB.
     */
    static int sum(@NotNull Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.lruMemoryRequireMB).sum();
    }
}