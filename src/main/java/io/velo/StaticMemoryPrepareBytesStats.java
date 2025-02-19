package io.velo;

import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

/**
 * Manages and tracks static memory requirements for different types of memory usage.
 * This class provides methods to add memory requirements, calculate total memory usage, and filter by type.
 */
public class StaticMemoryPrepareBytesStats {
    /**
     * Private constructor to prevent instantiation.
     */
    private StaticMemoryPrepareBytesStats() {
    }

    /**
     * Enum representing different types of memory usage.
     */
    public enum Type {
        wal_cache, wal_cache_init, meta_chunk_segment_flag_seq, fd_read_write_buffer
    }

    /**
     * Record representing a single memory requirement entry.
     */
    public record One(Type type, int staticMemoryRequireMB, boolean isExact) {
    }

    /**
     * List of memory requirement entries.
     */
    @VisibleForTesting
    static ArrayList<One> list = new ArrayList<>();

    /**
     * Adds a memory requirement entry to the list.
     *
     * @param type                  the type of memory usage
     * @param staticMemoryRequireMB the required memory in megabytes
     * @param isExact               true if the memory requirement is exact, false otherwise
     */
    public static void add(Type type, int staticMemoryRequireMB, boolean isExact) {
        list.add(new One(type, staticMemoryRequireMB, isExact));
    }

    /**
     * Calculates the total memory requirement across all entries.
     *
     * @return the total memory requirement in megabytes
     */
    public static int sum() {
        return list.stream().mapToInt(one -> one.staticMemoryRequireMB).sum();
    }

    /**
     * Calculates the total memory requirement for a specific type of memory usage.
     *
     * @param type the type of memory usage
     * @return the total memory requirement for the specified type in megabytes
     */
    public static int sum(Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.staticMemoryRequireMB).sum();
    }
}
