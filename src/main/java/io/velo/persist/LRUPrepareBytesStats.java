package io.velo.persist;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * Tracks memory usage statistics for LRU cache data structures.
 */
public class LRUPrepareBytesStats {

    private LRUPrepareBytesStats() {
    }

    enum Type {
        fd_key_bucket,
        fd_chunk_data,
        big_string,
        kv_read_group_by_wal_group,
        kv_write_in_wal,
        chunk_segment_merged_cv_buffer
    }

    private record One(Type type, String key, int lruMemoryRequireMB, boolean isExact) {
    }

    static ArrayList<One> list = new ArrayList<>();

    /**
     * @param type               the data structure type
     * @param key                the associated key
     * @param lruMemoryRequireMB memory required in MB
     * @param isExact            whether the requirement is exact
     */
    static void add(@NotNull Type type, @NotNull String key, int lruMemoryRequireMB, boolean isExact) {
        list.add(new One(type, key, lruMemoryRequireMB, isExact));
    }

    /**
     * @param type the data structure type
     * @param key  the associated key
     */
    static void removeOne(@NotNull Type type, @NotNull String key) {
        list.removeIf(one -> one.type == type && one.key.equals(key));
    }

    /**
     * @return total memory required in MB
     */
    static int sum() {
        return list.stream().mapToInt(one -> one.lruMemoryRequireMB).sum();
    }

    /**
     * @param type the data structure type
     * @return total memory required for the type in MB
     */
    static int sum(@NotNull Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.lruMemoryRequireMB).sum();
    }
}