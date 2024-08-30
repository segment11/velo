package io.velo;

import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;

public class StaticMemoryPrepareBytesStats {
    private StaticMemoryPrepareBytesStats() {
    }

    public enum Type {
        wal_cache, wal_cache_init, meta_chunk_segment_flag_seq, fd_read_write_buffer
    }

    public record One(Type type, int staticMemoryRequireMB, boolean isExact) {
    }

    @VisibleForTesting
    static ArrayList<One> list = new ArrayList<>();

    public static void add(Type type, int staticMemoryRequireMB, boolean isExact) {
        list.add(new One(type, staticMemoryRequireMB, isExact));
    }

    public static int sum() {
        return list.stream().mapToInt(one -> one.staticMemoryRequireMB).sum();
    }

    public static int sum(Type type) {
        return list.stream().filter(one -> one.type == type).mapToInt(one -> one.staticMemoryRequireMB).sum();
    }
}
