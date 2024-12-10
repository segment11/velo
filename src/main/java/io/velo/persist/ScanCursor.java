package io.velo.persist;

public record ScanCursor(short slot, int walGroupIndex, short skipCount, byte splitIndex) {
    public static ScanCursor END = new ScanCursor((short) 0, 0, (short) 0, (byte) 0);

    public long toLong() {
        // wal group index use 24 bits
        return ((long) slot << 48) | (long) walGroupIndex << 24 | ((long) skipCount << 8) | splitIndex;
    }

    public static ScanCursor fromLong(long value) {
        var walGroupIndexLong = (value << 16) >> 40;
        return new ScanCursor(
                (short) (value >> 48),
                (int) walGroupIndexLong,
                (short) (value >> 8),
                (byte) value
        );
    }
}
