package io.velo.persist;

public record ScanCursor(short slot, short walGroupIndex, short skipCount, byte splitIndex) {
    public long toLong() {
        return ((long) slot << 48) | ((long) walGroupIndex << 24) | ((long) skipCount << 8) | splitIndex;
    }

    public static ScanCursor fromLong(long value) {
        return new ScanCursor((short) (value >> 48), (short) (value >> 24), (short) (value >> 8), (byte) value);
    }
}
