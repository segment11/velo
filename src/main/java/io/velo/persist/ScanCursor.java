package io.velo.persist;

/**
 * Cursor for scanning keys across WAL groups and buckets.
 */
public record ScanCursor(short slot, int walGroupIndex, short walSkipCount, short keyBucketsSkipCount,
                         byte splitIndex, byte bucketIndex) {
    /**
     * Sentinel value for {@link #walSkipCount} marking that the current WAL group scan is done.
     */
    // one wal group can hold max 200 + 200 < 1024 keys.
    public static final short ONE_WAL_SKIP_COUNT_ITERATE_END = 1023;

    /**
     * Sentinel cursor indicating the entire scan has finished.
     */
    public static ScanCursor END = new ScanCursor((short) 0, 0, ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) 0, (byte) 0);

    /**
     * Returns whether the current WAL group iteration has reached its end sentinel.
     *
     * @return true if the WAL group scan is finished
     */
    public boolean isWalIterateEnd() {
        return walSkipCount == ONE_WAL_SKIP_COUNT_ITERATE_END;
    }

    /**
     * Packs this cursor into a single long (the END cursor packs to {@code 0L}).
     * Layout: slot 16 bits, wal group index 18 bits, wal skip count 10 bits,
     * bucket index in wal group 5 bits, key buckets skip count 10 bits, split index 5 bits.
     *
     * @return the packed long representation
     */
    public long toLong() {
        if (this == END) {
            return 0L;
        }

        assert (walGroupIndex < (1 << 18) && walSkipCount < 1024 && keyBucketsSkipCount < 1024
                && bucketIndex < 32 && splitIndex < 32);
        return ((long) slot << 48) | (long) walGroupIndex << 30 | ((long) walSkipCount << 20)
                | ((long) bucketIndex << 15) | ((long) keyBucketsSkipCount << 5) | splitIndex;
    }

    /**
     * Decodes a packed long back into a cursor.
     *
     * @param value the packed long representation
     * @return the decoded cursor
     */
    public static ScanCursor fromLong(long value) {
        return new ScanCursor(
                (short) (value >> 48),
                (int) ((value >> 30) & 0x3ffff),
                (short) ((value >> 20) & 0x3ff),
                (short) ((value >> 5) & 0x3ff),
                (byte) (value & 0x1f),
                (byte) ((value >> 15) & 0x1f)
        );
    }
}
