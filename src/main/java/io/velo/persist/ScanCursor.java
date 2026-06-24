package io.velo.persist;

/**
 * Cursor for scanning keys across WAL groups and buckets.
 */
public record ScanCursor(short slot, int walGroupIndex, short walSkipCount, short keyBucketsSkipCount,
                         byte splitIndex) {
    /**
     * Sentinel value for {@link #walSkipCount} marking that the current WAL group scan is done.
     */
    // one wal group can hold max 200 + 200 < 1024 keys.
    public static final short ONE_WAL_SKIP_COUNT_ITERATE_END = 1023;

    /**
     * Sentinel cursor indicating the entire scan has finished.
     */
    public static ScanCursor END = new ScanCursor((short) 0, 0, ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) 0);

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
     *
     * @return the packed long representation
     */
    public long toLong() {
        if (this == END) {
            return 0L;
        }

        assert (walSkipCount < 1024 && keyBucketsSkipCount < 1024);
        // slot use 16 bits, wal group index use 24 bits
        // wal skip count use 10 bits, key buckets skip count use 10 bits, split index use 4 bits
        return ((long) slot << 48) | (long) walGroupIndex << 24 | ((long) walSkipCount << 14) | ((long) keyBucketsSkipCount << 4) | splitIndex;
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
                (int) ((value >> 24) & 0xffffff),
                (short) ((value >> 14) & 0x3ff),
                (short) ((value >> 4) & 0x3ff),
                (byte) (value & 0xf)
        );
    }
}
