package io.velo.command;

import io.netty.buffer.ByteBuf;
import io.velo.rdb.RDBParser;

public class VeloRDBImporter implements RDBImporter {
    private final RDBParser parser = new RDBParser();


    @Override
    public void restore(ByteBuf buf, RDBCallback callback) {
        // Redis DUMP/RESTORE format: <type><value-bytes><rdb-version><crc64>
        int len = buf.readableBytes();
        if (len < 11) { // at least 1 type + 2 version + 8 crc64
            throw new IllegalArgumentException("Serialized value too short");
        }

        // RDB version: 2 bytes before last 8 bytes (little-endian)
        int rdbVersion = buf.getUnsignedShortLE(len - 10);
        if (rdbVersion < RDBParser.RDB_MIN_VERSION) {
            throw new IllegalArgumentException("Unsupported RDB version: " + rdbVersion);
        }

        // CRC64: last 8 bytes (little-endian)
        long expectedCrc = buf.getLongLE(len - 8);
        // CRC64 check: over all bytes except the last 8 (footer)
//        var actualCrc = RedisCrc.crc64(0, buf.array(), 0, len - 8);
//        if (actualCrc != expectedCrc) {
//            throw new IllegalArgumentException("CRC64 mismatch: expected " + expectedCrc + ", got " + actualCrc);
//        }

        parser.readEntry(buf, callback);
    }
}
