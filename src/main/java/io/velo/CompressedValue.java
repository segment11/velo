package io.velo;

import com.github.luben.zstd.Zstd;
import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents a compressed value with metadata for storage and retrieval.
 * Handles compression/decompression, expiration, type tracking, and serialization.
 *
 * <p>Key features:
 * <ul>
 *   <li>Supports multiple numeric types for efficient storage</li>
 *   <li>Handles Zstd compression with optional dictionaries</li>
 *   <li>Tracks expiration time and sequence numbers for versioning</li>
 *   <li>Supports special types for Redis data structures</li>
 * </ul>
 */
public class CompressedValue {
    /**
     * No expiration set, default value.
     */
    public static final long NO_EXPIRE = 0;

    /**
     * Expire immediately, used for temporary deletion.
     */
    public static final long EXPIRE_NOW = -1;

    /**
     * String type marker, no Zstd dictionary used.
     */
    public static final int NULL_DICT_SEQ = 0;

    /**
     * Numeric type markers (negative values).
     */
    public static final int SP_TYPE_NUM_BYTE = -1;   // 1-byte numeric value
    public static final int SP_TYPE_NUM_SHORT = -2;  // 2-byte numeric value
    public static final int SP_TYPE_NUM_INT = -4;    // 4-byte numeric value
    public static final int SP_TYPE_NUM_LONG = -8;   // 8-byte numeric value
    public static final int SP_TYPE_NUM_DOUBLE = -16; // 8-byte floating point

    /**
     * Short string type markers.
     * Short strings do not require key-value splitting; the value is stored in key buckets.
     */
    public static final int SP_TYPE_SHORT_STRING = -32;

    /**
     * Minimum length for short strings.
     */
    public static final int SP_TYPE_SHORT_STRING_MIN_LEN = 16;

    /**
     * Marker for large strings stored as separate files.
     */
    public static final int SP_TYPE_BIG_STRING = -64;

    /**
     * HyperLogLog data type marker.
     */
    public static final int SP_TYPE_HLL = -96;

    /**
     * Temporary deletion marker flag.
     */
    public static final byte SP_FLAG_DELETE_TMP = -128;

    /**
     * Redis data structure type markers.
     */
    public static final int SP_TYPE_HH = -512;         // Compact hash storage
    public static final int SP_TYPE_HASH = -1024;      // Full hash storage
    public static final int SP_TYPE_LIST = -2048;      // List structure
    public static final int SP_TYPE_SET = -4096;       // Set structure
    public static final int SP_TYPE_ZSET = -8192;      // Sorted set
    public static final int SP_TYPE_GEO = -8193;       // Geospatial data
    public static final int SP_TYPE_STREAM = -16384;   // Stream data
    public static final int SP_TYPE_BLOOM_BITMAP = -200;// Bloom filter

    /**
     * Size limits.
     */
    public static final short KEY_MAX_LENGTH = 256;    // Maximum allowed key size
    // must < Wal.ONE_GROUP_BUFFER_SIZE
    public static final int VALUE_MAX_LENGTH = 1024 * 1024;  // Maximum compressed value size

    /**
     * Header sizes for serialization:
     * seq long + expireAt long + keyHash long + dictSeqOrSpType int + compressedLength int
     */
    public static final int VALUE_HEADER_LENGTH = 8 + 8 + 8 + 4 + 4;
    public static final int KEY_HEADER_LENGTH = 2;

    /**
     * Sequence number for version control.
     */
    private long seq;

    /**
     * Returns the current sequence number used for version tracking.
     *
     * @return The current sequence number.
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Sets a new sequence number.
     *
     * @param seq The new sequence number to set.
     */
    public void setSeq(long seq) {
        this.seq = seq;
    }

    /**
     * 64-bit hash of the associated key.
     */
    private long keyHash;

    /**
     * Returns the 64-bit hash value of the associated key.
     *
     * @return The 64-bit hash value of the associated key.
     */
    public long getKeyHash() {
        return keyHash;
    }

    /**
     * Sets the key hash value.
     *
     * @param keyHash The 64-bit hash of the associated key.
     */
    public void setKeyHash(long keyHash) {
        this.keyHash = keyHash;
    }

    /**
     * Expiration time in milliseconds since epoch.
     */
    private long expireAt = NO_EXPIRE;

    /**
     * Returns the expiration time in milliseconds or {@link #NO_EXPIRE} if not set.
     *
     * @return The expiration time in milliseconds or {@link #NO_EXPIRE} if not set.
     */
    public long getExpireAt() {
        return expireAt;
    }

    /**
     * Sets the expiration time.
     *
     * @param expireAt Milliseconds since epoch or {@link #NO_EXPIRE}.
     */
    public void setExpireAt(long expireAt) {
        this.expireAt = expireAt;
    }

    /**
     * Union value for one of the following:
     * > 0 Zstd dictionary ID
     * == 0 No compression string type
     * < 0 Special type marker, number type, short string, or Redis data structures
     */
    private int dictSeqOrSpType = NULL_DICT_SEQ;

    /**
     * Returns the Zstd dictionary ID or special type marker.
     *
     * @return The Zstd dictionary ID or special type marker.
     */
    public int getDictSeqOrSpType() {
        return dictSeqOrSpType;
    }

    /**
     * Sets the Zstd dictionary ID or special type marker.
     *
     * @param dictSeqOrSpType Must be either a valid Zstd dictionary ID or special type constant.
     */
    public void setDictSeqOrSpType(int dictSeqOrSpType) {
        this.dictSeqOrSpType = dictSeqOrSpType;
    }

    /**
     * Returns the length of the original data before compression.
     *
     * @return The length of the original data before compression.
     */
    public int getUncompressedLength() {
        if (!isCompressed()) {
            return getCompressedLength();
        }

        return (int) Zstd.getFrameContentSize(compressedData);
    }

    /**
     * Returns the length of the compressed data.  If not compressed, this is the raw data length.
     * Only use 24 bits.
     *
     * @return The length of the compressed data.
     */
    public int getCompressedLength() {
        if (compressedData == null) {
            return 0;
        }
        return compressedData.length;
    }

    /**
     * Checks if this value represents a numeric type.
     *
     * @return {@code true} if the type is any of the numeric special types.
     */
    public boolean isTypeNumber() {
        return dictSeqOrSpType <= SP_TYPE_NUM_BYTE && dictSeqOrSpType >= SP_TYPE_NUM_DOUBLE;
    }

    /**
     * Static version of type check for numeric values.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is any of the numeric special types.
     */
    public static boolean isTypeNumber(int spType) {
        return spType <= SP_TYPE_NUM_BYTE && spType >= SP_TYPE_NUM_DOUBLE;
    }

    /**
     * Checks if this value represents a double precision floating point number.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_NUM_DOUBLE}.
     */
    public boolean isTypeDouble() {
        return dictSeqOrSpType == SP_TYPE_NUM_DOUBLE;
    }

    /**
     * Static version of type check for double values.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_NUM_DOUBLE}.
     */
    public static boolean isTypeDouble(int spType) {
        return spType == SP_TYPE_NUM_DOUBLE;
    }

    /**
     * Encodes numeric values with type-specific serialization.
     *
     * @return Byte array containing type header, sequence number, and value bytes.
     * @throws IllegalStateException if called on a non-numeric type.
     */
    public byte[] encodeAsNumber() {
        return switch (dictSeqOrSpType) {
            case SP_TYPE_NUM_BYTE -> {
                var buf = ByteBuffer.allocate(1 + 8 + 1);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData[0]);
                yield buf.array();
            }
            case SP_TYPE_NUM_SHORT -> {
                var buf = ByteBuffer.allocate(1 + 8 + 2);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            case SP_TYPE_NUM_INT -> {
                var buf = ByteBuffer.allocate(1 + 8 + 4);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            case SP_TYPE_NUM_LONG, SP_TYPE_NUM_DOUBLE -> {
                var buf = ByteBuffer.allocate(1 + 8 + 8);
                buf.put((byte) dictSeqOrSpType);
                buf.putLong(seq);
                buf.put(compressedData);
                yield buf.array();
            }
            default -> throw new IllegalStateException("Unexpected number type=" + dictSeqOrSpType);
        };
    }

    /**
     * Encodes a short string value with header information.
     *
     * @return Encoded byte array with type header, sequence, and data.
     */
    public byte[] encodeAsShortString() {
        return encodeAsShortString(seq, compressedData);
    }

    /**
     * Static version of encoding a short string value with header information.
     *
     * @param seq  Sequence number for version tracking.
     * @param data The string data bytes to encode.
     * @return Encoded byte array with type header, sequence, and data.
     */
    public static byte[] encodeAsShortString(long seq, byte[] data) {
        var buf = ByteBuffer.allocate(1 + 8 + data.length);
        buf.put((byte) SP_TYPE_SHORT_STRING);
        buf.putLong(seq);
        buf.put(data);
        return buf.array();
    }

    /**
     * Extracts only the sequence number from encoded numeric or short string values.
     *
     * @param bytes Encoded value bytes.
     * @return The sequence number stored in the encoding.
     */
    public static long getSeqFromNumberOrShortStringEncodedBytes(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong(1);
    }

    /**
     * Extracts the numeric value from the compressed data.
     *
     * @return The numeric value as an appropriate Number subtype.
     * @throws IllegalStateException if the value is not a numeric type.
     */
    public Number numberValue() {
        return switch (dictSeqOrSpType) {
            case SP_TYPE_NUM_BYTE -> compressedData[0];
            case SP_TYPE_NUM_SHORT -> ByteBuffer.wrap(compressedData).getShort();
            case SP_TYPE_NUM_INT -> ByteBuffer.wrap(compressedData).getInt();
            case SP_TYPE_NUM_LONG -> ByteBuffer.wrap(compressedData).getLong();
            case SP_TYPE_NUM_DOUBLE -> ByteBuffer.wrap(compressedData).getDouble();
            default -> throw new IllegalStateException("Not a number type=" + dictSeqOrSpType);
        };
    }

    /**
     * Checks if this value represents a big string.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_BIG_STRING}.
     */
    public boolean isBigString() {
        return dictSeqOrSpType == SP_TYPE_BIG_STRING;
    }

    /**
     * Checks if this value represents a Redis hash structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_HH} or {@link #SP_TYPE_HASH}.
     */
    public boolean isHash() {
        return dictSeqOrSpType == SP_TYPE_HH ||
                dictSeqOrSpType == SP_TYPE_HASH;
    }

    /**
     * Static version of hash type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_HH} or {@link #SP_TYPE_HASH}.
     */
    public static boolean isHash(int spType) {
        return spType == SP_TYPE_HH ||
                spType == SP_TYPE_HASH;
    }

    /**
     * Checks if this value represents a Redis list structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_LIST}.
     */
    public boolean isList() {
        return dictSeqOrSpType == SP_TYPE_LIST;
    }

    /**
     * Static version of list type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_LIST}.
     */
    public static boolean isList(int spType) {
        return spType == SP_TYPE_LIST;
    }

    /**
     * Checks if this value represents a Redis set structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_SET}.
     */
    public boolean isSet() {
        return dictSeqOrSpType == SP_TYPE_SET;
    }

    /**
     * Static version of set type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_SET}.
     */
    public static boolean isSet(int spType) {
        return spType == SP_TYPE_SET;
    }

    /**
     * Checks if this value represents a Redis sorted set structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_ZSET}.
     */
    public boolean isZSet() {
        return dictSeqOrSpType == SP_TYPE_ZSET;
    }

    /**
     * Static version of sorted set type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_ZSET}.
     */
    public static boolean isZSet(int spType) {
        return spType == SP_TYPE_ZSET;
    }

    /**
     * Checks if this value represents geospatial data.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_GEO}.
     */
    public boolean isGeo() {
        return dictSeqOrSpType == SP_TYPE_GEO;
    }

    /**
     * Static version of the geospatial type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_GEO}.
     */
    public static boolean isGeo(int spType) {
        return spType == SP_TYPE_GEO;
    }

    /**
     * Checks if this value represents a Redis stream structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_STREAM}.
     */
    public boolean isStream() {
        return dictSeqOrSpType == SP_TYPE_STREAM;
    }

    /**
     * Static version of stream type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_STREAM}.
     */
    public static boolean isStream(int spType) {
        return spType == SP_TYPE_STREAM;
    }

    /**
     * Checks if this value represents a Bloom filter bitmap.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_BLOOM_BITMAP}.
     */
    public boolean isBloomFilter() {
        return dictSeqOrSpType == SP_TYPE_BLOOM_BITMAP;
    }

    /**
     * Static version of Bloom filter type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_BLOOM_BITMAP}.
     */
    public static boolean isBloomFilter(int spType) {
        return spType == SP_TYPE_BLOOM_BITMAP;
    }

    /**
     * Checks if this value represents a HyperLogLog structure.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_HLL}.
     */
    public boolean isHll() {
        return dictSeqOrSpType == SP_TYPE_HLL;
    }

    /**
     * Static version of HyperLogLog type check.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_HLL}.
     */
    public static boolean isHll(int spType) {
        return spType == SP_TYPE_HLL;
    }

    @Override
    public String toString() {
        return "CompressedValue{" +
                "seq=" + seq +
                ", expireAt=" + expireAt +
                ", dictSeqOrSpType=" + dictSeqOrSpType +
                ", keyHash=" + keyHash +
                ", compressedLength=" + getCompressedLength() +
                '}';
    }

    private byte[] compressedData;

    /**
     * Returns the compressed data. If not compressed, this is raw data.
     *
     * @return The compressed data.
     */
    public byte[] getCompressedData() {
        return compressedData;
    }

    /**
     * Sets the compressed data.
     *
     * @param compressedData The compressed data.
     */
    public void setCompressedData(byte[] compressedData) {
        this.compressedData = compressedData;
    }

    /**
     * Checks if the value has expired based on the current system time.
     *
     * @return {@code true} if the expiration time is set and has passed.
     */
    public boolean isExpired() {
        return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
    }

    /**
     * Checks if the value has no expiration time set.
     *
     * @return {@code true} if the expiration time is {@link #NO_EXPIRE}.
     */
    public boolean noExpire() {
        return expireAt == NO_EXPIRE;
    }

    /**
     * Checks if the value uses compression.
     *
     * @return {@code true} if a Zstd dictionary ID is set.
     */
    public boolean isCompressed() {
        return dictSeqOrSpType > NULL_DICT_SEQ;
    }

    /**
     * Static check for string type values.
     *
     * @param spType The type marker to check.
     * @return {@code true} if the type is {@link #SP_TYPE_BIG_STRING} or higher (including number types).
     */
    public static boolean isTypeString(int spType) {
        return spType >= SP_TYPE_BIG_STRING;
    }

    /**
     * Checks if this value represents a string type.
     *
     * @return {@code true} if the type is {@link #SP_TYPE_BIG_STRING} or higher (including number types).
     */
    public boolean isTypeString() {
        return dictSeqOrSpType >= SP_TYPE_BIG_STRING;
    }

    /**
     * Checks if this value uses a Zstd dictionary for compression.
     *
     * @return {@code true} if a valid dictionary ID is set.
     */
    public boolean isUseDict() {
        return dictSeqOrSpType > NULL_DICT_SEQ;
    }

    /**
     * Decompresses the compressed data using the given Zstd dictionary.
     *
     * @param dict Given Zstd dictionary; {@code null} means use the self-trained dictionary.
     * @return Decompressed data.
     */
    public byte[] decompress(@Nullable Dict dict) {
        assert compressedData != null;
        var dst = new byte[(int) Zstd.getFrameContentSize(compressedData)];
        int r;
        if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
            r = (int) Zstd.decompress(dst, compressedData);
        } else {
            r = dict.decompressByteArray(dst, 0, compressedData, 0, compressedData.length);
        }
        if (r <= 0) {
            throw new IllegalStateException("Decompress error");
        }
        return dst;
    }

    /**
     * Compress result.
     *
     * @param data         Compressed data or raw data.
     * @param isCompressed true if the data is compressed.
     */
    public record CompressResult(byte[] data, boolean isCompressed) {
    }

    /**
     * Compresses the given data using the given Zstd dictionary.
     *
     * @param data The data to compress.
     * @param dict Given Zstd dictionary; {@code null} means use the self-trained dictionary.
     * @return Compress result.
     */
    public static CompressResult compress(byte[] data, @Nullable Dict dict) {
        return compress(data, 0, data.length, dict);
    }

    /**
     * Compresses the given data using the given Zstd dictionary.
     *
     * @param data   The data to compress.
     * @param offset The offset in the data array.
     * @param length The length of the data to compress.
     * @param dict   Given Zstd dictionary; {@code null} means use the self-trained dictionary.
     * @return Compress result.
     */
    public static CompressResult compress(byte[] data, int offset, int length, @Nullable Dict dict) {
        // Memory copy is too much, use direct buffer better.
        var dst = new byte[((int) Zstd.compressBound(length))];
        int compressedSize;
        if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compressByteArray(dst, 0, dst.length, data, offset, length, Zstd.defaultCompressionLevel());
        } else {
            compressedSize = dict.compressByteArray(dst, 0, data, offset, length);
        }
        if (compressedSize <= 0) {
            throw new IllegalStateException("Compress error");
        }

        if (compressedSize > data.length) {
            return new CompressResult(data, false);
        }

        // If wasting too much space, copy to another array.
        if (dst.length != compressedSize) {
            // Memory copy is too much.
            var newDst = new byte[compressedSize];
            System.arraycopy(dst, 0, newDst, 0, compressedSize);
            return new CompressResult(newDst, true);
        } else {
            return new CompressResult(dst, true);
        }
    }

    /**
     * Checks if this value represents a short string.
     *
     * @return {@code true} if the type is a string and the length is less than or equal to {@link #SP_TYPE_SHORT_STRING_MIN_LEN}.
     */
    public boolean isShortString() {
        if (!isTypeString()) {
            return false;
        }
        return compressedData != null && compressedData.length <= CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN;
    }

    /**
     * Checks if the given encoded bytes represent a deleted value.
     *
     * @param encoded Encoded bytes.
     * @return {@code true} if the encoded bytes represent a deleted value.
     */
    public static boolean isDeleted(byte[] encoded) {
        return encoded.length == 1 && encoded[0] == SP_FLAG_DELETE_TMP;
    }

    /**
     * Calculates the total encoded length including headers.
     *
     * @return Total byte length required for serialization.
     */
    public int encodedLength() {
        return VALUE_HEADER_LENGTH + getCompressedLength();
    }

    /**
     * Only reads the sequence number from the encoded bytes.
     *
     * @param encodedBytes Encoded bytes.
     * @return Sequence number.
     */
    public static long onlyReadSeq(byte[] encodedBytes) {
        var buffer = ByteBuffer.wrap(encodedBytes);

        long valueSeqCurrent;
        // Refer to CompressedValue decode.
        var firstByte = buffer.get(0);
        if (firstByte < 0) {
            valueSeqCurrent = buffer.position(1).getLong();
        } else {
            // Normal compressed value encoded.
            valueSeqCurrent = buffer.getLong();
        }
        return valueSeqCurrent;
    }

    /**
     * Only reads the Zstd dictionary ID or type marker from the encoded bytes.
     *
     * @param encodedBytes Encoded bytes.
     * @return Zstd dictionary ID or type marker.
     */
    public static int onlyReadSpType(byte[] encodedBytes) {
        var buffer = ByteBuffer.wrap(encodedBytes);

        var firstByte = buffer.get(0);
        if (firstByte < 0) {
            return firstByte;
        } else {
            // Normal compressed value encoded.
            // Skip seq long and expire at long and key hash long.
            return buffer.getInt(8 + 8 + 8);
        }
    }

    /**
     * Encodes to bytes.
     *
     * @return Encoded bytes.
     */
    public byte[] encode() {
        var bytes = new byte[encodedLength()];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeLong(keyHash);
        buf.writeInt(dictSeqOrSpType);
        buf.writeInt(getCompressedLength());
        if (compressedData != null) {
            buf.write(compressedData);
        }
        return bytes;
    }

    /**
     * Encodes to the target buffer.
     *
     * @param buf Target buffer.
     */
    public void encodeTo(ByteBuf buf) {
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeLong(keyHash);
        buf.writeInt(dictSeqOrSpType);
        buf.writeInt(getCompressedLength());
        if (compressedData != null) {
            buf.write(compressedData);
        }
    }

    public void setCompressedDataAsBigString(long uuid, int dictSeq) {
        assert dictSeqOrSpType == SP_TYPE_BIG_STRING;
        // UUID + dict int.
        compressedData = new byte[12];
        ByteBuffer.wrap(compressedData).putLong(uuid).putInt(dictSeq);
    }

    /**
     * Gets the big string UUID from the encoded bytes.
     *
     * @return Big string UUID.
     */
    public long getBigStringMetaUuid() {
        return ByteBuffer.wrap(compressedData).getLong();
    }

    /**
     * Gets the big string UUID from the encoded bytes.
     *
     * @param encodedBytes Encoded bytes.
     * @return Big string UUID.
     */
    public static long getBigStringMetaUuid(byte[] encodedBytes) {
        assert encodedBytes.length >= 8 + 8 + 8 + 4 + 4 + 8;
        // sp type already is a big string
        var buffer = ByteBuffer.wrap(encodedBytes);
        assert buffer.getInt(8 + 8 + 8) == SP_TYPE_BIG_STRING;
        return buffer.getLong(8 + 8 + 8 + 4 + 4);
    }

    private static final Logger log = LoggerFactory.getLogger(CompressedValue.class);

    /**
     * Decodes from the buffer.
     * No memory copy when iterating over many compressed values.
     *
     * @param buf      Buffer.
     * @param keyBytes Key bytes.
     * @param keyHash  64-bit key hash.
     * @return Decoded compressed value.
     */
    public static CompressedValue decode(io.netty.buffer.ByteBuf buf, byte[] keyBytes, long keyHash) {
        var cv = new CompressedValue();
        var firstByte = buf.getByte(0);
        if (firstByte < 0) {
            cv.dictSeqOrSpType = firstByte;
            buf.skipBytes(1);
            cv.seq = buf.readLong();
            cv.compressedData = new byte[buf.readableBytes()];
            buf.readBytes(cv.compressedData);
            return cv;
        }

        cv.seq = buf.readLong();
        cv.expireAt = buf.readLong();
        cv.keyHash = buf.readLong();
        cv.dictSeqOrSpType = buf.readInt();

        if (keyHash == 0 && keyBytes != null) {
            keyHash = KeyHash.hash(keyBytes);
        }

        if (keyHash != 0 && cv.keyHash != keyHash) {
            var compressedLength = buf.readInt();
            if (compressedLength > 0) {
                buf.skipBytes(compressedLength);
            }

            // why ? todo: check
            log.warn("Key hash not match, key={}, seq={}, compressedLength={}, keyHash={}, persisted keyHash={}",
                    new String(keyBytes), cv.seq, compressedLength, keyHash, cv.keyHash);
            throw new IllegalStateException("Key hash not match, key=" + new String(keyBytes) +
                    ", seq=" + cv.seq +
                    ", compressedLength=" + compressedLength +
                    ", keyHash=" + keyHash +
                    ", persisted keyHash=" + cv.keyHash);
        }

        var compressedLength = buf.readInt();
        if (compressedLength > 0) {
            cv.compressedData = new byte[compressedLength];
            buf.readBytes(cv.compressedData);
        }
        return cv;
    }
}
