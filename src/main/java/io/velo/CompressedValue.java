package io.velo;

import com.github.luben.zstd.Zstd;
import io.activej.bytebuf.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CompressedValue {
    public static final long NO_EXPIRE = 0;
    public static final long EXPIRE_NOW = -1;
    public static final int NULL_DICT_SEQ = 0;
    public static final int SP_TYPE_NUM_BYTE = -1;
    public static final int SP_TYPE_NUM_SHORT = -2;
    public static final int SP_TYPE_NUM_INT = -4;
    public static final int SP_TYPE_NUM_LONG = -8;
    public static final int SP_TYPE_NUM_DOUBLE = -16;
    public static final int SP_TYPE_SHORT_STRING = -32;

    // for redis-benchmark or other benchmarks, -d 16 is common
    // if string length <= 16, usually key bucket one cell can store a short value, refer to key bucket ONE_CELL_LENGTH
    // need not write to chunk, less ssd write
    public static final int SP_TYPE_SHORT_STRING_MIN_LEN = 16;

    // need save as a singe file
    public static final int SP_TYPE_BIG_STRING = -64;

    // hyperloglog
    public static final int SP_TYPE_HLL = -96;

    public static final byte SP_FLAG_DELETE_TMP = -128;

    public static final int SP_TYPE_HH = -512;
    public static final int SP_TYPE_HASH = -1024;
    public static final int SP_TYPE_LIST = -2048;
    public static final int SP_TYPE_SET = -4096;
    public static final int SP_TYPE_ZSET = -8192;
    public static final int SP_TYPE_GEO = -8193;
    public static final int SP_TYPE_STREAM = -16384;
    public static final int SP_TYPE_BLOOM_BITMAP = -200;

    // change here to limit key size
    public static final short KEY_MAX_LENGTH = 256;
    // change here to limit value size
    // 8KB data compress should <= 4KB can store in one PAGE_SIZE
    public static final int VALUE_MAX_LENGTH = 65536;

    // seq long + expireAt long + dictSeq int + keyHash long + uncompressedLength int + cvEncodedLength int
    public static final int VALUE_HEADER_LENGTH = 8 + 8 + 4 + 8 + 4 + 4;
    // key length use short
    public static final int KEY_HEADER_LENGTH = 2;

    private long seq;

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    private long keyHash;

    public long getKeyHash() {
        return keyHash;
    }

    public void setKeyHash(long keyHash) {
        this.keyHash = keyHash;
    }

    // milliseconds
    private long expireAt = NO_EXPIRE;

    public long getExpireAt() {
        return expireAt;
    }

    public void setExpireAt(long expireAt) {
        this.expireAt = expireAt;
    }

    // dict seq or special type, is a union
    private int dictSeqOrSpType = NULL_DICT_SEQ;

    public int getDictSeqOrSpType() {
        return dictSeqOrSpType;
    }

    public void setDictSeqOrSpType(int dictSeqOrSpType) {
        this.dictSeqOrSpType = dictSeqOrSpType;
    }

    private int uncompressedLength;

    public int getUncompressedLength() {
        return uncompressedLength;
    }

    public void setUncompressedLength(int uncompressedLength) {
        this.uncompressedLength = uncompressedLength;
    }

    private int compressedLength;

    public int getCompressedLength() {
        return compressedLength;
    }

    public void setCompressedLength(int compressedLength) {
        this.compressedLength = compressedLength;
    }

    public boolean isTypeNumber() {
        return dictSeqOrSpType <= SP_TYPE_NUM_BYTE && dictSeqOrSpType >= SP_TYPE_NUM_DOUBLE;
    }

    public static boolean isTypeNumber(int spType) {
        return spType <= SP_TYPE_NUM_BYTE && spType >= SP_TYPE_NUM_DOUBLE;
    }

    public boolean isTypeDouble() {
        return dictSeqOrSpType == SP_TYPE_NUM_DOUBLE;
    }

    public static boolean isTypeDouble(int spType) {
        return spType == SP_TYPE_NUM_DOUBLE;
    }

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

    // not seq, may have a problem
    public byte[] encodeAsShortString() {
        return encodeAsShortString(seq, compressedData);
    }

    public static byte[] encodeAsShortString(long seq, byte[] data) {
        var buf = ByteBuffer.allocate(1 + 8 + data.length);
        buf.put((byte) SP_TYPE_SHORT_STRING);
        buf.putLong(seq);
        buf.put(data);
        return buf.array();
    }

    public static long getSeqFromNumberOrShortStringEncodedBytes(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong(1);
    }

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

    public boolean isBigString() {
        return dictSeqOrSpType == SP_TYPE_BIG_STRING;
    }

    public boolean isHash() {
        return dictSeqOrSpType == SP_TYPE_HH ||
                dictSeqOrSpType == SP_TYPE_HASH;
    }

    public static boolean isHash(int spType) {
        return spType == SP_TYPE_HH ||
                spType == SP_TYPE_HASH;
    }

    public boolean isList() {
        return dictSeqOrSpType == SP_TYPE_LIST;
    }

    public static boolean isList(int spType) {
        return spType == SP_TYPE_LIST;
    }

    public boolean isSet() {
        return dictSeqOrSpType == SP_TYPE_SET;
    }

    public static boolean isSet(int spType) {
        return spType == SP_TYPE_SET;
    }

    public boolean isZSet() {
        return dictSeqOrSpType == SP_TYPE_ZSET;
    }

    public static boolean isZSet(int spType) {
        return spType == SP_TYPE_ZSET;
    }

    public boolean isGeo() {
        return dictSeqOrSpType == SP_TYPE_GEO;
    }

    public static boolean isGeo(int spType) {
        return spType == SP_TYPE_GEO;
    }

    public boolean isStream() {
        return dictSeqOrSpType == SP_TYPE_STREAM;
    }

    public static boolean isStream(int spType) {
        return spType == SP_TYPE_STREAM;
    }

    public boolean isBloomFilter() {
        return dictSeqOrSpType == SP_TYPE_BLOOM_BITMAP;
    }

    public static boolean isBloomFilter(int spType) {
        return spType == SP_TYPE_BLOOM_BITMAP;
    }

    public boolean isHll() {
        return dictSeqOrSpType == SP_TYPE_HLL;
    }

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
                ", uncompressedLength=" + uncompressedLength +
                ", compressedLength=" + compressedLength +
                '}';
    }

    private byte[] compressedData;

    public byte[] getCompressedData() {
        return compressedData;
    }

    public void setCompressedData(byte[] compressedData) {
        this.compressedData = compressedData;
    }

    public boolean isExpired() {
        return expireAt != NO_EXPIRE && expireAt < System.currentTimeMillis();
    }

    public boolean noExpire() {
        return expireAt == NO_EXPIRE;
    }

    public boolean isCompressed() {
        return dictSeqOrSpType > NULL_DICT_SEQ;
    }

    public static boolean isTypeString(int spType) {
        // number type also use string type
        return spType >= SP_TYPE_BIG_STRING;
    }

    public boolean isTypeString() {
        // number type also use string type
        return dictSeqOrSpType >= SP_TYPE_BIG_STRING;
    }

    public boolean isUseDict() {
        return dictSeqOrSpType > NULL_DICT_SEQ;
    }

    public boolean isIgnoreCompression(byte[] data) {
        if (compressedData.length != data.length) {
            return false;
        }

        var compareMinBytesNum = Math.min(20, data.length);
        var buffer1 = ByteBuffer.wrap(data, 0, compareMinBytesNum);
        var buffer2 = ByteBuffer.wrap(compressedData, 0, compareMinBytesNum);
        return buffer1.equals(buffer2);
    }

    public byte[] decompress(Dict dict) {
        var dst = new byte[uncompressedLength];
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

    public static CompressedValue compress(byte[] data, Dict dict) {
        var cv = new CompressedValue();

        // memory copy too much, use direct buffer better
        var dst = new byte[((int) Zstd.compressBound(data.length))];
        int compressedSize;
        if (dict == null || dict == Dict.SELF_ZSTD_DICT) {
            compressedSize = (int) Zstd.compress(dst, data, Zstd.defaultCompressionLevel());
        } else {
            compressedSize = dict.compressByteArray(dst, 0, data, 0, data.length);
        }
        if (compressedSize <= 0) {
            throw new IllegalStateException("Compress error");
        }

        if (compressedSize > data.length) {
            // need not compress
            cv.compressedData = data;
            cv.compressedLength = data.length;
            return cv;
        }

        // if waste too much space, copy to another
        if (dst.length != compressedSize) {
            // use heap buffer
            // memory copy too much
            var newDst = new byte[compressedSize];
            System.arraycopy(dst, 0, newDst, 0, compressedSize);
            cv.compressedData = newDst;
        } else {
            cv.compressedData = dst;
        }

        cv.compressedLength = compressedSize;
        cv.uncompressedLength = data.length;
        return cv;
    }

    public boolean isShortString() {
        if (!isTypeString()) {
            return false;
        }
        return compressedData != null && compressedData.length <= CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN;
    }

    public static boolean isDeleted(byte[] encoded) {
        return encoded.length == 1 && encoded[0] == SP_FLAG_DELETE_TMP;
    }

    public int encodedLength() {
        return VALUE_HEADER_LENGTH + compressedLength;
    }

    public static long onlyReadSeq(byte[] encodedBytes) {
        var buffer = ByteBuffer.wrap(encodedBytes);

        long valueSeqCurrent;
        // refer to CompressedValue decode
        var firstByte = buffer.get(0);
        if (firstByte < 0) {
            valueSeqCurrent = buffer.position(1).getLong();
        } else {
            // normal compressed value encoded
            valueSeqCurrent = buffer.getLong();
        }
        return valueSeqCurrent;
    }

    public byte[] encode() {
        var bytes = new byte[encodedLength()];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(dictSeqOrSpType);
        buf.writeLong(keyHash);
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        if (compressedData != null && compressedLength > 0) {
            buf.write(compressedData);
        }
        return bytes;
    }

    public void encodeTo(ByteBuf buf) {
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(dictSeqOrSpType);
        buf.writeLong(keyHash);
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        if (compressedData != null && compressedLength > 0) {
            buf.write(compressedData);
        }
    }

    // encoded length = 8 + 8 + 4 + 8 + 4 + 4 + 12 = 48
    public byte[] encodeAsBigStringMeta(long uuid) {
        // uuid + dict int
        compressedLength = 8 + 4;
        compressedData = new byte[12];
        ByteBuffer.wrap(compressedData).putLong(uuid).putInt(dictSeqOrSpType);

        var bytes = new byte[encodedLength()];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeLong(seq);
        buf.writeLong(expireAt);
        buf.writeInt(SP_TYPE_BIG_STRING);
        buf.writeLong(keyHash);
        // big string raw bytes length
        buf.writeInt(uncompressedLength);
        buf.writeInt(compressedLength);
        buf.write(compressedData);

        return bytes;
    }

    public long getBigStringMetaUuid() {
        return ByteBuffer.wrap(compressedData).getLong();
    }

    private static final Logger log = LoggerFactory.getLogger(CompressedValue.class);

    // no memory copy when iterate decode many compressed values
    public static CompressedValue decode(io.netty.buffer.ByteBuf buf, byte[] keyBytes, long keyHash) {
        var cv = new CompressedValue();
        var firstByte = buf.getByte(0);
        if (firstByte < 0) {
            cv.dictSeqOrSpType = firstByte;
            buf.skipBytes(1);
            cv.seq = buf.readLong();
            cv.compressedData = new byte[buf.readableBytes()];
            buf.readBytes(cv.compressedData);
            cv.compressedLength = cv.compressedData.length;
            cv.uncompressedLength = cv.compressedLength;
            return cv;
        }

        cv.seq = buf.readLong();
        cv.expireAt = buf.readLong();
        cv.dictSeqOrSpType = buf.readInt();
        cv.keyHash = buf.readLong();

        if (keyHash == 0 && keyBytes != null) {
            keyHash = KeyHash.hash(keyBytes);
        }

        if (keyHash != 0 && cv.keyHash != keyHash) {
            cv.uncompressedLength = buf.readInt();
            cv.compressedLength = buf.readInt();
            if (cv.compressedLength > 0) {
                buf.skipBytes(cv.compressedLength);
            }

            // why ? todo: check
            log.warn("Key hash not match, key={}, seq={}, uncompressedLength={}, cvEncodedLength={}, keyHash={}, persisted keyHash={}",
                    new String(keyBytes), cv.seq, cv.uncompressedLength, cv.compressedLength, keyHash, cv.keyHash);
            throw new IllegalStateException("Key hash not match, key=" + new String(keyBytes) +
                    ", seq=" + cv.seq +
                    ", uncompressedLength=" + cv.uncompressedLength +
                    ", cvEncodedLength=" + cv.compressedLength +
                    ", keyHash=" + keyHash +
                    ", persisted keyHash=" + cv.keyHash);
        }

        cv.uncompressedLength = buf.readInt();
        cv.compressedLength = buf.readInt();
        if (cv.compressedLength > 0) {
            cv.compressedData = new byte[cv.compressedLength];
            buf.readBytes(cv.compressedData);
        }
        return cv;
    }
}
