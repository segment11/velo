package io.velo.persist;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.VisibleForTesting;

import static io.velo.CompressedValue.NO_EXPIRE;

public class PersistValueMeta {
    // 2 bytes for 0 means not a compress value (number or short string), short type byte + segment sub block index byte
    // + segment index int + segment offset int
    // may other metadata in the future
    @VisibleForTesting
    static final int ENCODED_LENGTH = 2 + 1 + 1 + 4 + 4;

    // CompressedValue encoded length is much more than PersistValueMeta encoded length
    public static boolean isPvm(byte[] bytes) {
        // short string encoded
        // first byte is type, < 0 means number byte or short string
        return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
    }

    // for scan filter, need not read chunk segment
    byte shortType = KeyLoader.typeAsByteString;
    // if segment can be compressed, one chunk segment includes 1-4 compressed block(segment)s
    byte subBlockIndex;
    int segmentIndex;
    int segmentOffset;
    // need remove expired pvm in key loader to compress better, or reduce split
    long expireAt = NO_EXPIRE;
    long seq;

    // for reset compare with old CompressedValue
    byte[] keyBytes;
    long keyHash;
    int keyHash32;
    int bucketIndex;

    byte[] extendBytes;

    boolean isFromMerge;

    int cellCostInKeyBucket() {
        var valueLength = extendBytes != null ? extendBytes.length : ENCODED_LENGTH;
        if (valueLength > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Persist value meta extend bytes too long=" + valueLength);
        }
        return KeyBucket.KVMeta.calcCellCount((short) keyBytes.length, (byte) valueLength);
    }

    boolean isTargetSegment(int segmentIndex, byte subBlockIndex, int segmentOffset) {
        return this.segmentIndex == segmentIndex && this.subBlockIndex == subBlockIndex && this.segmentOffset == segmentOffset;
    }

    public String shortString() {
        return "si=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
    }

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "shortType=" + shortType +
                ", segmentIndex=" + segmentIndex +
                ", subBlockIndex=" + subBlockIndex +
                ", segmentOffset=" + segmentOffset +
                ", expireAt=" + expireAt +
                '}';
    }

    public byte[] encode() {
        var bytes = new byte[ENCODED_LENGTH];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeShort((short) 0);
        buf.writeByte(shortType);
        buf.writeByte(subBlockIndex);
        buf.writeInt(segmentIndex);
        buf.writeInt(segmentOffset);
        return bytes;
    }

    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        buf.readShort(); // skip 0
        pvm.shortType = buf.readByte();
        pvm.subBlockIndex = buf.readByte();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();
        return pvm;
    }
}
