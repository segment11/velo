package io.velo.persist;

import io.activej.bytebuf.ByteBuf;
import io.velo.CompressedValue;
import org.jetbrains.annotations.VisibleForTesting;

import static io.velo.CompressedValue.NO_EXPIRE;

public class PersistValueMeta {
    // 2 bytes for 0 if not special type + type int + segment sub block index byte
    // + length int + segment index int + segment offset int
    // may add type or other metadata in the future
    @VisibleForTesting
    static final int ENCODED_LENGTH = 2 + 4 + 2 + 4 + 4 + 4;

    // CompressedValue encoded length is much more than PersistValueMeta encoded length
    public static boolean isPvm(byte[] bytes) {
        // short string encoded
        // first byte is type, < 0 means special type
        return bytes[0] >= 0 && (bytes.length == ENCODED_LENGTH);
    }

    int spType = CompressedValue.NULL_DICT_SEQ;
    byte subBlockIndex;
    public int length;
    int segmentIndex;
    int segmentOffset;
    // need remove expired pvm in key loader to compress better, or reduce split
    long expireAt = NO_EXPIRE;
    long seq;

    // for reset compare with old CompressedValue
    byte[] keyBytes;
    long keyHash;
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
        return "l=" + length + ", si=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
    }

    @Override
    public String toString() {
        return "PersistValueMeta{" +
                "spType=" + spType +
                ", length=" + length +
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
        buf.writeInt(spType);
        buf.writeShort(subBlockIndex);
        buf.writeInt(length);
        buf.writeInt(segmentIndex);
        buf.writeInt(segmentOffset);
        return bytes;
    }

    public static PersistValueMeta decode(byte[] bytes) {
        var buf = ByteBuf.wrapForReading(bytes);
        var pvm = new PersistValueMeta();
        buf.readShort(); // skip 0
        pvm.spType = buf.readInt();
        pvm.subBlockIndex = (byte) buf.readShort();
        pvm.length = buf.readInt();
        pvm.segmentIndex = buf.readInt();
        pvm.segmentOffset = buf.readInt();
        return pvm;
    }

}
