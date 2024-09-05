package io.velo.repl.incremental;

import io.velo.CompressedValue;
import io.velo.persist.LocalPersist;
import io.velo.persist.Wal;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;

public class XWalV implements BinlogContent {
    private final Wal.V v;
    private final boolean isValueShort;
    private final int offset;

    public Wal.V getV() {
        return v;
    }

    public boolean isValueShort() {
        return isValueShort;
    }

    public int getOffset() {
        return offset;
    }

    public XWalV(Wal.V v, boolean isValueShort, int offset) {
        this.v = v;
        this.isValueShort = isValueShort;
        this.offset = offset;
    }

    @TestOnly
    public XWalV(Wal.V v) {
        this(v, true, 0);
    }

    @Override
    public BinlogContent.Type type() {
        return BinlogContent.Type.wal;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 1 byte for is value short, 4 bytes as int for offset
        // 8 bytes for seq, 4 bytes for bucket index, 8 bytes for key hash, 8 bytes for expire at, 4 bytes for sp type
        // 2 bytes for key length, key bytes, 4 bytes for cv encoded length, cv encoded bytes
        return 1 + 4 + 1 + 4 + 8 + 4 + 8 + 8 + 4 + 2 + v.key().length() + 4 + v.cvEncoded().length;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.put(isValueShort ? (byte) 1 : (byte) 0);
        buffer.putInt(offset);
        buffer.putLong(v.seq());
        buffer.putInt(v.bucketIndex());
        buffer.putLong(v.keyHash());
        buffer.putLong(v.expireAt());
        buffer.putInt(v.spType());
        buffer.putShort((short) v.key().length());
        buffer.put(v.key().getBytes());
        buffer.putInt(v.cvEncoded().length);
        buffer.put(v.cvEncoded());

        return bytes;
    }

    public static XWalV decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var isValueShort = buffer.get() == 1;
        var offset = buffer.getInt();
        var seq = buffer.getLong();
        var bucketIndex = buffer.getInt();
        var keyHash = buffer.getLong();
        var expireAt = buffer.getLong();
        var spType = buffer.getInt();
        var keyLength = buffer.getShort();

        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        var cvEncodedLength = buffer.getInt();
        var cvEncoded = new byte[cvEncodedLength];
        buffer.get(cvEncoded);

        var v = new Wal.V(seq, bucketIndex, keyHash, expireAt, spType, new String(keyBytes), cvEncoded, false);
        var r = new XWalV(v, isValueShort, offset);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length: " + encodedLength);
        }
        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);
        var targetWal = oneSlot.getWalByBucketIndex(v.bucketIndex());
        targetWal.putFromX(v, isValueShort, offset);

        replPair.setSlaveCatchUpLastSeq(v.seq());
    }
}
