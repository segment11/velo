package io.velo.repl.incremental;

import io.netty.buffer.Unpooled;
import io.velo.CompressedValue;
import io.velo.ConfForSlot;
import io.velo.KeyHash;
import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

public class XBigStrings implements BinlogContent {
    private final long uuid;

    private final String key;
    private final byte[] cvEncoded;

    public long getUuid() {
        return uuid;
    }

    public String getKey() {
        return key;
    }

    public byte[] getCvEncoded() {
        return cvEncoded;
    }

    public XBigStrings(long uuid, String key, byte[] cvEncoded) {
        this.uuid = uuid;
        this.key = key;
        this.cvEncoded = cvEncoded;
    }

    @Override
    public Type type() {
        return Type.big_strings;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 8 bytes for uuid, 2 bytes for key length, key bytes
        // 4 bytes for cvEncoded length, cvEncoded bytes
        return 1 + 4 + 8 + 2 + key.length() + 4 + cvEncoded.length;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putLong(uuid);
        buffer.putShort((short) key.length());
        buffer.put(key.getBytes());
        buffer.putInt(cvEncoded.length);
        buffer.put(cvEncoded);

        return bytes;
    }

    public static XBigStrings decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var uuid = buffer.getLong();
        var keyLength = buffer.getShort();

        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length: " + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        var key = new String(keyBytes);

        var cvEncodedLength = buffer.getInt();
        var cvEncoded = new byte[cvEncodedLength];
        buffer.get(cvEncoded);

        var r = new XBigStrings(uuid, key, cvEncoded);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length: " + encodedLength);
        }
        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    @Override
    public void apply(short slot, ReplPair replPair) {
        var keyHash = KeyHash.hash(key.getBytes());
        var bucketIndex = KeyHash.bucketIndex(keyHash, ConfForSlot.global.confBucket.bucketsPerSlot);
        var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncoded), key.getBytes(), keyHash);

        var oneSlot = localPersist.oneSlot(slot);
        oneSlot.put(key, bucketIndex, cv);

        replPair.addToFetchBigStringUuid(uuid);
    }
}
