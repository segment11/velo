package io.velo.repl.incremental;

import io.netty.buffer.Unpooled;
import io.velo.BaseCommand;
import io.velo.CompressedValue;
import io.velo.ConfForGlobal;
import io.velo.KeyHash;
import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a big string operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XBigStrings implements BinlogContent {
    private final long uuid;
    private final int bucketIndex;
    private final long keyHash;
    private final String key;
    private final byte[] cvEncoded;

    /**
     * Retrieves the unique identifier associated with this big string operation.
     *
     * @return the UUID of the operation
     */
    public long getUuid() {
        return uuid;
    }

    /**
     * Retrieves the bucket index associated with this big string operation.
     *
     * @return the bucket index
     */
    public int getBucketIndex() {
        return bucketIndex;
    }

    /**
     * Retrieves the key hash associated with this big string operation.
     *
     * @return the key hash
     */
    public long getKeyHash() {
        return keyHash;
    }

    /**
     * Retrieves the key associated with this big string operation.
     *
     * @return the key of the big string
     */
    public String getKey() {
        return key;
    }

    /**
     * Retrieves the encoded bytes of the compressed value associated with this big string operation.
     *
     * @return the encoded bytes of the compressed value
     */
    public byte[] getCvEncoded() {
        return cvEncoded;
    }

    /**
     * Constructs a new XBigStrings object with the specified UUID, key, and encoded compressed value.
     *
     * @param uuid        the unique identifier for this operation
     * @param bucketIndex the bucket index
     * @param keyHash     the key hash
     * @param key         the key associated with the big string
     * @param cvEncoded   the encoded bytes of the compressed value
     */
    public XBigStrings(long uuid, int bucketIndex, long keyHash, String key, byte[] cvEncoded) {
        this.uuid = uuid;
        this.bucketIndex = bucketIndex;
        this.keyHash = keyHash;
        this.key = key;
        this.cvEncoded = cvEncoded;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.big_strings;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 8 bytes for uuid, 4 bytes for bucket index, 8 bytes for key hash, 2 bytes for key length, key bytes
        // 4 bytes for cvEncoded length, cvEncoded bytes
        return 1 + 4 + 8 + 4 + 8 + 2 + key.length() + 4 + cvEncoded.length;
    }

    /**
     * Encodes this binlog content into a byte array, including the type byte and length check.
     *
     * @return the byte array representation of this binlog content
     */
    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putLong(uuid);
        buffer.putInt(bucketIndex);
        buffer.putLong(keyHash);
        buffer.putShort((short) key.length());
        buffer.put(key.getBytes());
        buffer.putInt(cvEncoded.length);
        buffer.put(cvEncoded);

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XBigStrings object
     * @throws IllegalStateException If the key length is invalid or the encoded length does not match the expected length.
     */
    public static XBigStrings decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var uuid = buffer.getLong();
        var bucketIndex = buffer.getInt();
        var keyHash = buffer.getLong();
        var keyLength = buffer.getShort();

        if (keyLength > CompressedValue.KEY_MAX_LENGTH || keyLength <= 0) {
            throw new IllegalStateException("Key length error, key length=" + keyLength);
        }

        var keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        var key = new String(keyBytes);

        var cvEncodedLength = buffer.getInt();
        var cvEncoded = new byte[cvEncodedLength];
        buffer.get(cvEncoded);

        var r = new XBigStrings(uuid, bucketIndex, keyHash, key, cvEncoded);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return r;
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the local storage with the new big string and adds the UUID to the list of big strings to fetch.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var keyHash = KeyHash.hash(key.getBytes());
        var cv = CompressedValue.decode(Unpooled.wrappedBuffer(cvEncoded), key.getBytes(), keyHash);

        var s = BaseCommand.slot(key, ConfForGlobal.slotNumber);
        var oneSlot = localPersist.oneSlot(s.slot());
        oneSlot.asyncExecute(() -> {
            oneSlot.put(key, s.bucketIndex(), cv);
        });

        // use remote bucket index
        replPair.addToFetchBigStringId(uuid, bucketIndex, keyHash, key);
    }
}