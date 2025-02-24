package io.velo.repl.incremental;

import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.DictMap;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a dictionary (Dict) operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XDict implements BinlogContent {
    private final String keyPrefixOrSuffix;
    private final Dict dict;

    /**
     * Retrieves the key prefix or suffix associated with this dictionary operation.
     *
     * @return The key prefix or suffix.
     */
    public String getKeyPrefixOrSuffix() {
        return keyPrefixOrSuffix;
    }

    /**
     * Retrieves the dictionary associated with this operation.
     *
     * @return The dictionary.
     */
    public Dict getDict() {
        return dict;
    }

    /**
     * Constructs a new XDict object with the specified key prefix or suffix and dictionary.
     *
     * @param keyPrefixOrSuffix The key prefix or suffix.
     * @param dict              The dictionary associated with the operation.
     */
    public XDict(String keyPrefixOrSuffix, Dict dict) {
        this.keyPrefixOrSuffix = keyPrefixOrSuffix;
        this.dict = dict;
    }

    private static final Logger log = LoggerFactory.getLogger(XDict.class);

    /**
     * Returns the type of this binlog content.
     *
     * @return The type of this binlog content.
     */
    @Override
    public Type type() {
        return BinlogContent.Type.dict;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return The total number of bytes required for encoding.
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 4 bytes for seq, 8 bytes for created time
        // 2 bytes for key prefix length, key prefix, 2 bytes for dict bytes length, dict bytes
        return 1 + 4 + 4 + 8 + 2 + keyPrefixOrSuffix.length() + 2 + dict.getDictBytes().length;
    }

    /**
     * Encodes this binlog content into a byte array, including the type byte and length check.
     *
     * @return The byte array representation of this binlog content.
     */
    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putInt(dict.getSeq());
        buffer.putLong(dict.getCreatedTime());
        buffer.putShort((short) keyPrefixOrSuffix.length());
        buffer.put(keyPrefixOrSuffix.getBytes());
        buffer.putShort((short) dict.getDictBytes().length);
        buffer.put(dict.getDictBytes());

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the encoded binlog content.
     * @return The decoded XDict object.
     * @throws IllegalStateException If the key prefix length is invalid or the encoded length does not match the expected length.
     */
    public static XDict decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var seq = buffer.getInt();
        var createdTime = buffer.getLong();
        var keyPrefixLength = buffer.getShort();

        if (keyPrefixLength > CompressedValue.KEY_MAX_LENGTH || keyPrefixLength <= 0) {
            throw new IllegalStateException("Key prefix length error, key prefix length=" + keyPrefixLength);
        }

        var keyPrefixBytes = new byte[keyPrefixLength];
        buffer.get(keyPrefixBytes);
        var keyPrefix = new String(keyPrefixBytes);
        var dictBytesLength = buffer.getShort();
        var dictBytes = new byte[dictBytesLength];
        buffer.get(dictBytes);

        var dict = new Dict();
        dict.setSeq(seq);
        dict.setCreatedTime(createdTime);
        dict.setDictBytes(dictBytes);

        var r = new XDict(keyPrefix, dict);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return r;
    }

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method logs the dictionary information and updates the global dictionary map.
     *
     * @param slot     The replication slot to which this content is applied.
     * @param replPair The repl pair associated with this replication session.
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl slave get dict, key prefix or suffix={}, seq={}", keyPrefixOrSuffix, dict.getSeq());
        // ignore slot, need sync
        var dictMap = DictMap.getInstance();
        synchronized (dictMap) {
            dictMap.putDict(keyPrefixOrSuffix, dict);
        }
    }
}