package io.velo.repl.incremental;

import io.velo.command.AGroup;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import io.velo.reply.ErrorReply;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for ACL user changes in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XAclUpdate implements BinlogContent {
    private final String line;

    /**
     * Constructs an XAclUpdate with the provided line.
     *
     * @param line The line of this ACL update.
     */
    public XAclUpdate(String line) {
        this.line = line;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return The type of this binlog content.
     */
    @Override
    public Type type() {
        return Type.acl_update;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return The total number of bytes required for encoding.
     */
    @Override
    public int encodedLength() {
        assert line != null;
        // 1 byte for type, 4 bytes for encoded length for check
        return 1 + 4 + 4 + line.length();
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
        buffer.putInt(line.length());
        buffer.put(line.getBytes());

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the encoded binlog content.
     * @return The decoded XAclUpdate object.
     * @throws IllegalStateException If the encoded content is invalid.
     */
    public static XAclUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();
        var lineLength = buffer.getInt();
        var lineBytes = new byte[lineLength];
        buffer.get(lineBytes);
        var line = new String(lineBytes);

        var r = new XAclUpdate(line);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }

        return r;
    }

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the local configuration with the new dynamic configuration settings.
     *
     * @param slot     The replication slot to which this content is applied.
     * @param replPair The repl pair associated with this replication session.
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var aGroup = new AGroup("acl", null, null);
        var reply = aGroup.execute(line);
        if (reply instanceof ErrorReply e) {
            throw new RuntimeException("Apply acl update error: " + e.getMessage());
        }
    }
}