package io.velo.repl.incremental;

import io.velo.command.AGroup;
import io.velo.persist.Wal;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import io.velo.reply.ErrorReply;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the binary log content for ACL user changes in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XAclUpdate implements BinlogContent {
    private final List<String> lines;

    /**
     * Constructs an XAclUpdate with a single line.
     *
     * @param line the line of this ACL update
     */
    public XAclUpdate(String line) {
        this.lines = List.of(line);
    }

    /**
     * Constructs an XAclUpdate with multiple lines.
     *
     * @param lines the list of ACL command lines
     */
    public XAclUpdate(ArrayList<String> lines) {
        this.lines = lines;
    }

    /**
     * Returns the type of this binlog content.
     *
     * @return the type of this binlog content
     */
    @Override
    public Type type() {
        return Type.acl_update;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return the total number of bytes required for encoding
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check, 4 bytes for number of lines
        int total = 1 + 4 + 4;
        for (var line : lines) {
            total += 4; // line length
            total += Wal.keyBytes(line).length;
        }
        return total;
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
        buffer.putInt(lines.size());

        for (var line : lines) {
            var lineBytes = Wal.keyBytes(line);
            buffer.putInt(lineBytes.length);
            buffer.put(lineBytes);
        }

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the encoded binlog content
     * @return the decoded XAclUpdate object
     * @throws IllegalStateException If the encoded content is invalid.
     */
    public static XAclUpdate decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();
        var lineCount = buffer.getInt();
        var lines = new ArrayList<String>(lineCount);

        for (int i = 0; i < lineCount; i++) {
            var lineLength = buffer.getInt();
            var lineBytes = new byte[lineLength];
            buffer.get(lineBytes);
            lines.add(Wal.keyString(lineBytes));
        }

        var r = new XAclUpdate(lines);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }

        return r;
    }

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method updates the local configuration with the new dynamic configuration settings.
     *
     * @param slot     the replication slot to which this content is applied
     * @param replPair the repl pair associated with this replication session
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var aGroup = new AGroup("acl", null, null);
        for (var line : lines) {
            var reply = aGroup.execute(line);
            if (reply instanceof ErrorReply e) {
                throw new RuntimeException("Apply acl update error: " + e.getMessage());
            }
        }
    }
}