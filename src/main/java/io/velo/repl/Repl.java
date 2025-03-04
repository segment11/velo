package io.velo.repl;

import io.netty.buffer.ByteBuf;
import io.velo.repl.content.RawBytesContent;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;

/**
 * Represents a REPL (slave-master-replication) protocol for communication.
 * This class provides methods to encode and decode messages according to the custom REPL protocol.
 */
public class Repl {
    private Repl() {
        // Private constructor to prevent instantiation
    }

    /**
     * Protocol keyword used to identify REPL messages.
     */
    public static final byte[] PROTOCOL_KEYWORD_BYTES = "X-REPL".getBytes();

    /**
     * Length of the header in the REPL protocol.
     * Includes protocol keyword, slave uuid long + slot short + type short + content length int.
     */
    public static final int HEADER_LENGTH = PROTOCOL_KEYWORD_BYTES.length + 8 + 2 + 2 + 4;

    /**
     * Creates a buffer containing the encoded REPL message.
     *
     * @param slaveUuid Unique identifier of the slave node.
     * @param slot      Slot number associated with the message.
     * @param type      Type of the REPL message.
     * @param content   Content of the REPL message.
     * @return Byte buffer containing the encoded message.
     */
    public static io.activej.bytebuf.ByteBuf buffer(long slaveUuid, short slot, ReplType type, ReplContent content) {
        var encodeLength = content.encodeLength();

        var bytes = new byte[HEADER_LENGTH + encodeLength];
        var buf = io.activej.bytebuf.ByteBuf.wrapForWriting(bytes);

        buf.write(PROTOCOL_KEYWORD_BYTES);
        buf.writeLong(slaveUuid);
        buf.writeShort(slot);
        buf.writeShort(type.code);
        buf.writeInt(encodeLength);
        content.encodeTo(buf);
        return buf;
    }

    /**
     * Represents a reply message in the REPL protocol.
     * Implements the {@link Reply} interface.
     */
    public record ReplReply(long slaveUuid, short slot, ReplType type, ReplContent content) implements Reply {
        @Override
        public io.activej.bytebuf.ByteBuf buffer() {
            if (content == BYTE_0_CONTENT) {
                return io.activej.bytebuf.ByteBuf.empty();
            }

            return Repl.buffer(slaveUuid, slot, type, content);
        }

        /**
         * Checks if the reply message type matches the specified type.
         *
         * @param type Type to compare with.
         * @return True if the types match, false otherwise.
         */
        public boolean isReplType(ReplType type) {
            return this.type == type;
        }

        /**
         * Checks if the reply message content is empty.
         *
         * @return True if the content is empty, false otherwise.
         */
        public boolean isEmpty() {
            return content == BYTE_0_CONTENT;
        }
    }

    /**
     * Creates a REPL reply message.
     *
     * @param slot     Slot number associated with the message.
     * @param replPair Pair containing the slave UUID.
     * @param type     Type of the REPL message.
     * @param content  Content of the REPL message.
     * @return REPL reply message.
     */
    public static ReplReply reply(short slot, ReplPair replPair, ReplType type, ReplContent content) {
        return new ReplReply(replPair.getSlaveUuid(), slot, type, content);
    }

    /**
     * Creates a REPL error message.
     *
     * @param slot         Slot number associated with the message.
     * @param replPair     Pair containing the slave UUID.
     * @param errorMessage Error message to include in the reply.
     * @return REPL error message.
     */
    public static ReplReply error(short slot, ReplPair replPair, String errorMessage) {
        return reply(slot, replPair, ReplType.error, new RawBytesContent(errorMessage.getBytes()));
    }

    private static final byte[] NULL_BYTES = "null".getBytes();

    /**
     * Creates a REPL error message.
     *
     * @param slot         Slot number associated with the message.
     * @param slaveUuid    Unique identifier of the slave node.
     * @param errorMessage Error message to include in the reply.
     * @return REPL error message.
     */
    public static ReplReply error(short slot, long slaveUuid, String errorMessage) {
        return new ReplReply(slaveUuid, slot, ReplType.error,
                new RawBytesContent(errorMessage == null ? NULL_BYTES : errorMessage.getBytes()));
    }

    /**
     * Creates a test REPL message for testing purposes.
     *
     * @param slot     Slot number associated with the message.
     * @param replPair Pair containing the slave UUID.
     * @param message  Test message to include in the reply.
     * @return Test REPL message.
     */
    @TestOnly
    public static ReplReply test(short slot, ReplPair replPair, String message) {
        return reply(slot, replPair, ReplType.test, new RawBytesContent(message.getBytes()));
    }

    private static final ReplContent BYTE_0_CONTENT = new ReplContent() {
        @Override
        public void encodeTo(io.activej.bytebuf.ByteBuf toBuf) {
        }

        @Override
        public int encodeLength() {
            return 0;
        }
    };

    private static final ReplReply EMPTY_REPLY = new ReplReply(0L, (byte) 0, null, BYTE_0_CONTENT);

    /**
     * Returns an empty REPL reply message.
     *
     * @return Empty REPL reply message.
     */
    public static ReplReply emptyReply() {
        return EMPTY_REPLY;
    }

    /**
     * Decodes a byte buffer into a REPL message data array.
     *
     * @param buf Buffer containing the encoded REPL message.
     * @return Array of byte arrays containing the decoded message data (slave UUID, slot, type, content), or null if decoding fails.
     */
    public static byte[][] decode(ByteBuf buf) {
        if (buf.readableBytes() <= HEADER_LENGTH) {
            return null;
        }
        buf.skipBytes(PROTOCOL_KEYWORD_BYTES.length);

        var slaveUuid = buf.readLong();
        var slot = buf.readShort();

        if (slot < 0) {
            throw new IllegalArgumentException("Repl slot should be positive");
        }

        var replType = ReplType.fromCode((byte) buf.readShort());
        if (replType == null) {
            return null;
        }

        var dataLength = buf.readInt();
        if (buf.readableBytes() < dataLength) {
            return null;
        }

        // 4 bytes arrays, first is slaveUuid, second is slot, third is replType, fourth is content data
        var data = new byte[4][];
        data[0] = new byte[8];
        ByteBuffer.wrap(data[0]).putLong(slaveUuid);

        data[1] = new byte[2];
        ByteBuffer.wrap(data[1]).putShort(slot);

        data[2] = new byte[1];
        data[2][0] = replType.code;

        var bytes = new byte[dataLength];
        buf.readBytes(bytes);
        data[3] = bytes;

        return data;
    }
}