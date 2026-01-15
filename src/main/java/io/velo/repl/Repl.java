package io.velo.repl;

import io.netty.buffer.ByteBuf;
import io.velo.repl.content.RawBytesContent;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;

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
     * @param slaveUuid the unique identifier of the slave node
     * @param slot      the slot index associated with the message
     * @param type      the type of the REPL message
     * @param content   the content of the REPL message
     * @return the byte buffer containing the encoded message
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
     * Represents a reply message from bytes in the REPL protocol.
     * Implements the {@link Reply} interface.
     */
    public record ReplReplyFromBytes(long slaveUuid, short slot, ReplType type, byte[] data,
                                     int offset, int length) implements Reply {
        @Override
        public io.activej.bytebuf.ByteBuf buffer() {
            var bytes = new byte[HEADER_LENGTH + length];
            var buf = io.activej.bytebuf.ByteBuf.wrapForWriting(bytes);

            buf.write(PROTOCOL_KEYWORD_BYTES);
            buf.writeLong(slaveUuid);
            buf.writeShort(slot);
            buf.writeShort(type.code);
            buf.writeInt(length);
            buf.write(data, offset, length);
            return buf;
        }
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
         * @param type the type to compare with
         * @return true if the types match, false otherwise
         */
        public boolean isReplType(ReplType type) {
            return this.type == type;
        }

        /**
         * Checks if the reply message content is empty.
         *
         * @return true if the content is empty, false otherwise
         */
        public boolean isEmpty() {
            return content == BYTE_0_CONTENT;
        }
    }

    /**
     * Creates a REPL reply message.
     *
     * @param slot     the slot index associated with the message
     * @param replPair the pair containing the slave UUID
     * @param type     the type of the REPL message
     * @param content  the content of the REPL message
     * @return the REPL reply message
     */
    public static ReplReply reply(short slot, ReplPair replPair, ReplType type, ReplContent content) {
        return new ReplReply(replPair.getSlaveUuid(), slot, type, content);
    }

    /**
     * Creates a REPL error message.
     *
     * @param slot         the slot index associated with the message
     * @param replPair     the pair containing the slave UUID
     * @param errorMessage the error message to include in the reply
     * @return the REPL error message
     */
    public static ReplReply error(short slot, ReplPair replPair, String errorMessage) {
        return reply(slot, replPair, ReplType.error, new RawBytesContent(errorMessage.getBytes()));
    }

    private static final byte[] NULL_BYTES = "null".getBytes();

    /**
     * Creates a REPL error message.
     *
     * @param slot         the slot index associated with the message
     * @param slaveUuid    the unique identifier of the slave node
     * @param errorMessage the error message to include in the reply
     * @return the REPL error message
     */
    public static ReplReply error(short slot, long slaveUuid, String errorMessage) {
        return new ReplReply(slaveUuid, slot, ReplType.error,
                new RawBytesContent(errorMessage == null ? NULL_BYTES : errorMessage.getBytes()));
    }

    /**
     * Creates a test REPL message for testing purposes.
     *
     * @param slot     the slot index associated with the message
     * @param replPair the pair containing the slave UUID
     * @param message  the test message to include in the reply
     * @return the test REPL message
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
     * @return the empty REPL reply message
     */
    public static ReplReply emptyReply() {
        return EMPTY_REPLY;
    }

    /**
     * Decodes a byte buffer into a REPL message data array.
     *
     * @param buf the buffer containing the encoded REPL message
     * @return a decoded {@link ReplRequest} or null if there aren't enough bytes to form a complete request
     */
    public static ReplRequest decode(ByteBuf buf) {
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

        var readLength = Math.min(buf.readableBytes(), dataLength);
        if (readLength != 0) {
            var bytes = new byte[readLength];
            buf.readBytes(bytes);
            var r = new ReplRequest(slaveUuid, slot, replType, bytes);
            r.dataLength = readLength;
            return r;
        } else {
            var r = new ReplRequest(slaveUuid, slot, replType, new byte[0]);
            r.dataLength = dataLength;
            return r;
        }
    }
}