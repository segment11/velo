package io.velo.repl;

import io.netty.buffer.ByteBuf;
import io.velo.repl.content.RawBytesContent;
import io.velo.reply.Reply;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;

public class Repl {
    private Repl() {
    }

    public static final byte[] PROTOCOL_KEYWORD_BYTES = "X-REPL".getBytes();
    // 8 bytes for slaveUuid, 2 byte for slot, 2 byte for type, 4 bytes for length
    public static final int HEADER_LENGTH = PROTOCOL_KEYWORD_BYTES.length + 8 + 2 + 2 + 4;

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

    public record ReplReply(long slaveUuid, short slot, ReplType type, ReplContent content) implements Reply {
        @Override
        public io.activej.bytebuf.ByteBuf buffer() {
            if (content == BYTE_0_CONTENT) {
                return io.activej.bytebuf.ByteBuf.empty();
            }

            return Repl.buffer(slaveUuid, slot, type, content);
        }

        public boolean isReplType(ReplType type) {
            return this.type == type;
        }

        public boolean isEmpty() {
            return content == BYTE_0_CONTENT;
        }
    }

    public static ReplReply reply(short slot, ReplPair replPair, ReplType type, ReplContent content) {
        return new ReplReply(replPair.getSlaveUuid(), slot, type, content);
    }

    public static ReplReply error(short slot, ReplPair replPair, String errorMessage) {
        return reply(slot, replPair, ReplType.error, new RawBytesContent(errorMessage.getBytes()));
    }

    private static final byte[] NULL_BYTES = "null".getBytes();

    public static ReplReply error(short slot, long slaveUuid, String errorMessage) {
        return new ReplReply(slaveUuid, slot, ReplType.error,
                new RawBytesContent(errorMessage == null ? NULL_BYTES : errorMessage.getBytes()));
    }

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

    public static ReplReply emptyReply() {
        return EMPTY_REPLY;
    }

    public static byte[][] decode(ByteBuf buf) {
        // data length should > 0, so <= means no enough data
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
