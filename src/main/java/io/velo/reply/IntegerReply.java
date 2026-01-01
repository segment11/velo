package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents an integer reply in the Velo protocol.
 * This class implements the {@link Reply} interface and provides methods to handle integer replies.
 */
public class IntegerReply implements Reply {

    private static final byte[] b0 = "0".getBytes();
    private static final byte[] b1 = "1".getBytes();

    /**
     * Represents an integer reply with the value 1.
     */
    public static final Reply REPLY_1 = new Reply() {
        @Override
        public ByteBuf buffer() {
            return integer1ByteBuf.slice();
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(b1);
        }

        @TestOnly
        @Override
        public boolean dumpForTest(StringBuilder sb, int nestCount) {
            sb.append("1");
            return true;
        }
    };

    /**
     * Represents an integer reply with the value 0.
     */
    public static final Reply REPLY_0 = new Reply() {
        @Override
        public ByteBuf buffer() {
            return integer0ByteBuf.slice();
        }

        @Override
        public ByteBuf bufferAsHttp() {
            return ByteBuf.wrapForReading(b0);
        }

        @TestOnly
        @Override
        public boolean dumpForTest(StringBuilder sb, int nestCount) {
            sb.append("0");
            return true;
        }
    };

    private static final ByteBuf integer1ByteBuf = bufferPreload(1L);
    private static final ByteBuf integer0ByteBuf = bufferPreload(0L);

    private static final byte MARKER = ':';

    /**
     * Returns the integer value stored in this reply.
     *
     * @return the integer value
     */
    public long getInteger() {
        return integer;
    }

    private final long integer;

    /**
     * Constructs an instance of IntegerReply with the specified integer value.
     *
     * @param integer the integer value to be stored in this reply
     */
    public IntegerReply(long integer) {
        this.integer = integer;
    }

    /**
     * Preloads the buffer with the specified integer value.
     *
     * @param x the integer value to be preloaded
     * @return the ByteBuf containing the preloaded integer value
     */
    static ByteBuf bufferPreload(Long x) {
        if (x == null) {
            return NilReply.INSTANCE.buffer();
        }

        var sizeBytes = BulkReply.numToBytes(x, true);
        int len = 1 + sizeBytes.length;

        var buf = ByteBuf.wrapForWriting(new byte[len]);
        buf.writeByte(MARKER);
        buf.write(sizeBytes);
        return buf;
    }

    /**
     * Returns a ByteBuf containing the stored integer value.
     *
     * @return the ByteBuf containing the integer value
     */
    @Override
    public ByteBuf buffer() {
        return bufferPreload(integer);
    }

    /**
     * Returns a ByteBuf containing the stored integer value formatted as a string for HTTP use.
     *
     * @return the ByteBuf containing the integer value formatted as a string
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(Long.toString(integer).getBytes());
    }

    /**
     * Dumps the integer value to the provided StringBuilder for testing purposes.
     *
     * @param sb        the StringBuilder to which the integer value will be appended
     * @param nestCount the nesting level for formatting purposes (unused in this method)
     * @return true indicating successful operation
     */
    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append(integer);
        return true;
    }
}