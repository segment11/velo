package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a reply that can be sent as a response in different protocols.
 */
public interface Reply {
    /**
     * Returns the underlying buffer containing the reply data.
     *
     * @return the buffer with the reply data
     */
    ByteBuf buffer();

    /**
     * Returns the buffer containing the reply data formatted as RESP3.
     * By default, it returns the same buffer as {@link #buffer()}.
     *
     * @return the buffer with the reply data formatted as RESP3
     */
    default ByteBuf bufferAsResp3() {
        return buffer();
    }

    /**
     * @return the buffer formatted as HTTP, or null if not supported
     */
    default ByteBuf bufferAsHttp() {
        return null;
    }

    /**
     * @param sb        the StringBuilder to append the dump to
     * @param nestCount the nesting level for formatting
     * @return true if successful
     */
    @TestOnly
    default boolean dumpForTest(StringBuilder sb, int nestCount) {
        return true;
    }

    @TestOnly
    class DumpReply implements Reply {
        @Override
        public ByteBuf buffer() {
            return null;
        }
    }

    @TestOnly
    static final Reply DUMP_REPLY = new DumpReply();
}