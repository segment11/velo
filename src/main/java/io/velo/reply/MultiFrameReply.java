package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.Slice;
import org.jetbrains.annotations.TestOnly;

/**
 * A reply that serializes as several complete RESP frames back to back, like the redis pubsub
 * confirmation messages: one frame per channel. RESP frames are self delimiting, so writing the
 * concatenated bytes in one buffer is indistinguishable on the wire from writing each frame
 * separately, which is what redis does for a multi-channel SUBSCRIBE / UNSUBSCRIBE.
 */
public class MultiFrameReply implements Reply {

    /** The frames, each one a complete reply. */
    private final Reply[] frames;

    /**
     * @return the frames
     */
    public Reply[] getFrames() {
        return frames;
    }

    /**
     * @param frames the replies, each serialized as its own complete frame
     */
    public MultiFrameReply(Reply... frames) {
        this.frames = frames;
    }

    @Override
    public ByteBuf buffer() {
        var slice = new Slice();
        for (var frame : frames) {
            var subBuffer = frame.buffer();
            slice.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
        }
        return ByteBuf.wrap(slice.getArray(), 0, slice.getWriteIndex());
    }

    @Override
    public ByteBuf bufferAsResp3() {
        var slice = new Slice();
        for (var frame : frames) {
            var subBuffer = frame.bufferAsResp3();
            slice.writeBytes(subBuffer.array(), subBuffer.head(), subBuffer.tail() - subBuffer.head());
        }
        return ByteBuf.wrap(slice.getArray(), 0, slice.getWriteIndex());
    }

    @Override
    public ByteBuf bufferAsHttp() {
        // like MultiBulkReply: the RESP text with the crlf folded to lf
        var buf = buffer();
        var string = new String(buf.array(), buf.head(), buf.tail() - buf.head());
        return ByteBuf.wrapForReading(string.replaceAll("\r\n", "\n").getBytes());
    }

    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        for (var frame : frames) {
            frame.dumpForTest(sb, nestCount);
        }
        return true;
    }
}
