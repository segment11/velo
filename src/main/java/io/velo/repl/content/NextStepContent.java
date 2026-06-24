package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

/**
 * Signals end of current operation, ready to accept next input.
 */
public class NextStepContent implements ReplContent {
    /**
     * Singleton instance signaling readiness for the next operation.
     */
    public static final NextStepContent INSTANCE = new NextStepContent();

    private NextStepContent() {
    }

    /**
     * Checks whether the given content bytes represent a NextStepContent message.
     *
     * @param contentBytes the content bytes to check
     * @return true if the bytes represent a NextStepContent message
     */
    public static boolean isNextStep(byte[] contentBytes) {
        return contentBytes.length == 1 && contentBytes[0] == 0;
    }

    /**
     * Encodes this NextStepContent to the provided ByteBuf.
     * Writes a single byte with value 0.
     *
     * @param toBuf the ByteBuf to write to
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    /**
     * Returns the length of the encoded form of this NextStepContent.
     *
     * @return the length of the encoded form (always 1 byte)
     */
    @Override
    public int encodeLength() {
        return 1;
    }
}