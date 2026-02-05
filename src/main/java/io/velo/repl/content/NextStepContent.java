package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

/**
 * Signals end of current operation, ready to accept next input. Single byte with value 0.
 */
public class NextStepContent implements ReplContent {
    public static final NextStepContent INSTANCE = new NextStepContent();

    private NextStepContent() {
    }

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