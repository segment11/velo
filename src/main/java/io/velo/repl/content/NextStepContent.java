package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

/**
 * Represents a special content type in the REPL (slave-master-replication) system indicating that the system should proceed to the next step.
 * This content type is used to signal the end of the current operation or input and the readiness to accept the next input.
 * It is represented by a single byte with the value 0.
 */
public class NextStepContent implements ReplContent {

    /**
     * A singleton instance of NextStepContent. This instance should be used instead of creating new instances.
     */
    public static final NextStepContent INSTANCE = new NextStepContent();

    /**
     * Private constructor to prevent instantiation from outside this class.
     * Use {@link #INSTANCE} to get the singleton instance of NextStepContent.
     */
    private NextStepContent() {
    }

    /**
     * Checks if the given byte array represents a NextStepContent.
     * A byte array represents a NextStepContent if it contains exactly one byte and that byte is 0.
     *
     * @param contentBytes the byte array to check
     * @return true if the byte array represents a NextStepContent, false otherwise
     */
    public static boolean isNextStep(byte[] contentBytes) {
        return contentBytes.length == 1 && contentBytes[0] == 0;
    }

    /**
     * Encodes this NextStepContent to the provided ByteBuf.
     * This method writes a single byte with the value 0 to the ByteBuf.
     *
     * @param toBuf the ByteBuf to write to
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    /**
     * Returns the length of the encoded form of this NextStepContent.
     * Since this content type is represented by a single byte, the length is always 1.
     *
     * @return the length of the encoded form of this NextStepContent
     */
    @Override
    public int encodeLength() {
        return 1;
    }
}