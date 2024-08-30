package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

public class NextStepContent implements ReplContent {
    public static final NextStepContent INSTANCE = new NextStepContent();

    private NextStepContent() {
    }

    public static boolean isNextStep(byte[] contentBytes) {
        return contentBytes.length == 1 && contentBytes[0] == 0;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeByte((byte) 0);
    }

    @Override
    public int encodeLength() {
        return 1;
    }
}
