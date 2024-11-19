package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

public interface Reply {
    ByteBuf buffer();

    default ByteBuf bufferAsResp3() {
        return buffer();
    }

    default ByteBuf bufferAsHttp() {
        return null;
    }

    @TestOnly
    default boolean dumpForTest(StringBuilder sb, int nestCount) {
        return true;
    }
}
