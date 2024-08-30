package io.velo.reply;

import io.activej.bytebuf.ByteBuf;

public interface Reply {
    ByteBuf buffer();

    default ByteBuf bufferAsHttp() {
        return null;
    }
}
