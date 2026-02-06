package io.velo.command;

import io.netty.buffer.ByteBuf;

public interface RDBImporter {
    void restore(ByteBuf nettyBuf, RDBCallback callback);
}
