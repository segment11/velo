package io.velo.command;

import io.netty.buffer.ByteBuf;

public interface RDBImporter {
    void restore(ByteBuf buf, RDBCallback callback);
}
