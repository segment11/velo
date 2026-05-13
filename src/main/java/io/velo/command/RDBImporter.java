package io.velo.command;

import io.netty.buffer.ByteBuf;

/**
 * Interface for RDB restore operations.
 */
public interface RDBImporter {
    /**
     * Restores data from RDB format.
     *
     * @param nettyBuf the buffer containing RDB data
     * @param callback the callback for each imported value
     */
    void restore(ByteBuf nettyBuf, RDBCallback callback);
}
