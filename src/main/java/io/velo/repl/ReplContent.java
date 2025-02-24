package io.velo.repl;

import io.activej.bytebuf.ByteBuf;

/**
 * An interface representing the content of a replication message in the Velo REPL system.
 */
public interface ReplContent {
    /**
     * Decodes the content from the given buffer.
     *
     * @param toBuf the buffer to decode from
     */
    void encodeTo(ByteBuf toBuf);

    /**
     * Calculates the length of the encoded content.
     *
     * @return the length of the encoded content
     */
    int encodeLength();
}
