package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

/**
 * Represents raw byte content in a slave-master-replication system.
 * This class encapsulates a byte array that is encoded into a byte buffer
 * for transmission between the slave and master nodes.
 */
public class RawBytesContent implements ReplContent {
    private final byte[] bytes;

    /**
     * Constructs a new RawBytesContent instance with the specified byte array.
     *
     * @param bytes a byte array containing the raw bytes to be sent
     * @throws IllegalArgumentException if the byte array is null or empty
     */
    public RawBytesContent(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Repl raw bytes cannot be null");
        }
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Repl raw bytes cannot be empty");
        }

        this.bytes = bytes;
    }

    /**
     * Encodes the raw byte content into the provided ByteBuf.
     * This method writes the byte array to the byte buffer
     * to be sent from the slave to the master node.
     *
     * @param toBuf the ByteBuf to write the raw byte content to
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(bytes);
    }

    /**
     * Returns the length of the encoded raw byte content.
     * This method provides the length of the byte array,
     * which is useful for encoding and decoding purposes.
     *
     * @return the length of the encoded raw byte content
     */
    @Override
    public int encodeLength() {
        return bytes.length;
    }

    @Override
    public String toString() {
        return new String(bytes);
    }
}