package io.velo.repl;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Represents a request message from bytes in the REPL protocol.
 */
public class ReplRequest {
    private long slaveUuid;
    private short slot;
    private ReplType type;
    private int expectLength;
    private byte[] data;
    private int dataLength;

    /**
     * Gets the slave's UUID.
     *
     * @return the slave's UUID
     */
    public long getSlaveUuid() {
        return slaveUuid;
    }

    /**
     * Gets the slot index.
     *
     * @return the slot index
     */
    public short getSlot() {
        return slot;
    }

    /**
     * Gets the replication type.
     *
     * @return the replication type
     */
    public ReplType getType() {
        return type;
    }

    /**
     * Gets the data bytes.
     *
     * @return the data bytes
     */
    public byte[] getData() {
        return isFullyRead() ? data : Arrays.copyOf(data, dataLength);
    }

    @TestOnly
    public void setSlaveUuid(long slaveUuid) {
        this.slaveUuid = slaveUuid;
    }

    @TestOnly
    public void setSlot(short slot) {
        this.slot = slot;
    }

    @TestOnly
    public void setType(ReplType type) {
        this.type = type;
    }

    @TestOnly
    public void setData(byte[] data) {
        this.data = data;
        this.dataLength = data.length;
        this.expectLength = data.length;
    }

    /**
     * Constructs a new ReplRequest.
     *
     * @param slaveUuid    the slave's UUID
     * @param slot         the slot index
     * @param type         the replication type
     * @param data         the data bytes
     * @param expectLength the expected length of the data bytes
     */
    public ReplRequest(long slaveUuid, short slot, ReplType type, byte[] data, int expectLength) {
        if (expectLength <= 0) {
            throw new IllegalArgumentException("Repl request expected length should be positive");
        }
        if (data.length > expectLength) {
            throw new IllegalArgumentException("Repl request data length exceeds expected length");
        }

        this.slaveUuid = slaveUuid;
        this.slot = slot;
        this.type = type;
        this.expectLength = expectLength;
        this.dataLength = data.length;

        if (data.length == expectLength) {
            this.data = data;
        } else {
            this.data = new byte[expectLength];
            System.arraycopy(data, 0, this.data, 0, data.length);
        }
    }

    /**
     * Checks if the request is fully read.
     *
     * @return true if the request is fully read, false otherwise
     */
    public boolean isFullyRead() {
        return dataLength == expectLength;
    }

    /**
     * Gets the number of bytes left to read.
     *
     * @return the number of bytes left to read
     */
    public int leftToRead() {
        return expectLength - dataLength;
    }

    /**
     * Updates the request with the next bytes to read.
     *
     * @param nettyBuf the source of bytes to read
     * @param n        the number of bytes to read
     */
    public void nextRead(ByteBuf nettyBuf, int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("Repl request next read length should be positive");
        }
        if (dataLength + n > expectLength) {
            throw new IllegalArgumentException("Repl request next read exceeds expected length");
        }
        nettyBuf.readBytes(data, dataLength, n);
        dataLength += n;
    }

    /**
     * Creates a copy of the request.
     *
     * @return a copy of the request
     */
    public ReplRequest copyShadow() {
        return new ReplRequest(slaveUuid, slot, type, getData(), expectLength);
    }
}
