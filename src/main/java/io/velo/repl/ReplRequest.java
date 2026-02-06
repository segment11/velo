package io.velo.repl;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a request message from bytes in the REPL protocol.
 */
public class ReplRequest {
    private long slaveUuid;
    private short slot;
    private ReplType type;
    private int expectLength;
    private byte[] data;

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
        return data;
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
        this.slaveUuid = slaveUuid;
        this.slot = slot;
        this.type = type;
        this.data = data;
        this.expectLength = expectLength;
        assert data.length <= expectLength;
    }

    /**
     * Checks if the request is fully read.
     *
     * @return true if the request is fully read, false otherwise
     */
    public boolean isFullyRead() {
        return data.length == expectLength;
    }

    /**
     * Gets the number of bytes left to read.
     *
     * @return the number of bytes left to read
     */
    public int leftToRead() {
        return expectLength - data.length;
    }

    /**
     * Updates the request with the next bytes to read.
     *
     * @param nettyBuf the source of bytes to read
     * @param n        the number of bytes to read
     */
    public void nextRead(ByteBuf nettyBuf, int n) {
        assert n > 0;
        var dataExtend = new byte[data.length + n];
        System.arraycopy(data, 0, dataExtend, 0, data.length);
        nettyBuf.readBytes(dataExtend, data.length, n);
        data = dataExtend;
    }

    /**
     * Creates a copy of the request.
     *
     * @return a copy of the request
     */
    public ReplRequest copyShadow() {
        return new ReplRequest(slaveUuid, slot, type, data, expectLength);
    }
}
