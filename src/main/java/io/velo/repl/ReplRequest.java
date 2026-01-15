package io.velo.repl;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.TestOnly;

public class ReplRequest {
    long slaveUuid;
    short slot;
    ReplType type;
    int dataLength;
    byte[] data;

    public long getSlaveUuid() {
        return slaveUuid;
    }

    public short getSlot() {
        return slot;
    }

    public ReplType getType() {
        return type;
    }

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
        this.dataLength = data.length;
    }

    public ReplRequest(long slaveUuid, short slot, ReplType type, byte[] data) {
        this.slaveUuid = slaveUuid;
        this.slot = slot;
        this.type = type;
        this.dataLength = data.length;
        this.data = data;
    }

    public boolean isFullyRead() {
        return data.length == dataLength;
    }

    public int leftToRead() {
        return dataLength - data.length;
    }

    public void nextRead(ByteBuf buf, int n) {
        var dataExtend = new byte[data.length + n];
        System.arraycopy(data, 0, dataExtend, 0, data.length);
        buf.readBytes(dataExtend, data.length, n);
    }
}
