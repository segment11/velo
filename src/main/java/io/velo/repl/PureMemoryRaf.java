package io.velo.repl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import static io.velo.repl.Binlog.FILE_NAME_PREFIX;

public class PureMemoryRaf implements ActAsRaf, ActAsFile {
    private final int fileIndex;
    private byte[] data;
    private ByteBuf buf;
    private int maxLength;

    public PureMemoryRaf(int fileIndex, byte[] data) {
        this.fileIndex = fileIndex;
        this.data = data;
        this.buf = Unpooled.wrappedBuffer(data);
    }

    @Override
    public void setLength(long length) {
        maxLength = (int) length;
    }

    @Override
    public void seekForRead(long offset) {
        buf.readerIndex((int) offset);
    }

    @Override
    public void seekForWrite(long offset) {
        buf.writerIndex((int) offset);
    }

    @Override
    public void write(byte[] bytes) {
        buf.writeBytes(bytes);
        maxLength = Math.max(maxLength, buf.writerIndex());
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        var readableN = buf.readableBytes();
        if (readableN == 0) {
            return 0;
        }

        if (readableN < bytes.length) {
            buf.readBytes(bytes, 0, readableN);
            return readableN;
        } else {
            buf.readBytes(bytes);
            return bytes.length;
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int fileIndex() {
        return fileIndex;
    }

    @Override
    public boolean delete() {
        this.data = null;
        this.buf = null;
        return true;
    }

    @Override
    public long length() {
        return maxLength;
    }

    @Override
    public String getName() {
        return FILE_NAME_PREFIX + fileIndex;
    }
}
