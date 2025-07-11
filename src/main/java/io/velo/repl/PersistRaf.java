package io.velo.repl;

import java.io.IOException;
import java.io.RandomAccessFile;

public class PersistRaf implements ActAsRaf {
    private final RandomAccessFile raf;

    public PersistRaf(RandomAccessFile raf) {
        this.raf = raf;
    }

    @Override
    public void setLength(long length) throws IOException {
        raf.setLength(length);
    }

    @Override
    public void seekForRead(long offset) throws IOException {
        raf.seek(offset);
    }

    @Override
    public void seekForWrite(long offset) throws IOException {
        raf.seek(offset);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        raf.write(bytes);
    }

    @Override
    public long length() throws IOException {
        return raf.length();
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        return raf.read(bytes);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
