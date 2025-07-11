package io.velo.repl;

import java.io.Closeable;
import java.io.IOException;

public interface ActAsRaf extends Closeable {
    void setLength(long length) throws IOException;

    void seekForRead(long offset) throws IOException;

    void seekForWrite(long offset) throws IOException;

    void write(byte[] bytes) throws IOException;

    long length() throws IOException;

    int read(byte[] bytes) throws IOException;
}
