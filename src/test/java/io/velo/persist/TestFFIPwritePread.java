package io.velo.persist;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.PageManager;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.LibraryLoader;
import jnr.posix.LibC;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

public class TestFFIPwritePread {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(TestFFIPwritePread.class);

    public static void main(String[] args) throws IOException {
        System.setProperty("jnr.ffi.asm.enabled", "false");

        var libC = LibraryLoader.create(LibC.class).load("c");
        String filePath = "/tmp/test_jnr_libc";
        var file = new File(filePath);
        FileUtils.touch(file);
        var fd = libC.open(filePath, LocalPersist.O_DIRECT | OpenFlags.O_WRONLY.value(), 00644);

        var m = MemoryIO.getInstance();
        var pageManager = PageManager.getInstance();

        var pageSize = pageManager.pageSize();
        log.info("page size={}", pageSize);
        final int pages = 2;

        var addr = pageManager.allocatePages(pages, LocalPersist.PROTECTION);
        log.info("addr={}", addr);
        log.info("addr % page size={}", addr % pageSize);

        var buf = m.newDirectByteBuffer(addr, (int) pageSize * pages);
        log.info("buf remaining={}", buf.remaining());

        // read
        byte[] bytes = new byte[1024];
        buf.get(bytes);
        log.info("bytes length={}", bytes.length);

        // write
        buf.flip();
        byte[] valueBytes = "hello".getBytes();
        buf.put(valueBytes);

        // read again
        buf.rewind();
        buf.mark();
        byte[] valueRead = new byte[valueBytes.length];
        buf.get(valueRead);
        log.info("value read={}", new String(valueRead));

        buf.reset();
        int n = libC.pwrite(fd, buf, buf.capacity(), 0);
        log.info("pwrite={}", n);

        buf.clear();
        libC.pread(fd, buf, buf.capacity(), 0);
        buf.rewind();
        byte[] valueRead2 = new byte[valueBytes.length];
        buf.get(valueRead2);
        log.info("pread bytes={}", new String(valueRead2));

        pageManager.freePages(addr, pages);
        libC.close(fd);
    }
}
