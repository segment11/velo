package io.velo.persist

import com.kenai.jffi.MemoryIO
import com.kenai.jffi.PageManager
import jnr.constants.platform.OpenFlags
import jnr.ffi.LastError
import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import spock.lang.Specification

class TestJnrPwrite extends Specification {
    public static final int PROTECTION = PageManager.PROT_READ | PageManager.PROT_WRITE | PageManager.PROT_EXEC

    def 'test pwrite and pread'() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')

        def libC = LibraryLoader.create(LibC.class).load('c')
        String filePath = 'test_jnr_libc'
        def file = new File(filePath)
        if (file.exists()) {
            def isDeleted = file.delete()
            if (!isDeleted) {
                throw new IOException('file delete failed')
            }
        }
        def isCreated = file.createNewFile()
        if (!isCreated) {
            throw new IOException('file create failed')
        }

        and:
        int pages = 1
        def m = MemoryIO.getInstance()
        def pageManager = PageManager.getInstance()
        def pageSize = pageManager.pageSize()
        println 'page size=' + pageSize
        def addr = pageManager.allocatePages(pages, PROTECTION)
        def addr2 = pageManager.allocatePages(pages, PROTECTION)
        println 'addr=' + addr
        println 'addr % page size=' + addr % pageSize

        expect:
        addr % pageSize == 0

        when:
        def buf = m.newDirectByteBuffer(addr, (int) pageSize * pages)
        def buf2 = m.newDirectByteBuffer(addr2, (int) pageSize * pages)
        println 'buf remaining=' + buf.remaining()
        // read
        def bytes = new byte[1024]
        buf.get(bytes)
        println 'bytes length=' + bytes.length
        then:
        bytes.length == 1024

        when:
        // write
        buf.flip()
        def valueBytes = 'hello'.bytes
        buf.put(valueBytes)
        // read again
        buf.rewind()
        buf.mark()
        def valueRead = new byte[valueBytes.length]
        buf.get(valueRead)
        println 'value read=' + new String(valueRead)
        then:
        valueRead == valueBytes

        when:
        buf.reset()
        def fd = libC.open(filePath, FdReadWrite.O_DIRECT | OpenFlags.O_RDWR.value(), 0644)
        if (fd < 0) {
            def systemRuntime = jnr.ffi.Runtime.systemRuntime
            def errno = LastError.getLastError(systemRuntime)
            println 'open error=' + libC.strerror(errno)
            throw new IOException('open failed')
        }
        def n = libC.pwrite(fd, buf, buf.capacity(), 0)
        println 'pwrite=' + n
        if (n == -1) {
            def systemRuntime = jnr.ffi.Runtime.systemRuntime
            def errno = LastError.getLastError(systemRuntime)
            println 'pwrite error=' + libC.strerror(errno)
        }
        then:
        n == buf.capacity()

        when:
        buf2.clear()
        libC.pread(fd, buf2, buf2.capacity(), 0)
        buf2.rewind()
        def valueRead2 = new byte[valueBytes.length]
        buf2.get(valueRead2)
        println 'pread bytes=' + new String(valueRead2)
        then:
        valueRead2 == valueBytes

        cleanup:
        pageManager.freePages(addr, pages)
        pageManager.freePages(addr2, pages)
        libC.close(fd)
    }
}
