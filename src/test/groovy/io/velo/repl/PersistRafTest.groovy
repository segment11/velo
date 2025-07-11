package io.velo.repl

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class PersistRafTest extends Specification {
    def 'test all'() {
        given:
        def file = new File('/tmp/test_raf.dat')
        if (!file.exists()) {
            FileUtils.touch(file)
        }

        and:
        def raf = new PersistRaf(new RandomAccessFile(file, 'rw'))

        when:
        raf.length = 1024
        raf.seekForWrite(128)
        raf.write('hello'.bytes)
        then:
        raf.length() == 1024

        when:
        raf.seekForRead(128)
        def bytes = new byte[5]
        raf.read(bytes)
        then:
        bytes == 'hello'.bytes

        cleanup:
        raf.close()
        file.delete()
    }
}
