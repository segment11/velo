package io.velo.repl

import spock.lang.Specification

class PureMemoryRafTest extends Specification {
    def 'test all'() {
        given:
        def raf = new PureMemoryRaf(0, new byte[1024])

        expect:
        raf.fileIndex() == 0
        raf.name == Binlog.FILE_NAME_PREFIX + 0

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
        raf.delete()
    }
}
