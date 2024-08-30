package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

import java.nio.ByteBuffer

class ToSlaveExistsBigStringTest extends Specification {
    def 'test all'() {
        given:
        def bigStringDir = new File(io.velo.persist.Consts.slotDir, 'big-string')
        if (!bigStringDir.exists()) {
            bigStringDir.mkdirs()
        }
        List<Long> uuidListInMaster = []
        List<Long> sentUuidList = []

        def content = new ToSlaveExistsBigString(bigStringDir, uuidListInMaster, sentUuidList)

        expect:
        content.encodeLength() == 5

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0
        buffer.get() == 1

        when:
        uuidListInMaster << 1L
        uuidListInMaster << 2L
        uuidListInMaster << 3L
        uuidListInMaster << 4L
        sentUuidList << 1L
        sentUuidList << 2L
        content = new ToSlaveExistsBigString(bigStringDir, uuidListInMaster, sentUuidList)
        then:
        content.encodeLength() == 5

        when:
        new File(bigStringDir, '1').text = '1' * 10
        new File(bigStringDir, '3').text = '3' * 30
        content = new ToSlaveExistsBigString(bigStringDir, uuidListInMaster, sentUuidList)
        then:
        content.encodeLength() == 5 + (8 + 4) * 1 + 30

        when:
        bytes = new byte[content.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 1
        buffer.get() == 1
        buffer.getLong() == 3L
        buffer.getInt() == 30

        when:
        uuidListInMaster.clear()
        (ToSlaveExistsBigString.ONCE_SEND_BIG_STRING_COUNT * 2).times {
            uuidListInMaster << (it as long)
            new File(bigStringDir, it.toString()).text = it.toString() * 10
        }
        content = new ToSlaveExistsBigString(bigStringDir, uuidListInMaster, sentUuidList)
        bytes = new byte[content.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == ToSlaveExistsBigString.ONCE_SEND_BIG_STRING_COUNT
        buffer.get() == 0

        cleanup:
        bigStringDir.deleteDir()
    }
}
