package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import io.velo.persist.Consts
import spock.lang.Specification

import java.nio.ByteBuffer

class ToSlaveExistsBigStringTest extends Specification {
    def 'test all'() {
        given:
        def bigStringDir = new File(Consts.slotDir, 'big-string')
        if (!bigStringDir.exists()) {
            bigStringDir.mkdirs()
        }
        List<Long> uuidListInMaster = []
        List<Long> sentUuidList = []

        def content = new ToSlaveExistsBigString(0, bigStringDir, uuidListInMaster, sentUuidList)

        expect:
        content.encodeLength() == 9

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0
        buffer.getInt() == 0
        buffer.get() == (byte) 1

        when:
        uuidListInMaster << 1L
        uuidListInMaster << 2L
        uuidListInMaster << 3L
        uuidListInMaster << 4L
        sentUuidList << 1L
        sentUuidList << 2L
        content = new ToSlaveExistsBigString(0, bigStringDir, uuidListInMaster, sentUuidList)
        then:
        content.encodeLength() == 9

        when:
        def subDir = new File(bigStringDir, '0')
        subDir.mkdir()
        new File(subDir, '1').text = '1' * 10
        new File(subDir, '3').text = '3' * 30
        content = new ToSlaveExistsBigString(0, bigStringDir, uuidListInMaster, sentUuidList)
        then:
        content.encodeLength() == 9 + (8 + 4) * 1 + 30

        when:
        bytes = new byte[content.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0
        buffer.getInt() == 1
        buffer.get() == (byte) 1
        buffer.getLong() == 3L
        buffer.getInt() == 30

        when:
        uuidListInMaster.clear()
        (ToSlaveExistsBigString.ONCE_SEND_BIG_STRING_COUNT * 2).times {
            uuidListInMaster << (it as long)
            new File(bigStringDir, 0 + '/' + it).text = it.toString() * 10
        }
        content = new ToSlaveExistsBigString(0, bigStringDir, uuidListInMaster, sentUuidList)
        bytes = new byte[content.encodeLength()]
        buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0
        buffer.getInt() == ToSlaveExistsBigString.ONCE_SEND_BIG_STRING_COUNT
        buffer.get() == (byte) 0

        cleanup:
        bigStringDir.deleteDir()
    }
}
