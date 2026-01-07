package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import io.velo.persist.BigStringFiles
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
        List<BigStringFiles.IdWithKey> idListInMaster = []
        List<BigStringFiles.IdWithKey> sentIdList = []

        def content = new ToSlaveExistsBigString(0, bigStringDir, idListInMaster, sentIdList)

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
        idListInMaster << new BigStringFiles.IdWithKey(1L, 0, 1L, "")
        idListInMaster << new BigStringFiles.IdWithKey(2L, 0, 2L, "")
        idListInMaster << new BigStringFiles.IdWithKey(3L, 0, 3L, "")
        idListInMaster << new BigStringFiles.IdWithKey(4L, 0, 4L, "")
        sentIdList << new BigStringFiles.IdWithKey(1L, 0, 1L, "")
        sentIdList << new BigStringFiles.IdWithKey(2L, 0, 2L, "")
        content = new ToSlaveExistsBigString(0, bigStringDir, idListInMaster, sentIdList)
        then:
        content.encodeLength() == 9

        when:
        def subDir = new File(bigStringDir, '0')
        subDir.mkdir()
        new File(subDir, '1_1').text = '1' * 10
        new File(subDir, '3_3').text = '3' * 30
        content = new ToSlaveExistsBigString(0, bigStringDir, idListInMaster, sentIdList)
        then:
        content.encodeLength() == 9 + (8 + 8 + 4) * 1 + 30

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
        buffer.getLong() == 3L
        buffer.getInt() == 30

        when:
        idListInMaster.clear()
        (ToSlaveExistsBigString.ONCE_SEND_BIG_STRING_COUNT * 2).times {
            idListInMaster << new BigStringFiles.IdWithKey(it, 0, it, "")
            new File(bigStringDir, 0 + '/' + it + '_' + it).text = it.toString() * 10
        }
        content = new ToSlaveExistsBigString(0, bigStringDir, idListInMaster, sentIdList)
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
