package io.velo.repl.incremental

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XWalRewriteTest extends Specification {
    def 'test encode and decode'() {
        given:
        def bytes = new byte[100 + 4]
        def x = new XWalRewrite(false, 0, bytes)
        def x2 = new XWalRewrite(true, 0, bytes)

        expect:
        x.type() == BinlogContent.Type.wal_rewrite
        x.isSkipWhenAllSlavesInCatchUpState()
        x.encodedLength() == 14 + 104

        when:
        def encoded = x.encodeWithType()
        def encoded2 = x2.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def x11 = XWalRewrite.decodeFrom(buffer)
        def buffer2 = ByteBuffer.wrap(encoded2)
        buffer2.get()
        def x22 = XWalRewrite.decodeFrom(buffer2)
        then:
        x11.encodedLength() == encoded.length
        x22.encodedLength() == encoded2.length

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            XWalRewrite.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        x.apply(slot, replPair)
        x2.apply(slot, replPair)
        def wal = localPersist.oneSlot(slot).getWalByGroupIndex(0)
        then:
        wal.writePosition == 100
        wal.writePositionShortValue == 100

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
