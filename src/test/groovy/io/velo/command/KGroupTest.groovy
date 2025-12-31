package io.velo.command

import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.AsyncReply
import io.velo.reply.ErrorReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
import spock.lang.Specification

class KGroupTest extends Specification {
    def _KGroup = new KGroup(null, null, null)
    final short slot = 0

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _KGroup.parseSlots('kx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def kGroup = new KGroup('keys', data1, null)
        kGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = kGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        kGroup.cmd = 'zzz'
        reply = kGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test keys'() {
        given:
        def kGroup = new KGroup(null, null, null)
        kGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        10.times {
            def key = 'key:' + it
            kGroup.set(key, key.bytes)
        }
        def reply = kGroup.execute('keys key:.*')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && (result as MultiBulkReply).replies.length == 10
        }.result

        when:
        reply = kGroup.execute('keys key:')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        when:
        reply = kGroup.execute('keys *a')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        when:
        reply = kGroup.execute('keys ?a')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        when:
        reply = kGroup.execute('keys [a')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        when:
        reply = kGroup.execute('keys .a')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        when:
        reply = kGroup.execute('keys +a')
        then:
        reply == KGroup.ONLY_SUPPORT_PREFIX_MATCH_PATTERN

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
