package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.SocketInspectorTest
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class CGroupTest extends Specification {
    def _CGroup = new CGroup(null, null, null)
    final short slot = 0

    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        when:
        LocalPersist.instance.addOneSlotForTest2(slot)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def sCopyList = _CGroup.parseSlots('copy', data3, slotNumber)
        then:
        sCopyList.size() == 2

        when:
        def data2 = new byte[2][]
        sCopyList = _CGroup.parseSlots('copy', data2, slotNumber)
        then:
        sCopyList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def cGroup = new CGroup('client', data1, null)
        cGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'clusterx'
        cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'config'
        cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'copy'
        reply = cGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        cGroup.cmd = 'zzz'
        reply = cGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test client'() {
        given:
        def socket = SocketInspectorTest.mockTcpSocket()

        def cGroup = new CGroup(null, null, socket)
        cGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = cGroup.execute('client id')
        then:
        reply instanceof IntegerReply

        when:
        reply = cGroup.execute('client info')
        then:
        reply instanceof BulkReply

        when:
        reply = cGroup.execute('client reply on')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = cGroup.execute('client reply off')
        then:
        reply == EmptyReply.INSTANCE

        when:
        reply = cGroup.execute('client reply on xxx')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = cGroup.execute('client setinfo')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = cGroup.execute('client getname')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = cGroup.execute('client setname xxx')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = cGroup.execute('client getname')
        then:
        reply instanceof BulkReply

        when:
        reply = cGroup.execute('client getname xxx')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = cGroup.execute('client setname')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = cGroup.execute('client zzz')
        then:
        reply == NilReply.INSTANCE
    }

    def 'test clusterx'() {
        given:
        def cGroup = new CGroup(null, null, null)

        and:
        def loader = CachedGroovyClassLoader.instance
        def classpath = Utils.projectPath('/dyn/src')
        loader.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        when:
        def reply = cGroup.execute('clusterx setnodeid')
        then:
        // cluster disabled
        reply instanceof ErrorReply
    }

    def 'test config'() {
        given:
        def cGroup = new CGroup(null, null, null)

        and:
        def loader = CachedGroovyClassLoader.instance
        def classpath = Utils.projectPath('/dyn/src')
        loader.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        when:
        def reply = cGroup.execute('config zzz')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test copy'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def cGroup = new CGroup(null, null, null)
        cGroup.byPassGetSet = inMemoryGetSet
        cGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = cGroup.execute('copy a b')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = cGroup.execute('copy a b')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = cGroup.execute('copy a b replace_')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = cGroup.execute('copy a b replace')
        then:
        reply == IntegerReply.REPLY_1

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        LocalPersist.instance.addOneSlot(slot, eventloop)
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        cGroup.crossRequestWorker = true
        reply = cGroup.execute('copy a b replace')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = cGroup.execute('copy a b replace')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        reply = cGroup.execute('copy a b replace_')
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
