package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.MultiWorkerServer
import io.velo.SocketInspector
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
        int slotNumber = 128
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        and:
        LocalPersist.instance.addOneSlotForTest2(slot)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        expect:
        _CGroup.parseSlots('copy', data3, slotNumber).size() == 2

        and: 'insufficient data'
        def data2 = new byte[2][]
        _CGroup.parseSlots('copy', data2, slotNumber).size() == 0
    }

    def 'test handle'() {
        given:
        def cGroup = new CGroup(null, null, null)
        cGroup.from(BaseCommand.mockAGroup())

        expect:
        cGroup.execute(input) == expected

        where:
        input      | expected
        'client'   | ErrorReply.FORMAT
        'clusterx' | ErrorReply.FORMAT
        'config'   | ErrorReply.FORMAT
        'copy'     | ErrorReply.FORMAT
        'zzz'      | NilReply.INSTANCE
    }

    def 'test client'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = new long[]{Thread.currentThread().threadId()}
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = new long[]{Thread.currentThread().threadId()}

        def socket = SocketInspectorTest.mockTcpSocket()
        LocalPersist.instance.setSocketInspector(inspector)

        def cGroup = new CGroup(null, null, socket)
        cGroup.from(BaseCommand.mockAGroup())

        def reply

        // ----- basic client subcommands -----

        when:
        reply = cGroup.execute('client id')
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
        reply == ErrorReply.FORMAT

        when:
        reply = cGroup.execute('client setinfo lib-name Jedis lib-ver 4.3')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = cGroup.execute('client setinfo lib-version 1.0')
        then:
        reply == ErrorReply.SYNTAX

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

        // ----- CLIENT KILL (Redis Sentinel reconfiguration path) -----

        when: 'CLIENT KILL without args — format error'
        reply = cGroup.execute('client kill')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT KILL with unsupported filter clause'
        reply = cGroup.execute('client kill id 123')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with unknown type — must be explicit, not silently accepted'
        reply = cGroup.execute('client kill type whatever')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL TYPE normal — the only filter Sentinel needs'
        // Real-world scenario: the slot worker thread that runs this command is NOT the
        // socket's owning net worker reactor thread. A direct s.close() would throw
        // "Not in reactor thread"; a reactor.submit(s::close) succeeds.
        def netWorkerEventloop = Eventloop.builder()
                .withThreadName('client-kill-test-net')
                .withIdleInterval(Duration.ofMillis(50))
                .build()
        netWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.sleep(200)
        def otherSocket = SocketInspectorTest.mockTcpSocket(netWorkerEventloop)
        // wire inspector so close() triggers onDisconnect and removes the socket from socketMap
        otherSocket.setInspector(inspector)
        inspector.onConnect(otherSocket)
        reply = cGroup.execute('client kill type normal')
        then:
        reply instanceof IntegerReply
        // the only non-issuing normal socket must have been queued for close
        (reply as IntegerReply).integer == 1

        and: 'after the owning reactor processes the close, the non-issuing socket is removed'
        // Poll briefly for the async onDisconnect to propagate through the background reactor
        def deadline = System.currentTimeMillis() + 2000
        while (inspector.socketMap.size() != 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20)
        }
        // issuing socket was never added; other normal socket has been removed via onDisconnect
        inspector.socketMap.size() == 0

        cleanup:
        netWorkerEventloop.breakEventloop()
        if (inspector.socketMap.containsValue(otherSocket)) {
            inspector.onDisconnect(otherSocket)
        }
        inspector.onDisconnect(socket)
        LocalPersist.instance.setSocketInspector(null)
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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = cGroup.execute('copy a b replace')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        reply = cGroup.execute('copy a b replace_')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
