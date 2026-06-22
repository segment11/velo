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
        def fixture = clientFixture()
        def cGroup = fixture.cGroup
        def socket = fixture.socket
        def inspector = fixture.inspector

        // Track extra sockets we add during the test so we can clean them all up
        // in a single method-level cleanup block (Spock does not allow per-when
        // cleanup blocks).
        def extras = []
        def eventloopsToBreak = []

        def reply

        // ----- arity / format errors -----

        when: 'no subcommand — format error'
        reply = cGroup.execute('client')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT ID with extra args is rejected'
        reply = cGroup.execute('client id extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT INFO with extra args is rejected'
        reply = cGroup.execute('client info extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT GETNAME with extra args is rejected'
        reply = cGroup.execute('client getname extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT SETNAME with no name is rejected'
        reply = cGroup.execute('client setname')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT REPLY with no mode is rejected'
        reply = cGroup.execute('client reply')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT REPLY with extra args is rejected'
        reply = cGroup.execute('client reply on xxx')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT SETINFO with no fields is rejected'
        reply = cGroup.execute('client setinfo')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT SETINFO with unknown option is rejected'
        reply = cGroup.execute('client setinfo lib-version 1.0')
        then:
        reply == ErrorReply.SYNTAX

        // ----- metadata subcommands -----

        when: 'CLIENT ID returns the monotonic id stamped at connect'
        reply = cGroup.execute('client id')
        then:
        reply instanceof IntegerReply
        // The fixture registered the issuing socket, so it has a monotonic id >= 1
        (reply as IntegerReply).integer >= 1L
        (reply as IntegerReply).integer == SocketInspector.createUserDataIfNotSet(socket).getClientId()

        when: 'CLIENT INFO returns a bulk reply with id=, addr=, user=default'
        reply = cGroup.execute('client info')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString().contains('id=')
        (reply as BulkReply).asString().contains('addr=')
        (reply as BulkReply).asString().contains('user=default')

        when: 'CLIENT REPLY on returns OK'
        reply = cGroup.execute('client reply on')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT REPLY off returns EmptyReply'
        reply = cGroup.execute('client reply off')
        then:
        reply == EmptyReply.INSTANCE

        when: 'CLIENT SETINFO lib-name + lib-ver is accepted'
        reply = cGroup.execute('client setinfo lib-name Jedis lib-ver 4.3')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT GETNAME with no prior SETNAME returns nil'
        reply = cGroup.execute('client getname')
        then:
        reply == NilReply.INSTANCE

        when: 'CLIENT SETNAME persists a client name'
        reply = cGroup.execute('client setname xxx')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT GETNAME returns the persisted name'
        reply = cGroup.execute('client getname')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == 'xxx'

        // ----- CLIENT LIST (Task 2) -----

        when: 'CLIENT LIST with extra args is rejected (no filters supported yet)'
        reply = cGroup.execute('client list extra')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT LIST returns a bulk reply containing one line per socket, including the issuing socket'
        reply = cGroup.execute('client list')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString().contains('id=')
        (reply as BulkReply).asString().contains('addr=')
        (reply as BulkReply).asString().contains('user=default')
        (reply as BulkReply).asString().contains('name=xxx')
        // Exactly one line for the only registered socket (the issuing one)
        (reply as BulkReply).asString().split('\n').length == 1

        when: 'CLIENT LIST also reports an extra registered socket'
        def secondSocket = SocketInspectorTest.mockTcpSocket(null, 46410)
        secondSocket.setInspector(inspector)
        inspector.onConnect(secondSocket)
        extras << secondSocket
        reply = cGroup.execute('client list')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString().split('\n').length == 2

        // ----- CLIENT KILL filter parsing (Task 3) -----

        when: 'CLIENT KILL with no filter args is format error'
        reply = cGroup.execute('client kill')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT KILL with only a filter name is syntax error'
        reply = cGroup.execute('client kill id')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with a non-numeric id is syntax error'
        reply = cGroup.execute('client kill id not-a-number')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with unsupported type is syntax error'
        reply = cGroup.execute('client kill type whatever')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with invalid SKIPME value is syntax error'
        reply = cGroup.execute('client kill skipme maybe')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with negative MAXAGE is syntax error'
        reply = cGroup.execute('client kill maxage -1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with non-numeric MAXAGE is syntax error'
        reply = cGroup.execute('client kill maxage abc')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with no id/value is a syntax error (legacy form needs ip:port)'
        reply = cGroup.execute('client kill type')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with unknown filter name is syntax error'
        reply = cGroup.execute('client kill bogus value')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL with odd number of filter args is syntax error'
        reply = cGroup.execute('client kill id 1 extra')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT KILL ADDR with no match returns 0'
        reply = cGroup.execute('client kill addr 127.0.0.1:1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        when: 'CLIENT KILL ID with no match returns 0'
        reply = cGroup.execute('client kill id 999999')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        when: 'CLIENT KILL USER with no match returns 0'
        reply = cGroup.execute('client kill user unknown')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        when: 'CLIENT KILL LADDR with no match returns 0 (LADDR is best-effort against listen addresses)'
        reply = cGroup.execute('client kill laddr 0.0.0.0:0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        when: 'CLIENT KILL MAXAGE 0 matches no client when none are old enough'
        reply = cGroup.execute('client kill maxage 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        // ----- CLIENT KILL behavior (Task 3 continued) -----

        when: 'CLIENT KILL legacy form ip:port matches by remote address'
        def addrToKill = SocketInspectorTest.mockTcpSocket(null, 46411)
        addrToKill.setInspector(inspector)
        inspector.onConnect(addrToKill)
        extras << addrToKill
        def addrToKillStr = addrToKill.remoteAddress.toString()
        def sizeBefore = inspector.socketMap.size()
        reply = cGroup.execute("client kill ${addrToKillStr}")
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L
        !inspector.socketMap.containsValue(addrToKill)
        inspector.socketMap.size() == sizeBefore - 1

        when: 'CLIENT KILL ADDR matches by remote address'
        def addrSocket = SocketInspectorTest.mockTcpSocket(null, 46412)
        addrSocket.setInspector(inspector)
        inspector.onConnect(addrSocket)
        extras << addrSocket
        def addrSocketStr = addrSocket.remoteAddress.toString()
        reply = cGroup.execute("client kill addr ${addrSocketStr}")
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L
        !inspector.socketMap.containsValue(addrSocket)

        when: 'CLIENT KILL ID matches by monotonic id'
        def idSocket = SocketInspectorTest.mockTcpSocket(null, 46413)
        idSocket.setInspector(inspector)
        inspector.onConnect(idSocket)
        extras << idSocket
        def idToKill = idSocket.userData.clientId
        def sizeBefore2 = inspector.socketMap.size()
        reply = cGroup.execute("client kill id ${idToKill}")
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L
        !inspector.socketMap.containsValue(idSocket)
        inspector.socketMap.size() == sizeBefore2 - 1

        when: 'CLIENT KILL TYPE normal SKIPME yes does not kill the issuing socket'
        def sizeBefore3 = inspector.socketMap.size()
        reply = cGroup.execute('client kill type normal skipme yes')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L
        // the issuing socket is still present
        inspector.socketMap.containsValue(socket)
        inspector.socketMap.size() == sizeBefore3

        when: 'CLIENT KILL TYPE normal SKIPME no kills the issuing socket too'
        def sizeBefore4 = inspector.socketMap.size()
        reply = cGroup.execute('client kill type normal skipme no')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L
        // the issuing socket is gone
        !inspector.socketMap.containsValue(socket)
        inspector.socketMap.size() == sizeBefore4 - 1
        // re-register the issuing socket so later cases can still issue commands on it
        inspector.onConnect(socket)

        when: 'CLIENT KILL USER matches by authenticated username'
        def aliceSocket = SocketInspectorTest.mockTcpSocket(null, 46414)
        aliceSocket.setInspector(inspector)
        inspector.onConnect(aliceSocket)
        extras << aliceSocket
        SocketInspector.setAuthUser(aliceSocket, 'alice')
        def sizeBefore5 = inspector.socketMap.size()
        reply = cGroup.execute('client kill user alice')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L
        !inspector.socketMap.containsValue(aliceSocket)
        inspector.socketMap.size() == sizeBefore5 - 1

        when: 'CLIENT KILL TYPE pubsub with no subscribers returns 0'
        reply = cGroup.execute('client kill type pubsub')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0L

        when: 'CLIENT KILL TYPE normal with reactor on a different net worker (reactor-safe close)'
        def netWorkerEventloop = Eventloop.builder()
                .withThreadName('client-kill-test-net')
                .withIdleInterval(Duration.ofMillis(50))
                .build()
        netWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.sleep(200)
        eventloopsToBreak << netWorkerEventloop
        def otherSocket = SocketInspectorTest.mockTcpSocket(netWorkerEventloop, 46415)
        otherSocket.setInspector(inspector)
        inspector.onConnect(otherSocket)
        extras << otherSocket
        def sizeBefore6 = inspector.socketMap.size()
        reply = cGroup.execute('client kill type normal skipme yes')
        then:
        reply instanceof IntegerReply
        // the only non-issuing normal socket must have been queued for close
        (reply as IntegerReply).integer == 1L

        and: 'after the owning reactor processes the close, the non-issuing socket is removed'
        def deadline = System.currentTimeMillis() + 2000
        while (inspector.socketMap.size() != sizeBefore6 - 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20)
        }
        inspector.socketMap.size() == sizeBefore6 - 1
        !inspector.socketMap.containsValue(otherSocket)

        // ----- CLIENT NO-EVICT / CLIENT NO-TOUCH (Task 5) — safe compatibility no-ops -----

        when: 'CLIENT NO-EVICT on returns OK'
        reply = cGroup.execute('client no-evict on')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT NO-EVICT off returns OK'
        reply = cGroup.execute('client no-evict off')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT NO-EVICT with no value is format error'
        reply = cGroup.execute('client no-evict')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT NO-EVICT with unknown value is syntax error'
        reply = cGroup.execute('client no-evict maybe')
        then:
        reply == ErrorReply.SYNTAX

        when: 'CLIENT NO-TOUCH on returns OK'
        reply = cGroup.execute('client no-touch on')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT NO-TOUCH off returns OK'
        reply = cGroup.execute('client no-touch off')
        then:
        reply == OKReply.INSTANCE

        when: 'CLIENT NO-TOUCH with no value is format error'
        reply = cGroup.execute('client no-touch')
        then:
        reply == ErrorReply.FORMAT

        when: 'CLIENT NO-TOUCH with unknown value is syntax error'
        reply = cGroup.execute('client no-touch maybe')
        then:
        reply == ErrorReply.SYNTAX

        // ----- unsupported stateful CLIENT subcommands (Task 6) -----

        expect:
        cGroup.execute('client caching yes') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client getredir') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client tracking on') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client trackinginfo') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client pause 10') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client unpause') == ErrorReply.NOT_SUPPORT
        cGroup.execute('client unblock 1') == ErrorReply.NOT_SUPPORT

        // ----- unknown CLIENT subcommand (Open Decision N2) returns syntax error -----

        when: 'unknown CLIENT subcommand returns syntax error (Redis-compatible)'
        reply = cGroup.execute('client zzz')
        then:
        reply == ErrorReply.SYNTAX

        cleanup:
        extras.each {
            if (inspector.socketMap.containsValue(it)) {
                inspector.onDisconnect(it)
            }
        }
        eventloopsToBreak.each { it.breakEventloop() }
        fixture.cleanup()
    }

    /**
     * Builds a focused fixture for the {@code CLIENT} command tests.
     *
     * <p>Creates a current-thread {@link Eventloop}, a fresh {@link SocketInspector}
     * with a single net-worker/slot-worker reactor, a mocked issuing socket, and a
     * {@link CGroup} ready to call {@code execute(...)} on. The issuing socket is
     * registered with the inspector so {@code CLIENT ID}, {@code CLIENT INFO}, and
     * {@code CLIENT LIST} see it; the cleanup closure unregisters it.
     */
    private static Map clientFixture() {
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        def inspector = new SocketInspector()
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        inspector.connectedClientCountArray = [0]
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = new long[]{Thread.currentThread().threadId()}
        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = new long[]{Thread.currentThread().threadId()}

        def socket = SocketInspectorTest.mockTcpSocket()
        LocalPersist.instance.setSocketInspector(inspector)
        inspector.onConnect(socket)
        def cGroup = new CGroup(null, null, socket)
        cGroup.from(BaseCommand.mockAGroup())

        [
                inspector: inspector,
                socket: socket,
                cGroup: cGroup,
                cleanup: {
                    if (inspector.socketMap.containsValue(socket)) {
                        inspector.onDisconnect(socket)
                    }
                    LocalPersist.instance.setSocketInspector(null)
                }
        ]
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
