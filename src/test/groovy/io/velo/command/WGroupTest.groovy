package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.ReplPairTest
import io.velo.reply.AsyncReply
import io.velo.reply.ErrorReply
import io.velo.reply.IntegerReply
import io.velo.reply.NilReply
import spock.lang.Specification

import java.time.Duration

class WGroupTest extends Specification {
    final short slot = 0
    def _WGroup = new WGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _WGroup.parseSlots('wx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def wGroup = new WGroup('wait', data1, null)
        wGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = wGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        wGroup.cmd = 'wx'
        reply = wGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test wait'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()

        def wGroup = new WGroup(null, null, socket)
        wGroup.byPassGetSet = inMemoryGetSet
        wGroup.from(BaseCommand.mockAGroup())

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        Thread.sleep(100)
        localPersist.fixSlotThreadId(slot, eventloop.eventloopThread.threadId())
        oneSlot.slotWorkerEventloop = eventloop

        when:
        def veloUserData = SocketInspector.createUserDataIfNotSet(socket)
        // not set yet
        def reply = wGroup.execute('wait 2 100')
        then:
        reply == IntegerReply.REPLY_0

        when:
        // as master, no slaves
        SocketInspector.updateLastSetSeq(socket, 1L, slot)
        reply = wGroup.execute('wait 2 100')
        then:
        reply == IntegerReply.REPLY_0

        when:
        // mock as slave
        def replPairAsSlave = ReplPairTest.mockAsSlave()
        oneSlot.replPairs.add(replPairAsSlave)
        reply = wGroup.execute('wait 2 100')
        then:
        reply == IntegerReply.REPLY_0

        when:
        // mock as master
        oneSlot.replPairs.clear()
        def replPairAsMaster = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster)
        reply = wGroup.execute('wait 2 100')
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 0
        }.result

        when:
        replPairAsMaster.slaveCatchUpLastSeq = 1L
        reply = wGroup.execute('wait 2 100')
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 1
        }.result

        when:
        def replPairAsMaster2 = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster2)
        replPairAsMaster2.slaveCatchUpLastSeq = 1L
        reply = wGroup.execute('wait 2 100')
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 2
        }.result

        when:
        reply = wGroup.execute('wait 0 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = wGroup.execute('wait 1 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        // over max timeout millis
        reply = wGroup.execute('wait 1 60001')
        then:
        reply instanceof ErrorReply

        when:
        reply = wGroup.execute('wait a 0')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = wGroup.execute('wait 0 a')
        then:
        reply == ErrorReply.NOT_INTEGER

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
