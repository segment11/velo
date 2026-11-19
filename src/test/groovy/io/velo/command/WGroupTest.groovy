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
        def wGroup = new WGroup(null, null, null)
        wGroup.from(BaseCommand.mockAGroup())

        expect:
        wGroup.execute(input) == expected

        where:
        input  | expected
        'wait' | ErrorReply.FORMAT
        'wx'   | ErrorReply.UNKNOWN_COMMAND('wx')
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
        // not set yet: redis counts every online replica for a client without writes (offset 0)
        def replPairAsMaster0 = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster0)
        def reply = wGroup.execute('wait 1 100')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1L

        when:
        // as master, no slaves: redis blocks until the timeout, then answers 0
        SocketInspector.updateLastSetSeq(socket, 1L, slot)
        reply = wGroup.execute('wait 2 100')
        then:
        reply instanceof AsyncReply

        when:
        // mock as slave, redis answers an explicit error instead of 0
        def replPairAsSlave = ReplPairTest.mockAsSlave()
        oneSlot.replPairs.add(replPairAsSlave)
        reply = wGroup.execute('wait 2 100')
        then:
        reply == new ErrorReply('WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.')

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
        // both slaves already caught up: redis answers immediately, without waiting the timeout
        long beginMillis = System.currentTimeMillis()
        reply = wGroup.execute('wait 2 100')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2L
        System.currentTimeMillis() - beginMillis < 90

        when:
        // the third slave catches up at ~50ms: the poller wakes long before the 300ms timeout
        def replPairAsMaster4 = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster4)
        long beginMillis4 = System.currentTimeMillis()
        reply = wGroup.execute('wait 3 300')
        then:
        reply instanceof AsyncReply

        when:
        Thread.sleep(50)
        replPairAsMaster4.slaveCatchUpLastSeq = 1L
        def promise4 = (reply as AsyncReply).settablePromise
        def deadlineMillis4 = System.currentTimeMillis() + 2000
        while (!promise4.isComplete() && System.currentTimeMillis() < deadlineMillis4) {
            Thread.sleep(10)
        }
        then:
        promise4.isComplete()
        promise4.getResult() instanceof IntegerReply
        (promise4.getResult() as IntegerReply).integer == 3L
        System.currentTimeMillis() - beginMillis4 < 290

        when:
        // timeout 0 has no deadline: the wait blocks until the replicas catch up
        def replPairAsMaster5 = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster5)
        reply = wGroup.execute('wait 4 0')
        then:
        reply instanceof AsyncReply
        !(reply as AsyncReply).settablePromise.isComplete()

        when:
        Thread.sleep(50)
        replPairAsMaster5.slaveCatchUpLastSeq = 1L
        def promise5 = (reply as AsyncReply).settablePromise
        def deadlineMillis5 = System.currentTimeMillis() + 2000
        while (!promise5.isComplete() && System.currentTimeMillis() < deadlineMillis5) {
            Thread.sleep(10)
        }
        then:
        promise5.isComplete()
        promise5.getResult() instanceof IntegerReply
        (promise5.getResult() as IntegerReply).integer == 4L

        when:
        // redis accepts numreplicas <= 0 and answers immediately with the current ack count
        reply = wGroup.execute('wait 0 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4L

        when:
        // a third slave that has not caught up is not counted
        def replPairAsMaster3 = ReplPairTest.mockAsMaster()
        oneSlot.replPairs.add(replPairAsMaster3)
        reply = wGroup.execute('wait -1 100')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4L

        when:
        // only a negative timeout errors, with the redis error text
        reply = wGroup.execute('wait 1 -1')
        then:
        reply == new ErrorReply('timeout is negative')

        when:
        // timeout 0 means no deadline in redis, it must not error
        reply = wGroup.execute('wait 1 0')
        then:
        reply instanceof IntegerReply || reply instanceof AsyncReply
        if (reply instanceof AsyncReply) {
            def promise = (reply as AsyncReply).settablePromise
            def deadline = System.currentTimeMillis() + 2000
            while (!promise.isComplete() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10)
            }
            promise.getResult() instanceof IntegerReply && (promise.getResult() as IntegerReply).integer == 4L
        }

        when:
        // redis has no timeout cap
        reply = wGroup.execute('wait 0 60001')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4L

        when:
        reply = wGroup.execute('wait 1 60001')
        then:
        !(reply instanceof ErrorReply)

        when:
        reply = wGroup.execute('wait a 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = wGroup.execute('wait 0 a')
        then:
        reply == new ErrorReply('timeout is not an integer or out of range')

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
