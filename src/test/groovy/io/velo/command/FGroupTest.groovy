package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class FGroupTest extends Specification {
    def _FGroup = new FGroup(null, null, null)

    def 'test parse slot'() {
        given:
        int slotNumber = 128
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        expect:
        _FGroup.parseSlots('failover', data2, slotNumber).size() == 0
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        def fGroup = new FGroup(cmd, data2, null)
        fGroup.from(BaseCommand.mockAGroup())

        expect:
        fGroup.handle() == expected

        where:
        cmd        | expected
        'failover' | OKReply.INSTANCE
        'flushall' | OKReply.INSTANCE
        'flushdb'  | OKReply.INSTANCE
        'zzz'      | NilReply.INSTANCE
    }

    final short slot = 0

    def 'test failover'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('failover', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        when:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def reply = fGroup.failover()
        then:
        // no slave
        reply instanceof ErrorReply

        when:
        def firstOneSlot = localPersist.currentThreadFirstOneSlot()
        def rp1 = firstOneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        def rp2 = firstOneSlot.createIfNotExistReplPairAsMaster(12L, 'localhost', 6381)
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 0L)
        reply = fGroup.failover()
        then:
        reply instanceof ErrorReply

        when:
        firstOneSlot.binlog.moveToNextSegment()
        rp2.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1L)
        reply = fGroup.failover()
        then:
        reply instanceof ErrorReply

        when:
        firstOneSlot.binlog.moveToNextSegment()
        rp2.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 0L)
        reply = fGroup.failover()
        then:
        reply instanceof ErrorReply

        when:
        rp2.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 0L)
        boolean doThisCase = Consts.checkConnectAvailable()
        Eventloop eventloop
        if (doThisCase) {
            eventloop = Eventloop.builder()
                    .withIdleInterval(Duration.ofMillis(100))
                    .build()
            eventloop.keepAlive(true)
            Thread.start {
                eventloop.run()
            }
            def eventloopCurrent = Eventloop.builder()
                    .withCurrentThread()
                    .withIdleInterval(Duration.ofMillis(100))
                    .build()
            reply = fGroup.failover()
            eventloopCurrent.run()
            Thread.sleep(1000)
        } else {
            SettablePromise<Reply> finalPromise = new SettablePromise<>()
            finalPromise.set(OKReply.INSTANCE)
            reply = new AsyncReply(finalPromise)
        }
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        if (eventloop) {
            eventloop.breakEventloop()
        }
        localPersist.cleanUp()
    }

    def 'test flushdb'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('flushdb', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

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
        def reply = fGroup.execute('flushdb')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
