package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class FGroupTest extends Specification {
    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sFlushDbList = FGroup.parseSlots('failover', data2, slotNumber)
        then:
        sFlushDbList.size() == 0
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def fGroup = new FGroup('failover', data2, null)
        fGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = fGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'flushall'
        reply = fGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'flushdb'
        reply = fGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        fGroup.cmd = 'zzz'
        reply = fGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    final short slot = 0

    def 'test failover'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('failover', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        when:
        ConfForGlobal.zookeeperConnectString = null
        def reply = fGroup.failover()
        then:
        reply instanceof ErrorReply

        when:
        ConfForGlobal.zookeeperConnectString = 'localhost:2181'
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        reply = fGroup.failover()
        then:
        // no slave
        reply instanceof ErrorReply

        when:
        def firstOneSlot = localPersist.currentThreadFirstOneSlot()
        def rp1 = firstOneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        var rp2 = firstOneSlot.createIfNotExistReplPairAsMaster(12L, 'localhost', 6381)
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
            SettablePromise<Reply> finalPromise = new SettablePromise<>();
            finalPromise.set(OKReply.INSTANCE)
            reply = new AsyncReply(finalPromise)
        }
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
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
        def reply = fGroup.flushdb()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        eventloop.breakEventloop()
    }
}
