package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.MultiWorkerServer
import io.velo.SnowFlake
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class VGroupTest extends Specification {
    def _VGroup = new VGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        _VGroup.snowFlake = new SnowFlake(1, 1)

        when:

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _VGroup.parseSlots('vx', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        data2[1] = 'count'.bytes
        def sVVCountList = _VGroup.parseSlots('vv', data2, slotNumber)
        then:
        sVVCountList.size() == 0

        when:
        data2[1] = 'add'.bytes
        def sVVAddList = _VGroup.parseSlots('vv', data2, slotNumber)
        then:
        sVVCountList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'add'.bytes
        data3[2] = 'bad'.bytes
        sVVAddList = _VGroup.parseSlots('vv', data3, slotNumber)
        then:
        sVVAddList.size() == 1

        when:
        data3[1] = 'count'.bytes
        sVVCountList = _VGroup.parseSlots('vv', data3, slotNumber)
        then:
        sVVCountList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def vGroup = new VGroup('vx', data1, null)
        vGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = vGroup.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        vGroup.cmd = 'vv'
        reply = vGroup.handle()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test vv'() {
        given:
        def data1 = new byte[1][]

        def vGroup = new VGroup('vv', data1, null)
        vGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = vGroup.vv()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data4 = new byte[4][]
        data4[1] = 'add'.bytes
        data4[2] = 'bad'.bytes
        vGroup.data = data4
        reply = vGroup.vv()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[1] = 'count'.bytes
        reply = vGroup.vv()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[1] = 'query'.bytes
        reply = vGroup.vv()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[1] = 'zzz'.bytes
        reply = vGroup.vv()
        then:
        reply == ErrorReply.SYNTAX
    }

    final short slot = 0

    def 'test vv_add and vv_count'() {
        given:
        def data3 = new byte[3][]

        def vGroup = new VGroup('vv', data3, null)
        vGroup.from(BaseCommand.mockAGroup())
        vGroup.snowFlake = new SnowFlake(1, 1)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        data3[1] = 'add'.bytes
        data3[2] = '123'.bytes
        vGroup.slotWithKeyHashListParsed = vGroup.parseSlots('vv', data3, vGroup.slotNumber)
        def reply = vGroup.vv_add()
        then:
        // skip 123 as not all alphabet
        reply == IntegerReply.REPLY_0

        when:
        data3[2] = 'bad,cake,1ab'.bytes
        reply = vGroup.vv_add()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof IntegerReply
        // skip 1ab as not all alphabet
        ((IntegerReply) reply).integer == 2

        when:
        data3[1] = 'count'.bytes
        data3[2] = 'bad'.bytes
        reply = vGroup.vv_count()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        when:
        data3[2] = 'bad,cake'.bytes
        reply = vGroup.vv_count()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && ((MultiBulkReply) result).replies.length == 2
        }.result

        when:
        data3[2] = ''.bytes
        reply = vGroup.vv_count()
        then:
        reply == IntegerReply.REPLY_0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test vv_query'() {
        given:
        def data3 = new byte[3][]

        def vGroup = new VGroup('vv', data3, null)
        vGroup.from(BaseCommand.mockAGroup())
        vGroup.snowFlake = new SnowFlake(1, 1)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds = [Thread.currentThread().threadId()]

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        data3[1] = 'query'.bytes
        data3[2] = ''.bytes
        def reply = vGroup.vv_query()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data3[2] = 'a&b&c'.bytes
        reply = vGroup.vv_query()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data3[2] = 'abc&cde&efg'.bytes
        reply = vGroup.vv_query()
        then:
        reply instanceof ErrorReply

        when:
        data3[2] = 'bad'.bytes
        vGroup.slotWithKeyHashListParsed = vGroup.parseSlots('vv', data3, vGroup.slotNumber)
        reply = vGroup.vv_query()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && ((MultiBulkReply) result).replies.length == 1
        }.result

        when:
        data3[2] = 'bad&cake'.bytes
        reply = vGroup.vv_query()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply

        when:
        data3[2] = 'bad|cake'.bytes
        reply = vGroup.vv_query()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply

        when:
        data3[1] = 'add'.bytes
        data3[2] = 'bad,cake'.bytes
        vGroup.slotWithKeyHashListParsed = vGroup.parseSlots('vv', data3, vGroup.slotNumber)
        reply = vGroup.vv_add()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof IntegerReply

        when:
        data3[1] = 'query'.bytes
        data3[2] = 'bad'.bytes
        reply = vGroup.vv_query()
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        reply instanceof AsyncReply

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
