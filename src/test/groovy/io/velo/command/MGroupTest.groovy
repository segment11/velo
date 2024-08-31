package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class MGroupTest extends Specification {
    def _MGroup = new MGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data5 = new byte[5][]
        int slotNumber = 128

        and:
        data5[1] = 'a'.bytes
        data5[2] = '0'.bytes
        data5[3] = '0'.bytes
        data5[4] = '0'.bytes

        when:
        def sMgetList = _MGroup.parseSlots('mget', data5, slotNumber)
        def sMsetList = _MGroup.parseSlots('mset', data5, slotNumber)
        def s = _MGroup.parseSlots('mxxx', data5, slotNumber)
        then:
        sMgetList.size() == 4
        sMsetList.size() == 2
        s.size() == 0


        when:
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        data5[3] = 'view-persist-key-count'.bytes
        def sManageList = _MGroup.parseSlots('manage', data5, slotNumber)
        then:
        sManageList.size() == 1

        when:
        def data1 = new byte[1][]
        def sList = _MGroup.parseSlots('mget', data1, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _MGroup.parseSlots('mset', data1, slotNumber)
        then:
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        sMsetList = _MGroup.parseSlots('mset', data4, slotNumber)
        then:
        sMsetList.size() == 0

        when:
        sList = _MGroup.parseSlots('manage', data1, slotNumber)
        then:
        sList.size() == 0

        when:
        sList = _MGroup.parseSlots('mset', data5, slotNumber)
        then:
        sList.size() == 2

        when:
        data5[1] = 'view-persist-key-count'.bytes

        sList = _MGroup.parseSlots('manage', data5, slotNumber)
        then:
        sList.size() == 0

        when:
        data5[1] = 'view-slot-bucket-keys'.bytes
        sList = _MGroup.parseSlots('manage', data5, slotNumber)
        then:
        sList.size() == 0

        when:
        data5[1] = 'output-dict-bytes'.bytes
        sList = _MGroup.parseSlots('manage', data5, slotNumber)
        then:
        sList.size() == 0

        when:
        data5[1] = 'xxx'.bytes
        sList = _MGroup.parseSlots('manage', data5, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def mGroup = new MGroup('mget', data1, null)
        mGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = mGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        mGroup.cmd = 'mset'
        reply = mGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.getInstance().init(GroovyClassLoader.getClass().classLoader, classpath, null)
        mGroup.cmd = 'manage'
        reply = mGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        mGroup.cmd = 'zzz'
        reply = mGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test mget'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mget', data3, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when:
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('mget', data3, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = mGroup.mget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cv.compressedData
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

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
        mGroup.crossRequestWorker = true
        reply = mGroup.mget()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test mset'() {
        given:
        final short slot = 0

        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '1'.bytes
        data5[3] = 'b'.bytes
        data5[4] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def mGroup = new MGroup('mset', data5, null)
        mGroup.byPassGetSet = inMemoryGetSet
        mGroup.from(BaseCommand.mockAGroup())

        when:
        mGroup.slotWithKeyHashListParsed = _MGroup.parseSlots('mset', data5, mGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = mGroup.mset()
        then:
        reply == OKReply.INSTANCE

        when:
        def valA = mGroup.get('a'.bytes, mGroup.slotWithKeyHashListParsed[0])
        def valB = mGroup.get('b'.bytes, mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '1'.bytes
        valB == '2'.bytes

        when:
        data5[2] = '11'.bytes
        data5[4] = '22'.bytes
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
        mGroup.crossRequestWorker = true
        reply = mGroup.mset()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        valA = mGroup.get('a'.bytes, mGroup.slotWithKeyHashListParsed[0])
        valB = mGroup.get('b'.bytes, mGroup.slotWithKeyHashListParsed[1])
        then:
        valA == '11'.bytes
        valB == '22'.bytes

        when:
        def data4 = new byte[4][]
        mGroup.data = data4
        reply = mGroup.mset()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        eventloop.breakEventloop()
    }
}
