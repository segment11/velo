package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class EGroupTest extends Specification {
    final short slot = 0
    def _EGroup = new EGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sExistsList = _EGroup.parseSlots('exists', data2, slotNumber)
        def sExpireList = _EGroup.parseSlots('expire', data2, slotNumber)
        def sExpireAtList = _EGroup.parseSlots('expireat', data2, slotNumber)
        def sExpireTimeList = _EGroup.parseSlots('expiretime', data2, slotNumber)
        def sList = _EGroup.parseSlots('exxx', data2, slotNumber)
        then:
        sExistsList.size() == 1
        sExpireList.size() == 1
        sExpireAtList.size() == 1
        sExpireTimeList.size() == 1
        sList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        sExistsList = _EGroup.parseSlots('exists', data3, slotNumber)
        then:
        sExistsList.size() == 2

        when:
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)
        def sExtendList = _EGroup.parseSlots('extend', data3, slotNumber)
        then:
        sExtendList.size() == 0

        when:
        def data1 = new byte[1][]
        sExistsList = _EGroup.parseSlots('exists', data1, slotNumber)
        sExpireList = _EGroup.parseSlots('expire', data1, slotNumber)
        then:
        sExistsList.size() == 0
        sExpireList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('exists', data1, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup())

        eGroup.slotWithKeyHashListParsed = _EGroup.parseSlots('exists', data1, eGroup.slotNumber)

        when:
        def reply = eGroup.handle()
        then:
        reply == ErrorReply.FORMAT
        when:
        eGroup.cmd = 'expire'
        reply = eGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        eGroup.cmd = 'expireat'
        reply = eGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        eGroup.cmd = 'expiretime'
        reply = eGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        eGroup.cmd = 'echo'
        reply = eGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        eGroup.data = data2
        eGroup.cmd = 'echo'
        reply = eGroup.handle()
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        def classpath = Utils.projectPath("/dyn/src")
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)
        eGroup.cmd = 'extend'
        reply = eGroup.handle()
        then:
        reply instanceof BulkReply

        when:
        eGroup.cmd = 'zzz'
        reply = eGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test exists'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup(null, null, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = eGroup.execute('exists')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = eGroup.execute('exists >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.execute('exists a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = eGroup.execute('exists a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

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
        eGroup.crossRequestWorker = true
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, 'b', 0, cv)
        reply = eGroup.execute('exists a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 1
        }.result

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test expire'() {
        given:
        def data1 = new byte[1][]

        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup('expire', data1, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = eGroup.expire(true, true)
        then:
        reply == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        eGroup.data = data3
        eGroup.slotWithKeyHashListParsed = _EGroup.parseSlots('expire', data3, eGroup.slotNumber)
        reply = eGroup.expire(true, true)
        then:
        // not exists
        reply == IntegerReply.REPLY_0

        when:
        // not valid integer
        data3[2] = 'a'.bytes
        reply = eGroup.expire(true, true)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data3[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data3[2] = (1000 * 60).toString().bytes
        reply = eGroup.expire(false, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data3[2] = ((System.currentTimeMillis() / 1000).intValue() + 60).toString().bytes
        reply = eGroup.expire(true, false)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data3[2] = 60.toString().bytes
        reply = eGroup.expire(false, false)
        then:
        reply == IntegerReply.REPLY_1

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'nx'.bytes
        eGroup.data = data4
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'xx'.bytes
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        data4[3] = 'xx'.bytes
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 + 1000).toString().bytes
        data4[3] = 'gt'.bytes
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 - 1000).toString().bytes
        data4[3] = 'gt'.bytes
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 - 1000).toString().bytes
        data4[3] = 'lt'.bytes
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1

        when:
        data4[2] = (System.currentTimeMillis() + 1000 * 60 + 1000).toString().bytes
        data4[3] = 'lt'.bytes
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.expire(true, true)
        then:
        reply == IntegerReply.REPLY_1
    }

    def 'test expiretime'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def eGroup = new EGroup(null, null, null)
        def pGroup = new PGroup(null, null, null)
        eGroup.byPassGetSet = inMemoryGetSet
        eGroup.from(BaseCommand.mockAGroup())
        pGroup.from(eGroup)

        when:
        def reply = eGroup.execute('expiretime')
        then:
        reply == ErrorReply.FORMAT

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = eGroup.execute('expiretime a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -1

        when:
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = pGroup.execute('pexpiretime a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == cv.expireAt

        when:
        reply = eGroup.execute('expiretime a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == (cv.expireAt / 1000).intValue()

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = eGroup.execute('expiretime a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2
    }
}
