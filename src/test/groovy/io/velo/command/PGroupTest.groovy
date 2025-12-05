package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.acl.AclUsers
import io.velo.acl.RPubSub
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration

class PGroupTest extends Specification {
    def _PGroup = new PGroup(null, null, null)

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        def data4 = new byte[4][]

        and:
        data2[1] = 'a'.bytes

        data4[1] = 'a'.bytes

        when:
        def sPexpireList = _PGroup.parseSlots('pexpire', data4, slotNumber)
        def sPexpireatList = _PGroup.parseSlots('pexpireat', data4, slotNumber)
        def sPexpiretimeList = _PGroup.parseSlots('pexpiretime', data2, slotNumber)
        def sPfaddList = _PGroup.parseSlots('pfadd', data2, slotNumber)
        def sPfcountList = _PGroup.parseSlots('pfcount', data2, slotNumber)
        def sPfmergeList = _PGroup.parseSlots('pfmerge', data2, slotNumber)
        def sPttlList = _PGroup.parseSlots('pttl', data2, slotNumber)
        def sPersistList = _PGroup.parseSlots('persist', data2, slotNumber)
        def sPsetexList = _PGroup.parseSlots('psetex', data4, slotNumber)
        def sList = _PGroup.parseSlots('pxxx', data2, slotNumber)
        then:
        sPexpireList.size() == 1
        sPexpireatList.size() == 1
        sPexpiretimeList.size() == 1
        sPfaddList.size() == 0
        sPfcountList.size() == 1
        sPfmergeList.size() == 0
        sPttlList.size() == 1
        sPersistList.size() == 1
        sPsetexList.size() == 1
        sList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes
        sPexpireatList = _PGroup.parseSlots('pexpireat', data3, slotNumber)
        then:
        sPexpireatList.size() == 1

        when:
        // wrong size
        sPexpireList = _PGroup.parseSlots('pexpire', data2, slotNumber)
        then:
        sPexpireList.size() == 0

        when:
        sPexpireatList = _PGroup.parseSlots('pexpireat', data2, slotNumber)
        then:
        sPexpireatList.size() == 0

        when:
        sPexpiretimeList = _PGroup.parseSlots('pexpiretime', data4, slotNumber)
        then:
        sPexpiretimeList.size() == 0

        when:
        sPfaddList = _PGroup.parseSlots('pfadd', data3, slotNumber)
        then:
        sPfaddList.size() == 1

        when:
        def data1 = new byte[1][]
        sPfcountList = _PGroup.parseSlots('pfcount', data1, slotNumber)
        then:
        sPfcountList.size() == 0

        when:
        sPfmergeList = _PGroup.parseSlots('pfmerge', data3, slotNumber)
        then:
        sPfmergeList.size() == 2

        when:
        sPttlList = _PGroup.parseSlots('pttl', data4, slotNumber)
        then:
        sPttlList.size() == 0

        when:
        sPersistList = _PGroup.parseSlots('persist', data4, slotNumber)
        then:
        sPersistList.size() == 0

        when:
        sPsetexList = _PGroup.parseSlots('psetex', data2, slotNumber)
        then:
        sPsetexList.size() == 0
    }

    def 'test handle'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '60000'.bytes

        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()

        def pGroup = new PGroup('pexpire', data3, socket)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('pexpire', data3, pGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.handle()
        then:
        reply == IntegerReply.REPLY_0

        when:
        pGroup.cmd = 'pexpireat'
        reply = pGroup.handle()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        pGroup.cmd = 'pexpiretime'
        pGroup.data = data2
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('pexpiretime', data2, pGroup.slotNumber)
        reply = pGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2

        when:
        def data1 = new byte[1][]
        pGroup.cmd = 'persist'
        pGroup.data = data1
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        pGroup.cmd = 'pfadd'
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        pGroup.cmd = 'pfcount'
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        pGroup.cmd = 'pfmerge'
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        pGroup.data = data2
        pGroup.cmd = 'pttl'
        reply = pGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2

        when:
        pGroup.cmd = 'psetex'
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '60000'.bytes
        data4[3] = 'value'.bytes
        pGroup.cmd = 'psetex'
        pGroup.data = data4
        pGroup.slotWithKeyHashListParsed = _PGroup.parseSlots('psetex', data4, pGroup.slotNumber)
        reply = pGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        pGroup.cmd = 'pxxx'
        reply = pGroup.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        pGroup.cmd = 'publish'
        pGroup.data = data1
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        pGroup.cmd = 'pubsub'
        reply = pGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def socketInspector = new SocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        data2[0] = 'psubscribe'.bytes
        data2[1] = 'test_channel'.bytes
        pGroup.cmd = 'psubscribe'
        pGroup.data = data2
        reply = pGroup.handle()
        then:
        reply instanceof MultiBulkReply

        when:
        data2[0] = 'punsubscribe'.bytes
        pGroup.cmd = 'punsubscribe'
        reply = pGroup.handle()
        then:
        reply instanceof MultiBulkReply
    }

    def 'test persist'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('persist', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        cv.expireAt = System.currentTimeMillis() + 1000
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = pGroup.execute('persist a')
        then:
        reply == IntegerReply.REPLY_1

        when:
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt == CompressedValue.NO_EXPIRE

        when:
        reply = pGroup.execute('persist')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test pfadd'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfcount', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('pfadd a abc')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = pGroup.execute('pfadd a abc')
        then:
        reply == IntegerReply.REPLY_0

        when:
        boolean exception = false
        String errorMessage = null
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        try {
            reply = pGroup.execute('pfadd a abc')
        } catch (RuntimeException e) {
            println e.message
            errorMessage = e.message
            exception = true
        }
        then:
        exception
        errorMessage == ErrorReply.WRONG_TYPE.message
    }

    def 'test pfcount and pfmerge'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pfcount', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = pGroup.execute('pfcount a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        pGroup.execute('pfadd a abc')
        pGroup.execute('pfadd b xyz')
        reply = pGroup.execute('pfcount a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = pGroup.execute('pfmerge dst a b')
        then:
        reply == OKReply.INSTANCE

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
        pGroup.crossRequestWorker = true
        reply = pGroup.execute('pfcount a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 2
        }.result

        when:
        reply = pGroup.execute('pfmerge dst a b')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        pGroup.crossRequestWorker = false
        reply = pGroup.execute('pfcount dst')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test publish'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'test_channel'.bytes
        data3[2] = 'message'.bytes

        and:
        LocalPersist.instance.socketInspector = new SocketInspector()

        and:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()

        when:
        def reply = _PGroup.publish(data3, null)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        aclUsers.upInsert('default') {u ->
            u.addRPubSub(true, RPubSub.fromLiteral('&special_channel'))
        }
        reply = _PGroup.publish(data3, socket)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        aclUsers.upInsert('default') {u ->
            u.on = false
        }
        reply = _PGroup.publish(data3, socket)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        def data1 = new byte[1][]
        reply = _PGroup.publish(data1, null)
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test pubsub'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def pGroup = new PGroup('pubsub', null, null)
        pGroup.byPassGetSet = inMemoryGetSet
        pGroup.from(BaseCommand.mockAGroup())

        when:
        def socketInspector = new SocketInspector()
        LocalPersist.instance.socketInspector = socketInspector
        def reply = pGroup.execute('pubsub channels')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def channel = 'test_channel'
        reply = pGroup.execute('pubsub channels ' + channel)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        socketInspector.subscribe(channel, false, socket)
        reply = pGroup.execute('pubsub channels ' + channel)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).raw == channel.bytes

        when:
        reply = pGroup.execute('pubsub numpat')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = pGroup.execute('pubsub numsub ' + channel)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).raw == channel.bytes
        (reply as MultiBulkReply).replies[1] instanceof IntegerReply
        ((reply as MultiBulkReply).replies[1] as IntegerReply).integer == 1

        when:
        reply = pGroup.execute('pubsub numsub')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = pGroup.execute('pubsub xxx')
        then:
        reply == NilReply.INSTANCE
    }
}
