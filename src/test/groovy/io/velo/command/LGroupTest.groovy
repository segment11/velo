package io.velo.command

import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisList
import redis.clients.jedis.Jedis
import spock.lang.Specification

class LGroupTest extends Specification {
    final short slot = 0
    def _LGroup = new LGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sLindexList = _LGroup.parseSlots('lindex', data2, slotNumber)
        def sLinsertList = _LGroup.parseSlots('linsert', data2, slotNumber)
        def sLlenList = _LGroup.parseSlots('llen', data2, slotNumber)
        def sLpopList = _LGroup.parseSlots('lpop', data2, slotNumber)
        def sLposList = _LGroup.parseSlots('lpos', data2, slotNumber)
        def sLpushList = _LGroup.parseSlots('lpush', data2, slotNumber)
        def sLpushxList = _LGroup.parseSlots('lpushx', data2, slotNumber)
        def sLrangeList = _LGroup.parseSlots('lrange', data2, slotNumber)
        def sLremList = _LGroup.parseSlots('lrem', data2, slotNumber)
        def sLsetList = _LGroup.parseSlots('lset', data2, slotNumber)
        def sLtrimList = _LGroup.parseSlots('ltrim', data2, slotNumber)
        def sList = _LGroup.parseSlots('lxxx', data2, slotNumber)
        then:
        sLindexList.size() == 1
        sLinsertList.size() == 1
        sLlenList.size() == 1
        sLpopList.size() == 1
        sLposList.size() == 1
        sLpushList.size() == 1
        sLpushxList.size() == 1
        sLrangeList.size() == 1
        sLremList.size() == 1
        sLsetList.size() == 1
        sLtrimList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]
        sLinsertList = _LGroup.parseSlots('linsert', data1, slotNumber)
        then:
        sLinsertList.size() == 0

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'a'.bytes
        def sLmoveList = _LGroup.parseSlots('lmove', data5, slotNumber)
        then:
        sLmoveList.size() == 2

        when:
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'b'.bytes
        sLmoveList = _LGroup.parseSlots('lmove', data6, slotNumber)
        then:
        sLmoveList.size() == 2

        when:
        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = 'b'.bytes
        sLmoveList = _LGroup.parseSlots('lmove', data7, slotNumber)
        then:
        sLmoveList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def lGroup = new LGroup('lastsave', data1, null)
        lGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = lGroup.handle()
        then:
        reply instanceof IntegerReply

        when:
        lGroup.cmd = 'lindex'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'linsert'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'llen'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lmove'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpop'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpos'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpush'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lpushx'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lrange'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lrem'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'lset'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'ltrim'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'load-rdb'
        reply = lGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'zzz'
        reply = lGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test lindex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lindex a 0')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lindex a 0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        reply = lGroup.execute('lindex a 1')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lindex a -1')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        reply = lGroup.execute('lindex a -2')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lindex a 65536')
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        reply = lGroup.execute('lindex a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lindex >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test linsert'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('linsert a after b c')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('linsert a after b c')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('linsert a before b c')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        rl.removeFirst()
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('linsert a before b c')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = lGroup.execute('linsert a xxx b c')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('linsert >key before b c')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('linsert a before >value c')
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        reply = lGroup.execute('linsert a before b >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test llen'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('llen a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('llen a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = lGroup.execute('llen >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lmove'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = lGroup.execute('lmove a b left left')
        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cv = cvList[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        def cv1 = cvList[1]
        cv1.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl1 = new RedisList()
        rl1.addFirst('b'.bytes)
        cv1.compressedData = rl1.encode()
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.execute('lmove a b left left')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.execute('lmove a b left right')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.execute('lmove a b right left')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        reply = lGroup.execute('lmove a b right right')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        reply = lGroup.execute('lmove a b xxx right')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('lmove a b left xxx')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('lmove >key b left left')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('lmove a >key left left')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.lmove(true)
        then:
        reply == ErrorReply.FORMAT

        when:
        inMemoryGetSet.remove(slot, 'a')
        lGroup.execute('lmove a b left left 0')
        reply = lGroup.lmove(true)
        then:
        reply == NilReply.INSTANCE

        when:
        lGroup.data[5] = 'a'.bytes
        reply = lGroup.lmove(true)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        lGroup.data[5] = '3601'.bytes
        reply = lGroup.lmove(true)
        then:
        reply instanceof ErrorReply
    }

    def 'test lpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()

        def lGroup = new LGroup(null, null, socket)
        def rGroup = new RGroup(null, null, socket)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())
        rGroup.from(lGroup)

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lpop a')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lpop a')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'a'.bytes

        when:
        reply = lGroup.execute('lpop a')
        then:
        reply == NilReply.INSTANCE

        when:
        rl.removeFirst()
        100.times {
            rl.addFirst(('aaaaabbbbbccccc' * 5).bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.execute('rpop a 2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == ('aaaaabbbbbccccc' * 5).bytes

        when:
        // clear all
        resetRedisList(rl, 0)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = rGroup.execute('rpop a 2')
        then:
        reply == MultiBulkReply.NULL

        when:
        reply = lGroup.execute('lpop a')
        then:
        reply == NilReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = rGroup.execute('rpop a 2')
        then:
        reply == MultiBulkReply.NULL

        when:
        SocketInspector.setResp3(socket, true)
        reply = rGroup.execute('rpop a 2')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lpop a')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lpop a -1')
        then:
        reply == ErrorReply.RANGE_OUT_OF_INDEX

        when:
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lpop a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = lGroup.execute('lpop a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lpop >key 0')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lpos'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lpos a a')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lpos a a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = lGroup.execute('lpos a b')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lpos a a rank -1 count 1 maxlen 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        rl.removeFirst()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lpos a 5 rank -1 count 1 maxlen 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 15

        when:
        reply = lGroup.execute('lpos a 5 rank 2 count 1 maxlen 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 15

        when:
        // maxlen
        reply = lGroup.execute('lpos a 5 rank 2 count 1 maxlen 10')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = lGroup.execute('lpos a 5 rank 2 count 2 maxlen 10')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = lGroup.execute('lpos a 5 rank 1 count 2 maxlen 0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[0]).integer == 5
        (reply as MultiBulkReply).replies[1] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[1]).integer == 15

        when:
        reply = lGroup.execute('lpos a 5 rank -1 count 2 maxlen 0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[0]).integer == 15
        (reply as MultiBulkReply).replies[1] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[1]).integer == 5

        when:
        reply = lGroup.execute('lpos a 5 rank -1 count 0 maxlen 0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        reply = lGroup.execute('lpos a 5 rank')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('lpos a 5 count')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('lpos a 5 maxlen')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = lGroup.execute('lpos a a rank a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lpos a a count a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lpos a a maxlen a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lpos a a maxlen -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = lGroup.execute('lpos a a count -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = lGroup.execute('lpos >key a count -1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('lpos a >value count -1')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lpush'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        def rGroup = new RGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())
        rGroup.from(lGroup)

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lpushx a a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = lGroup.execute('lpush a a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = rGroup.execute('rpush a a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        BlockingList.addBlockingListPromiseByKey('a', finalPromise, null, true)
        reply = lGroup.execute('lpush a a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3
        finalPromise.isComplete()

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        SettablePromise<Reply> finalPromise2 = new SettablePromise<>()
        BlockingList.addBlockingListPromiseByKey('a', finalPromise2, null, true)
        reply = rGroup.execute('rpush a a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3
        finalPromise2.isComplete()

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        SettablePromise<Reply> finalPromise3 = new SettablePromise<>()
        BlockingList.addBlockingListPromiseByKey('a', finalPromise3, null, true)
        inMemoryGetSet.remove(slot, 'a')
        reply = lGroup.execute('lpush a a')
        then:
        reply == IntegerReply.REPLY_1
        finalPromise3.isComplete()

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        SettablePromise<Reply> finalPromise4 = new SettablePromise<>()
        BlockingList.addBlockingListPromiseByKey('a', finalPromise4, null, false)
        reply = lGroup.execute('lpush a a b c')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3
        finalPromise4.isComplete()
        finalPromise4.getResult() instanceof MultiBulkReply
        ((finalPromise4.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'c'.bytes

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        SettablePromise<Reply> finalPromise5 = new SettablePromise<>()
        BlockingList.addBlockingListPromiseByKey('a', finalPromise5, null, true)
        reply = rGroup.execute('rpush a a b c')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3 - 1 + 3
        finalPromise5.isComplete()
        finalPromise5.getResult() instanceof MultiBulkReply
        ((finalPromise5.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'c'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        RedisList.LIST_MAX_SIZE.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lpush a a')
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        reply = lGroup.execute('lpush >key a b c')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('lpush a >value b c')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lrange'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lrange a 0 2')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lrange a 0 2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3

        when:
        reply = lGroup.execute('lrange a 2 1')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = lGroup.execute('lrange a a 1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lrange >key a 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lrem'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lrem a 1 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lrem a 1 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = lGroup.execute('lrem a -1 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = lGroup.execute('lrem a -1 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = lGroup.execute('lrem a 0 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = lGroup.execute('lrem a 3 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = lGroup.execute('lrem a a 2')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lrem >key 3 2')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('lrem a 3 >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lset'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('lset a 1 a')
        then:
        reply == ErrorReply.NO_SUCH_KEY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('lset a 1 a')
        then:
        reply == OKReply.INSTANCE

        when:
        // set again, not change
        reply = lGroup.execute('lset a 1 a')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = lGroup.execute('lset a -1 a')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = lGroup.execute('lset a -11 a')
        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        reply = lGroup.execute('lset a 10 a')
        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        reply = lGroup.execute('lset a 65536 a')
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        reply = lGroup.execute('lset a a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('lset >key 1 a')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = lGroup.execute('lset a 1 >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    private static void resetRedisList(RedisList rl, int n) {
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        n.times {
            rl.addLast(it.toString().bytes)
        }
    }

    def 'test ltrim'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.execute('ltrim a 0 9')
        then:
        reply == OKReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        resetRedisList(rl, 10)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('ltrim a 0 9')
        then:
        reply == OKReply.INSTANCE

        when:
        resetRedisList(rl, 10)
        reply = lGroup.execute('ltrim a 1 2')
        then:
        reply == OKReply.INSTANCE

        when:
        resetRedisList(rl, 10)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('ltrim a 2 1')
        then:
        reply == OKReply.INSTANCE

        when:
        resetRedisList(rl, 10)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('ltrim a 100 200')
        then:
        reply == OKReply.INSTANCE

        when:
        resetRedisList(rl, 10)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.execute('ltrim a -100 -90')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = lGroup.execute('ltrim a a 9')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = lGroup.execute('ltrim >key 0 9')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test load rdb'() {
        // check redis-server bin file and generate rdb file
        final String redisServerBin = '/usr/local/bin/redis-server'
        final String rdbFilePath = '/tmp/dump.rdb'

        Process process

        int mockDataNum = 100
        int tmpPort = 16379
        if (!new File(rdbFilePath).exists()) {
            if (!new File(redisServerBin).exists()) {
                println 'skip test load rdb as redis-server bin file not exists: ' + redisServerBin
                return
            }

            process = new ProcessBuilder(redisServerBin, '--dir', '/tmp', '--port', tmpPort.toString()).start()
            Thread.sleep(5000)
            Thread.start {
                def jedis = new Jedis('localhost', tmpPort)
                mockDataNum.times { i ->
                    jedis.set('key:' + i, UUID.randomUUID().toString())
                }
                println "generate ${mockDataNum} string keys / values"

                mockDataNum.times { i ->
                    10.times { j ->
                        jedis.lpush('list:' + i, 'element:' + j)
                    }
                }
                println "generate ${mockDataNum} list keys / values"

                mockDataNum.times { i ->
                    10.times { j ->
                        jedis.hset('hash:' + i, 'field:' + j, UUID.randomUUID().toString())
                    }
                }
                println "generate ${mockDataNum} hash keys / values"

                mockDataNum.times { i ->
                    10.times { j ->
                        jedis.sadd('set:' + i, 'member:' + j)
                    }
                }
                println "generate ${mockDataNum} set keys / values"

                mockDataNum.times { i ->
                    10.times { j ->
                        jedis.zadd('zset:' + i, j, 'element:' + j)
                    }
                }
                println "generate ${mockDataNum} zset keys / values"

//                jedis.save()
//                println 'save rdb done'
                jedis.close()
                process.destroy()
                println 'destroy redis-server process after mock data'
            }
            process.waitFor()
            process = null
            println 'generate rdb file done'
        } else {
            println 'use exist rdb file to test load rdb: ' + rdbFilePath
        }

        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup(null, null, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = lGroup.execute('load-rdb ' + rdbFilePath)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == mockDataNum * 5

        cleanup:
        if (process != null) {
            process.destroy()
        }
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
