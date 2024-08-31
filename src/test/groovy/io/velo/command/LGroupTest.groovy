package io.velo.command

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisList
import spock.lang.Specification

class LGroupTest extends Specification {
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
        // wrong size
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'a'.bytes
        sLmoveList = _LGroup.parseSlots('lmove', data6, slotNumber)
        then:
        sLmoveList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def lGroup = new LGroup('lindex', data1, null)
        lGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = lGroup.handle()
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

//        when:
//        lGroup.cmd = 'load-rdb'
//        reply = lGroup.handle()
//
//        then:
//        reply == ErrorReply.FORMAT

        when:
        lGroup.cmd = 'zzz'
        reply = lGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test lindex'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lindex', data3, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lindex', data3, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lindex()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lindex()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        data3[2] = '1'.bytes
        reply = lGroup.lindex()
        then:
        reply == NilReply.INSTANCE

        when:
        data3[2] = '-1'.bytes
        reply = lGroup.lindex()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        data3[2] = '-2'.bytes
        reply = lGroup.lindex()
        then:
        reply == NilReply.INSTANCE

        when:
        data3[2] = RedisList.LIST_MAX_SIZE.toString().bytes
        reply = lGroup.lindex()
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        data3[2] = 'a'.bytes
        reply = lGroup.lindex()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lindex()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test linsert'() {
        given:
        final short slot = 0

        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'after'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'c'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('linsert', data5, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('linsert', data5, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.linsert()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.linsert()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rl = new RedisList()
        rl.addFirst('b'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data5[2] = 'before'.bytes
        reply = lGroup.linsert()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rl.removeFirst()
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.linsert()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data5[2] = 'xxx'.bytes
        reply = lGroup.linsert()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data5[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.linsert()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data5[1] = 'a'.bytes
        data5[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.linsert()
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        data5[3] = 'b'.bytes
        data5[4] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.linsert()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test llen'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('llen', data2, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('llen', data2, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.llen()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.llen()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.llen()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lmove'() {
        given:
        final short slot = 0

        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'b'.bytes
        data5[3] = 'left'.bytes
        data5[4] = 'left'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lmove', data5, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lmove', data5, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = lGroup.lmove()
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
        reply = lGroup.lmove()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        data5[3] = 'left'.bytes
        data5[4] = 'right'.bytes
        reply = lGroup.lmove()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        data5[3] = 'right'.bytes
        data5[4] = 'left'.bytes
        reply = lGroup.lmove()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        inMemoryGetSet.put(slot, 'b', 0, cv1)
        data5[3] = 'right'.bytes
        data5[4] = 'right'.bytes
        reply = lGroup.lmove()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        data5[3] = 'xxx'.bytes
        reply = lGroup.lmove()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data5[3] = 'left'.bytes
        data5[4] = 'xxx'.bytes
        reply = lGroup.lmove()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data5[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lmove()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data5[1] = 'a'.bytes
        data5[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lmove()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lpop'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpop', data2, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lpop', data2, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpop(true)
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpop(true)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        reply = lGroup.lpop(true)
        then:
        reply == NilReply.INSTANCE

        when:
        rl.removeFirst()
        100.times {
            rl.addFirst(('aaaaabbbbbccccc' * 5).bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '2'.bytes
        lGroup.data = data3
        reply = lGroup.lpop(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == ('aaaaabbbbbccccc' * 5).bytes

        when:
        data3[2] = '0'.bytes
        reply = lGroup.lpop(true)
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = lGroup.lpop(true)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpop(true)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lpos'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpos', data3, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lpos', data3, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpos()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        rl.addFirst('a'.bytes)
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpos()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data3[2] = 'b'.bytes
        reply = lGroup.lpos()
        then:
        reply == NilReply.INSTANCE

        when:
        def data9 = new byte[9][]
        data9[1] = 'a'.bytes
        data9[2] = 'a'.bytes
        data9[3] = 'rank'.bytes
        data9[4] = '-1'.bytes
        data9[5] = 'count'.bytes
        data9[6] = '1'.bytes
        data9[7] = 'maxlen'.bytes
        data9[8] = '0'.bytes
        lGroup.data = data9
        reply = lGroup.lpos()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

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
        // member
        data9[2] = '5'.bytes
        reply = lGroup.lpos()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 15

        when:
        // rank
        data9[4] = '2'.bytes
        reply = lGroup.lpos()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 15

        when:
        // maxlen
        data9[8] = '10'.bytes
        reply = lGroup.lpos()
        then:
        reply == NilReply.INSTANCE

        when:
        // count
        data9[6] = '2'.bytes
        reply = lGroup.lpos()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        // rank
        data9[4] = '1'.bytes
        // maxlen
        data9[8] = '0'.bytes
        reply = lGroup.lpos()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 5
        ((MultiBulkReply) reply).replies[1] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[1]).integer == 15

        when:
        // rank
        data9[4] = '-1'.bytes
        reply = lGroup.lpos()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 15
        ((MultiBulkReply) reply).replies[1] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[1]).integer == 5

        when:
        // count
        data9[6] = '0'.bytes
        reply = lGroup.lpos()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'rank'.bytes
        lGroup.data = data4
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'count'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'maxlen'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'rank'.bytes
        data5[4] = 'a'.bytes
        lGroup.data = data5
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[3] = 'count'.bytes
        data5[4] = 'a'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[3] = 'maxlen'.bytes
        data5[4] = 'a'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[4] = '-1'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[3] = 'count'.bytes
        data5[4] = '-1'.bytes
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data5[1] = 'a'.bytes
        data5[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lpos()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lpush'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lpush', data3, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lpush', data3, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lpush(true, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = lGroup.lpush(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = lGroup.lpush(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        RedisList.LIST_MAX_SIZE.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.lpush(true, false)
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lpush(true, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lpush(true, false)
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lrange'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '0'.bytes
        data4[3] = '2'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lrange', data4, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lrange', data4, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lrange()
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
        reply = lGroup.lrange()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 3

        when:
        data4[2] = '10'.bytes
        reply = lGroup.lrange()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[2] = '1'.bytes
        data4[3] = '0'.bytes
        reply = lGroup.lrange()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[2] = '8'.bytes
        data4[3] = '10'.bytes
        reply = lGroup.lrange()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data4[2] = '-2'.bytes
        data4[3] = '-1'.bytes
        reply = lGroup.lrange()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data4[2] = '-12'.bytes
        data4[3] = '1'.bytes
        reply = lGroup.lrange()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        data4[2] = '-12'.bytes
        data4[3] = '-13'.bytes
        reply = lGroup.lrange()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[2] = 'a'.bytes
        reply = lGroup.lrange()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lrange()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test lrem'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lrem', data4, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lrem', data4, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lrem()
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
        reply = lGroup.lrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data4[2] = '-1'.bytes
        reply = lGroup.lrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = lGroup.lrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data4[2] = '0'.bytes
        data4[3] = '1'.bytes
        reply = lGroup.lrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '3'.bytes
        data4[3] = '2'.bytes
        reply = lGroup.lrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = 'a'.bytes
        reply = lGroup.lrem()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lrem()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lrem()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test lset'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('lset', data4, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('lset', data4, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.lset()
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
        reply = lGroup.lset()
        then:
        reply == OKReply.INSTANCE

        when:
        // set again, not change
        reply = lGroup.lset()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '-1'.bytes
        reply = lGroup.lset()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '-11'.bytes
        reply = lGroup.lset()
        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        data4[2] = '10'.bytes
        reply = lGroup.lset()
        then:
        reply == ErrorReply.INDEX_OUT_OF_RANGE

        when:
        data4[2] = RedisList.LIST_MAX_SIZE.toString().bytes
        reply = lGroup.lset()
        then:
        reply == ErrorReply.LIST_SIZE_TO_LONG

        when:
        data4[2] = 'a'.bytes
        reply = lGroup.lset()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.lset()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = lGroup.lset()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test ltrim'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '0'.bytes
        data4[3] = '9'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def lGroup = new LGroup('ltrim', data4, null)
        lGroup.byPassGetSet = inMemoryGetSet
        lGroup.from(BaseCommand.mockAGroup())

        when:
        lGroup.slotWithKeyHashListParsed = _LGroup.parseSlots('ltrim', data4, lGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '-10'.bytes
        data4[3] = '-1'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '-11'.bytes
        data4[3] = '-1'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '2'.bytes
        data4[3] = '3'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '0'.bytes
        data4[3] = '-11'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = '10'.bytes
        data4[3] = '10'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        while (rl.size() != 0) {
            rl.removeFirst()
        }
        10.times {
            rl.addLast(it.toString().bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '1'.bytes
        data4[3] = '0'.bytes
        reply = lGroup.ltrim()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = 'a'.bytes
        reply = lGroup.ltrim()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = lGroup.ltrim()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
