package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.time.Duration

class ZGroupTest extends Specification {
    def _ZGroup = new ZGroup(null, null, null)

    def singleKeyCmdList1 = '''
zadd
zcard
zcount
zincrby
zlexcount
zmscore
zpopmax
zpopmin
zrandmember
zrange
zrangebylex
zrangebyscore
zrank
zrem
zremrangebylex
zremrangebyrank
zremrangebyscore
zrevrange
zrevrangebylex
zrevrangebyscore
zrevrank
zscore
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList2 = '''
zdiff
zinter
zintercard
zunion
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList3 = '''
zdiffstore
zinterstore
zunionstore
'''.readLines().collect { it.trim() }.findAll { it }

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]
        def data4 = new byte[4][]
        int slotNumber = 128

        and:
        data4[1] = '2'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes

        when:
        def sZdiff = _ZGroup.parseSlots('zdiff', data1, slotNumber)
        def sList = _ZGroup.parseSlots('zxxx', data4, slotNumber)
        then:
        sZdiff.size() == 0
        sList.size() == 0

        when:
        def sListList1 = singleKeyCmdList1.collect {
            _ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList11 = singleKeyCmdList1.collect {
            _ZGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList1.size() == 22
        sListList1.every { it.size() == 1 }
        sListList11.size() == 22
        sListList11.every { it.size() == 0 }

        when:
        def sListList2 = multiKeyCmdList2.collect {
            _ZGroup.parseSlots(it, data4, slotNumber)
        }
        def sListList22 = multiKeyCmdList2.collect {
            _ZGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList2.size() == 4
        sListList2.every { it.size() > 1 }
        sListList22.size() == 4
        sListList22.every { it.size() == 0 }

        when:
        def data6 = new byte[6][]
        data6[1] = '5'.bytes
        data6[2] = 'a'.bytes
        data6[3] = 'b'.bytes
        data6[4] = 'c'.bytes
        data6[5] = 'd'.bytes
        def sListList222 = multiKeyCmdList2.collect {
            try {
                _ZGroup.parseSlots(it, data6, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList222.every { it == ErrorReply.SYNTAX }

        when:
        data6[1] = 'a'.bytes
        sListList222 = multiKeyCmdList2.collect {
            try {
                _ZGroup.parseSlots(it, data6, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList222.every { it == ErrorReply.SYNTAX }

        when:
        data6[1] = '1'.bytes
        sListList222 = multiKeyCmdList2.collect {
            try {
                _ZGroup.parseSlots(it, data6, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList222.every { it == ErrorReply.SYNTAX }

        when:
        def data5 = new byte[5][]
        data5[1] = 'dst'.bytes
        data5[2] = '2'.bytes
        data5[3] = 'a'.bytes
        data5[4] = 'b'.bytes
        def sListList3 = multiKeyCmdList3.collect {
            _ZGroup.parseSlots(it, data5, slotNumber)
        }
        def sListList33 = multiKeyCmdList3.collect {
            _ZGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList3.size() == 3
        // exclude dst key bytes
        sListList3.every { it.size() == 2 }
        sListList33.size() == 3
        sListList33.every { it.size() == 0 }

        when:
        def data7 = new byte[7][]
        data7[1] = 'dst'.bytes
        data7[2] = '5'.bytes
        data7[3] = 'a'.bytes
        data7[4] = 'b'.bytes
        data7[5] = 'c'.bytes
        data7[6] = 'd'.bytes
        def sListList333 = multiKeyCmdList3.collect {
            try {
                _ZGroup.parseSlots(it, data7, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList333.every { it == ErrorReply.SYNTAX }

        when:
        data7[2] = 'a'.bytes
        sListList333 = multiKeyCmdList3.collect {
            try {
                _ZGroup.parseSlots(it, data7, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList333.every { it == ErrorReply.SYNTAX }

        when:
        data7[2] = '1'.bytes
        sListList333 = multiKeyCmdList3.collect {
            try {
                _ZGroup.parseSlots(it, data7, slotNumber)
                return OKReply.INSTANCE
            } catch (ErrorReplyException e) {
                println e.message
                return ErrorReply.SYNTAX
            }
        }
        then:
        sListList333.every { it == ErrorReply.SYNTAX }

        when:
        // zrangestore
        data5[1] = 'dst'.bytes
        data5[2] = 'a'.bytes
        data5[3] = '0'.bytes
        data5[4] = '-1'.bytes
        def sZrangestoreList = _ZGroup.parseSlots('zrangestore', data5, slotNumber)
        then:
        sZrangestoreList.size() == 2

        when:
        sZrangestoreList = _ZGroup.parseSlots('zrangestore', data1, slotNumber)
        then:
        sZrangestoreList.size() == 0

        when:
        // zintercard
        data4[1] = '2'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes
        def sZintercardList = _ZGroup.parseSlots('zintercard', data4, slotNumber)
        then:
        sZintercardList.size() == 2

        when:
        sZintercardList = _ZGroup.parseSlots('zintercard', data1, slotNumber)
        then:
        sZintercardList.size() == 0
    }

    final short slot = 0

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def zGroup = new ZGroup('zadd', data1, null)
        zGroup.from(BaseCommand.mockAGroup())

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + multiKeyCmdList3 + ['zrangestore', 'zintercard']

        when:
        zGroup.data = data1
        def sAllList = allCmdList.collect {
            zGroup.cmd = it
            zGroup.handle()
        }
        then:
        sAllList.every {
            it == ErrorReply.FORMAT
        }

        when:
        zGroup.cmd = 'zzz'
        def reply = zGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    private RedisZSet fromMem(InMemoryGetSet inMemoryGetSet, String key) {
        def buf = inMemoryGetSet.getBuf(slot, key.bytes, 0, 0L)
        RedisZSet.decode(buf.cv().compressedData)
    }

    def 'test zadd'() {
        given:
        final short slot = 0

        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'member0'.bytes
        data6[4] = '1'.bytes
        data6[5] = 'member1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zadd', data6, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zadd', data6, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 0
        fromMem(inMemoryGetSet, 'a').get('member1').score() == 1

        when:
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data6[2] = 'a'.bytes
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data6[1] = 'a'.bytes
        data6[2] = '0'.bytes
        data6[3] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data6[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'nx'.bytes
        data4[3] = '0'.bytes
        zGroup.data = data4
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = 'nx'.bytes
        data7[3] = '0'.bytes
        data7[4] = 'member0'.bytes
        data7[5] = '1'.bytes
        data7[6] = 'member1'.bytes
        zGroup.data = data7
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'xx'.bytes
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'gt'.bytes
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'lt'.bytes
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data7[2] = 'incr'.bytes
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'incr'.bytes
        data5[3] = '0'.bytes
        data5[4] = 'member0'.bytes
        zGroup.data = data5
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[2] = 'ch'.bytes
        data5[3] = '1'.bytes
        reply = zGroup.zadd()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[3] = 'nx'.bytes
        data5[4] = 'lt'.bytes
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.SYNTAX

        when:
        rz.remove('member0')
        rz.remove('member1')
        RedisZSet.ZSET_MAX_SIZE.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data5[2] = 'ch'.bytes
        data5[3] = '0'.bytes
        data5[4] = 'extend_member0'.bytes
        reply = zGroup.zadd()
        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG
    }

    def 'test zcard'() {
        given:
        final short slot = 0

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zcard', data2, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zcard', data2, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zcard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zcard()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zcount'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zcount', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zcount', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zcount(false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcount(false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zcount(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '(member1'.bytes
        data4[3] = '(member4'.bytes
        reply = zGroup.zcount(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '[member1'.bytes
        data4[3] = '[member4'.bytes
        reply = zGroup.zcount(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data4[2] = '2'.bytes
        data4[3] = '3'.bytes
        reply = zGroup.zcount(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '-inf'.bytes
        data4[3] = '+inf'.bytes
        reply = zGroup.zcount(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        when:
        data4[2] = '3'.bytes
        data4[3] = '2'.bytes
        reply = zGroup.zcount(false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '[3'.bytes
        data4[3] = '[2'.bytes
        reply = zGroup.zcount(false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '[member3'.bytes
        data4[3] = '[member2'.bytes
        reply = zGroup.zcount(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zcount(false)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = '1'.bytes
        data4[3] = 'a'.bytes
        reply = zGroup.zcount(false)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = 'member1'.bytes
        reply = zGroup.zcount(true)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = '[member1'.bytes
        data4[3] = 'member4'.bytes
        reply = zGroup.zcount(true)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = '4'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zcount(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zdiff'() {
        given:
        final short slot = 0

        def data5 = new byte[5][]
        data5[1] = '2'.bytes
        data5[2] = 'a'.bytes
        data5[3] = 'b'.bytes
        data5[4] = 'withscores_'.bytes

        // zinter/zunion
        def data10 = new byte[10][]
        data10[1] = '2'.bytes
        data10[2] = 'a'.bytes
        data10[3] = 'b'.bytes
        data10[4] = 'weights'.bytes
        data10[5] = '1'.bytes
        data10[6] = '2'.bytes
        data10[7] = 'aggregate'.bytes
        data10[8] = 'sum'.bytes
        data10[9] = 'withscores_'.bytes

        def dstKeyBytes = 'dst'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zdiff', data5, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zdiff', data5, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzA = new RedisZSet()
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        rzA.add(0.1, 'member0')
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member0'.bytes

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[4] = 'withscores'.bytes
        data10[9] = 'withscores'.bytes
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member0'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '0.1'.bytes

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data5[4] = 'withscores_'.bytes
        data10[9] = 'withscores_'.bytes
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rzB.add(0.1, 'member0')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        zGroup.data = data5
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        zGroup.data = data5
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data10[8] = 'min'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data10[8] = 'max'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data10[8] = 'xxx'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[8] = 'sum'.bytes
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data10[8] = 'min'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data10[8] = 'max'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rzA.remove('member0')
        10.times {
            rzA.add(it as double, 'member' + it)
        }
        cvA.compressedData = rzA.encode()
        rzB.remove('member0')
        5.times {
            rzB.add(it as double, 'member' + it)
        }
        rzB.add(10.0, 'member10')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 5

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 5

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 11

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 11

        when:
        rzB.clear()
        RedisZSet.ZSET_MAX_SIZE.times {
            rzB.add(it as double, 'member' + it)
        }
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        zGroup.data = data10
        boolean exception = false
        String exceptionMessage = null
        try {
            zGroup.zdiff(false, true)
        } catch (ErrorReplyException e) {
            exception = true
            exceptionMessage = e.message
        }
        then:
        exception
        exceptionMessage.contains(ErrorReply.ZSET_SIZE_TO_LONG.message)

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
        zGroup.crossRequestWorker = true
        rzB.clear()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == MultiBulkReply.EMPTY
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        5.times {
            rzB.add(it as double, 'member' + it)
        }
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && ((MultiBulkReply) result).replies.length == 5
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 5
        }.result

        when:
        data10[9] = 'withscores'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && ((MultiBulkReply) result).replies.length == 10
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 10
        }.result

        when:
        data5[1] = 'a'.bytes
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data5[1] = '1'.bytes
        reply = zGroup.zdiff(false, false)
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[1] = '2'.bytes
        data5[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zdiff(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data6 = new byte[6][]
        data6[1] = '2'.bytes
        data6[2] = 'a'.bytes
        data6[3] = 'b'.bytes
        data6[4] = 'withscores_'.bytes
        data6[5] = 'c'.bytes
        zGroup.data = data6
        reply = zGroup.zdiff(false, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data6[1] = '5'.bytes
        reply = zGroup.zdiff(false, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data6[4] = 'weights'.bytes
        data6[5] = '1'.bytes
        reply = zGroup.zdiff(true, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data7 = new byte[7][]
        data7[1] = '2'.bytes
        data7[2] = 'a'.bytes
        data7[3] = 'b'.bytes
        data7[4] = 'weights'.bytes
        data7[5] = '1'.bytes
        data7[6] = 'a'.bytes
        zGroup.data = data7
        reply = zGroup.zdiff(true, false)
        then:
        reply == ErrorReply.NOT_FLOAT

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test zincrby'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'member0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zincrby', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zincrby', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zincrby()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '1.0'.bytes

        when:
        reply = zGroup.zincrby()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '2.0'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        RedisZSet.ZSET_MAX_SIZE.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[3] = 'extend_member0'.bytes
        reply = zGroup.zincrby()
        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG

        when:
        data4[3] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zincrby()
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zincrby()
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zincrby()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zintercard'() {
        // todo
        given:
        final short slot = 0

        def data6 = new byte[6][]
        data6[1] = '2'.bytes
        data6[2] = 'a'.bytes
        data6[3] = 'b'.bytes
        data6[4] = 'limit'.bytes
        data6[5] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zintercard', data6, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zintercard', data6, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzA = new RedisZSet()
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = zGroup.zintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rzA.add(0.1, 'member0')
        rzA.add(0.2, 'member1')
        rzA.add(0.3, 'member2')
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = zGroup.zintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zintercard()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rzB.add(0.1, 'member0')
        rzB.add(0.2, 'member1')
        rzB.add(0.4, 'member3')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data6[5] = '1'.bytes
        reply = zGroup.zintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        data6[5] = '3'.bytes
        reply = zGroup.zintercard()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rzB.remove('member0')
        rzB.remove('member1')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zintercard()
        then:
        reply == IntegerReply.REPLY_0

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
        zGroup.crossRequestWorker = true
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        rzB.add(0.1, 'member0')
        rzB.add(0.2, 'member1')
        rzB.add(0.4, 'member3')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 2
        }.result

        when:
        data6[5] = '1'.bytes
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        when:
        data6[5] = '0'.bytes
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 2
        }.result

        when:
        rzB.clear()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = zGroup.zintercard()
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        data6[4] = 'limit_'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data6[4] = 'limit'.bytes
        data6[5] = 'a'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data6[5] = '-1'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data6[5] = '0'.bytes
        data6[1] = '1'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data6[1] = 'a'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data6[1] = '2'.bytes
        data6[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data7 = new byte[7][]
        data7[1] = '6'.bytes
        data7[2] = 'a'.bytes
        data7[3] = 'b'.bytes
        data7[4] = 'c'.bytes
        data7[5] = 'd'.bytes
        data7[6] = 'e'.bytes
        zGroup.data = data7
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data7[1] = '4'.bytes
        data7[6] = 'limit'.bytes
        reply = zGroup.zintercard()
        then:
        reply == ErrorReply.SYNTAX

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test zmscore'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'member0'.bytes
        data4[3] = 'member1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zmscore', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zmscore', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zmscore()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zmscore()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zmscore()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == '0.1'.bytes
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        data4[2] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zmscore()
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'member0'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zmscore()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zpopmax'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zpopmax', data3, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zpopmax', data3, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zpopmax(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '100.0'.bytes

        when:
        rz.add(100, 'member100')
        rz.add(10, 'member10')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zpopmax(true)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member10'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '10.0'.bytes

        when:
        data3[2] = '0'.bytes
        reply = zGroup.zpopmax(false)
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = zGroup.zpopmax(false)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = '1'.bytes
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zpopmax(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        zGroup.data = data2
        reply = zGroup.zpopmax(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
    }

    def 'test zrandmember'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = 'withscores'.bytes

        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrandmember', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrandmember', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrandmember()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        zGroup.data = data4
        reply = zGroup.zrandmember()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()
        then:
        reply == NilReply.INSTANCE

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        zGroup.data = data4
        reply = zGroup.zrandmember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '100.0'.bytes

        when:
        zGroup.data = data2
        reply = zGroup.zrandmember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes

        when:
        data4[2] = '2'.bytes
        zGroup.data = data4
        reply = zGroup.zrandmember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        rz.remove('member100')
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '5'.bytes
        reply = zGroup.zrandmember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        data4[2] = '-3'.bytes
        reply = zGroup.zrandmember()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 6

        when:
        data4[3] = '_withscores'.bytes
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[3] = 'withscores'.bytes
        data4[2] = '0'.bytes
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'withscores'.bytes

        zGroup.data = data3
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        zGroup.data = data5
        reply = zGroup.zrandmember()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test zrange'() {
        given:
        final short slot = 0

        def data10 = new byte[10][]
        data10[1] = 'a'.bytes
        data10[2] = '(1'.bytes
        data10[3] = '(4'.bytes
        data10[4] = 'byscore'.bytes
        data10[5] = 'rev'.bytes
        data10[6] = 'limit'.bytes
        data10[7] = '0'.bytes
        data10[8] = '0'.bytes
        data10[9] = 'withscores'.bytes

        // no rev first
        data10[5] = 'byscore'.bytes

        def dstKeyBytes = 'dst'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrange', data10, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrange', data10, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // limit count
        data10[8] = '3'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // not withscores
        data10[9] = 'byscore'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit offset
        data10[7] = '1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        // limit offset
        data10[7] = '3'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit offset
        data10[7] = '0'.bytes
        // limit count
        data10[8] = '0'.bytes
        // rev
        data10[5] = 'rev'.bytes
        // start / stop
        data10[2] = '(4'.bytes
        data10[3] = '(1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        def tmpData5 = new byte[5][]
        tmpData5[1] = dstKeyBytes
        tmpData5[2] = 'a'.bytes
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        // rev
        data10[5] = 'byscore'.bytes
        // start / stop
        data10[2] = '(1'.bytes
        data10[3] = '(4'.bytes
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        inMemoryGetSet.getBuf(slot, dstKeyBytes, 0, 0L) != null

        when:
        // limit count
        data10[8] = '2'.bytes
        // query result count = 3
        data10[2] = '(1'.bytes
        data10[3] = '(5'.bytes
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[2] = '1.1'.bytes
        data10[3] = '1.2'.bytes
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '1.1'.bytes
        data10[3] = '1.2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.1'.bytes
        data10[3] = '[1.2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.1'.bytes
        data10[3] = '[1.a'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data10[5] = 'byscore'.bytes
        data10[2] = '[1.a'.bytes
        data10[3] = '[1.2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        // limit count
        data10[8] = '2'.bytes
        data10[5] = 'byscore'.bytes
        data10[2] = '-inf'.bytes
        data10[3] = '+inf'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[5] = 'byscore'.bytes
        data10[2] = '2'.bytes
        data10[3] = '1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[5] = 'rev'.bytes
        data10[2] = '1'.bytes
        data10[3] = '2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        // by index
        when:
        data10[4] = 'byindex'.bytes
        data10[5] = 'byindex'.bytes
        // withscores
        data10[9] = 'byindex'.bytes
        data10[2] = '0'.bytes
        data10[3] = '1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data10[2] = '-3'.bytes
        data10[3] = '-1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 3

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 3

        when:
        data10[2] = '1'.bytes
        data10[3] = '0'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[2] = '3'.bytes
        data10[3] = '6'.bytes
        data10[8] = '2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        // limit offset
        data10[7] = '1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        // limit offset
        data10[7] = '4'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit offset count
        data10[7] = '0'.bytes
        data10[8] = '2'.bytes
        data10[9] = 'withscores'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // limit count
        data10[8] = '0'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 8

        when:
        // limit count
        data10[8] = '0'.bytes
        data10[9] = 'withscores'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 8

        when:
        data10[8] = '2'.bytes
        data10[9] = 'byindex'.bytes
        data10[5] = 'rev'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        // bylex
        when:
        data10[4] = 'bylex'.bytes
        // rev
        data10[5] = 'bylex'.bytes
        // limit count
        data10[8] = '2'.bytes
        // withscores
        data10[9] = 'bylex'.bytes
        data10[2] = '(member1'.bytes
        data10[3] = '(member4'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data10[2] = '(member4'.bytes
        data10[3] = '(member1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[2] = '-'.bytes
        data10[3] = '+'.bytes
        data10[8] = '0'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        when:
        data10[2] = '('.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[2] = '-'.bytes
        data10[3] = '('.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[2] = '[member1'.bytes
        data10[3] = '[member4'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data10[2] = '[member10'.bytes
        data10[3] = '[member11'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[2] = '[member1'.bytes
        data10[3] = '[member8'.bytes
        // limit offset
        data10[7] = '3'.bytes
        data10[8] = '2'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data10[9] = 'withscores'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        data10[7] = '0'.bytes
        data10[8] = '0'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 16

        when:
        data10[7] = '10'.bytes
        data10[8] = '2'.bytes
        data10[9] = 'bylex'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(data10, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data10[5] = 'rev'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data10[2] = 'member1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[2] = '(member1'.bytes
        data10[3] = 'member4'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[7] = '-1'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data10[7] = 'a'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data10[7] = '0'.bytes
        data10[8] = 'a'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data10[8] = '0'.bytes
        data10[9] = 'withscores_'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[9] = 'limit'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data10[4] = 'byindex'.bytes
        data10[5] = 'byindex'.bytes
        data10[9] = 'withscores'.bytes
        data10[2] = 'a'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data10[2] = '1'.bytes
        data10[3] = 'a'.bytes
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data10[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrange(data10)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zrangebylex'() {
        given:
        final short slot = 0

        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = '(1'.bytes
        data7[3] = '(4'.bytes
        data7[4] = 'limit'.bytes
        data7[5] = '0'.bytes
        data7[6] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrangebylex', data7, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangebylex', data7, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes
        zGroup.data = data4
        reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def data5 = new byte[5][]
        zGroup.data = data5
        reply = zGroup.handle()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test zrangebyscore'() {
        given:
        final short slot = 0

        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = '(1'.bytes
        data7[3] = '(4'.bytes
        data7[4] = 'limit'.bytes
        data7[5] = '0'.bytes
        data7[6] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrangebyscore', data7, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangebyscore', data7, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes
        zGroup.data = data4
        reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrangestore'() {
        given:
        final short slot = 0

        def data8 = new byte[8][]
        data8[1] = 'dst'.bytes
        data8[2] = 'a'.bytes
        data8[3] = '1'.bytes
        data8[4] = '4'.bytes
        data8[5] = 'limit'.bytes
        data8[6] = '0'.bytes
        data8[7] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrangestore', data8, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', data8, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == IntegerReply.REPLY_0
    }

    def 'test zrevrange'() {
        given:
        final short slot = 0

        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = '1'.bytes
        data5[3] = '4'.bytes
        data5[4] = 'withscores'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrevrange', data5, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrevrange', data5, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrevrangebylex'() {
        given:
        final short slot = 0

        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = '(1'.bytes
        data7[3] = '(4'.bytes
        data7[4] = 'limit'.bytes
        data7[5] = '0'.bytes
        data7[6] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrevrangebylex', data7, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrevrangebylex', data7, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrevrangebyscore'() {
        given:
        final short slot = 0

        def data7 = new byte[7][]
        data7[1] = 'a'.bytes
        data7[2] = '(1'.bytes
        data7[3] = '(4'.bytes
        data7[4] = 'limit'.bytes
        data7[5] = '0'.bytes
        data7[6] = '0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrevrangebyscore', data7, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrevrangebyscore', data7, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.handle()
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrank'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'member0'.bytes
        data4[3] = 'withscore_'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrank', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrank', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrank(false)
        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'withscore'.bytes
        reply = zGroup.zrank(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[3] = 'withscore_'.bytes
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrank(false)
        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'withscore'.bytes
        reply = zGroup.zrank(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[3] = 'withscore_'.bytes
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrank(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = zGroup.zrank(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data4[3] = 'withscore'.bytes
        reply = zGroup.zrank(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 0
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '0.1'.bytes

        when:
        data4[2] = 'member1'.bytes
        data4[3] = 'withscore_'.bytes
        reply = zGroup.zrank(false)
        then:
        reply == NilReply.INSTANCE

        when:
        data4[3] = 'withscore'.bytes
        reply = zGroup.zrank(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data4[2] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zrank(false)
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'member0'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrank(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zrem'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'member0'.bytes
        data4[3] = 'member1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zrem', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrem', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zrem()
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrem()
        then:
        reply == IntegerReply.REPLY_0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.2, 'member1')
        rz.add(0.3, 'member2')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zrem()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.zrem()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zrem()
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data4[2] = 'member0'.bytes
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zrem()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zremrangebyscore'() {
        given:
        final short slot = 0

        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zremrangebyscore', data4, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zremrangebyscore', data4, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '1'.bytes
        data4[3] = '4'.bytes
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '(member1'.bytes
        data4[3] = '(member4'.bytes
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '1'.bytes
        data4[3] = '4'.bytes
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        data4[2] = '(1'.bytes
        data4[3] = '(4'.bytes
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '[1'.bytes
        data4[3] = '[4'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data4[2] = '[4'.bytes
        data4[3] = '[1'.bytes
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // remove again
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = '[a'.bytes
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = '[1'.bytes
        data4[3] = '[a'.bytes
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[2] = '-inf'.bytes
        data4[3] = '+inf'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        // by rank
        when:
        data4[2] = '1'.bytes
        data4[3] = '4'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data4[2] = '-3'.bytes
        data4[3] = '-1'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 3

        when:
        data4[3] = '-11'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = 'a'.bytes
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[2] = '1'.bytes
        data4[3] = 'a'.bytes
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[2] = '(member1'.bytes
        data4[3] = '(member4'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '[member1'.bytes
        data4[3] = '[member4'.bytes
        // reset 10 members
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        // remove again
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[2] = 'member0'.bytes
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = '(member0'.bytes
        data4[3] = 'member4'.bytes
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = '(member4'.bytes
        data4[3] = '(member0'.bytes
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.zremrangebyscore(false, false, false)
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test zscore'() {
        given:
        final short slot = 0

        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'member0'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup('zscore', data3, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zscore', data3, zGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.zscore()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zscore()
        then:
        reply == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = zGroup.zscore()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '0.1'.bytes

        when:
        data3[2] = 'member1'.bytes
        reply = zGroup.zscore()
        then:
        reply == NilReply.INSTANCE

        when:
        data3[2] = new byte[RedisZSet.ZSET_MEMBER_MAX_LENGTH + 1]
        reply = zGroup.zscore()
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        data3[2] = 'member0'.bytes
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = zGroup.zscore()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
