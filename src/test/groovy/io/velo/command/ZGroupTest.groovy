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
    final short slot = 0
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
            def ss = _ZGroup.parseSlots(it, data6, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
        }
        then:
        sListList222.every { it == ErrorReply.SYNTAX }

        when:
        data6[1] = 'a'.bytes
        sListList222 = multiKeyCmdList2.collect {
            def ss = _ZGroup.parseSlots(it, data6, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
        }
        then:
        sListList222.every { it == ErrorReply.SYNTAX }

        when:
        data6[1] = '1'.bytes
        sListList222 = multiKeyCmdList2.collect {
            def ss = _ZGroup.parseSlots(it, data6, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
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
            def ss = _ZGroup.parseSlots(it, data7, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
        }
        then:
        sListList333.every { it == ErrorReply.SYNTAX }

        when:
        data7[2] = 'a'.bytes
        sListList333 = multiKeyCmdList3.collect {
            def ss = _ZGroup.parseSlots(it, data7, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
        }
        then:
        sListList333.every { it == ErrorReply.SYNTAX }

        when:
        data7[2] = '1'.bytes
        sListList333 = multiKeyCmdList3.collect {
            def ss = _ZGroup.parseSlots(it, data7, slotNumber)
            ss.isEmpty() ? ErrorReply.SYNTAX : OKReply.INSTANCE
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
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zadd a 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 0
        fromMem(inMemoryGetSet, 'a').get('member1').score() == 1

        when:
        reply = zGroup.execute('zadd a 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = zGroup.execute('zadd a a member0 1 member1')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zadd a 0 >key 1 member1')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zadd >key 0 member0 1 member1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.execute('zadd a nx 0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a nx 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zadd a nx 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a xx 0 member0 1 member1')
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a xx 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = zGroup.execute('zadd a incr 0 member0 1 member1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = zGroup.execute('zadd a ch 1 member0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = zGroup.execute('zadd a ch nx lt')
        then:
        reply == ErrorReply.SYNTAX

        when:
        rz.remove('member0')
        rz.remove('member1')
        RedisZSet.ZSET_MAX_SIZE.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a ch 0 extend_member0')
        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG
    }

    def 'test zcard'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zcard a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zcard a')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = zGroup.execute('zcard >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zcount'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zcount a (1 (4')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zcount a (1 (4')
        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zcount a (1 (4')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zlexcount a (member1 (member4')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zlexcount a [member1 [member4')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        reply = zGroup.execute('zcount a 2 3')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zcount a -inf +inf')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        when:
        reply = zGroup.execute('zcount a 3 2')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zcount a [3 [2')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zlexcount a [member3 [member2')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zcount a a 3')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zcount a 1 a')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zlexcount a member1 [member4')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zlexcount a [member1 member4')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zcount >key 2 3')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zdiff'() {
        given:
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
        def errorReply = zGroup.zdiff(false, true)
        then:
        errorReply == ErrorReply.ZSET_SIZE_TO_LONG

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
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zincrby a 1 member0')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '1.0'.bytes

        when:
        reply = zGroup.execute('zincrby a 1 member0')
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zincrby a 1 extend_member0')
        then:
        reply == ErrorReply.ZSET_SIZE_TO_LONG

        when:
        reply = zGroup.execute('zincrby a 1 >key')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zincrby a a member0')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zincrby >key 1 member0')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zintercard'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzA = new RedisZSet()
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = zGroup.execute('zintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rzA.add(0.1, 'member0')
        rzA.add(0.2, 'member1')
        rzA.add(0.3, 'member2')
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = zGroup.execute('zintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.execute('zintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rzB.add(0.1, 'member0')
        rzB.add(0.2, 'member1')
        rzB.add(0.4, 'member3')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.execute('zintercard 2 a b limit 0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zintercard 2 a b limit 1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = zGroup.execute('zintercard 2 a b limit 3')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        rzB.remove('member0')
        rzB.remove('member1')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.execute('zintercard 2 a b limit 3')
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
        reply = zGroup.execute('zintercard 2 a b limit 3')
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
        reply = zGroup.execute('zintercard 2 a b limit 3')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 2
        }.result

        when:
        reply = zGroup.execute('zintercard 2 a b limit 1')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 1
        }.result

        when:
        reply = zGroup.execute('zintercard 2 a b limit 0')
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
        reply = zGroup.execute('zintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = zGroup.execute('zintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        zGroup.crossRequestWorker = false
        reply = zGroup.execute('zintercard 2 a b limit_ 0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zintercard 2 a b limit a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zintercard 2 a b limit -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zintercard 1 a b limit 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zintercard a a b limit 0')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zintercard 2 >key b limit 0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.execute('zintercard 6 a b c d e')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zintercard 4 a b c d limit')
        then:
        reply == ErrorReply.SYNTAX

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test zmscore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zmscore a member0 member1')
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zmscore a member0 member1')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zmscore a member0 member1')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == '0.1'.bytes
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zmscore a >key member1')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zmscore >key member0 member1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zpopmax'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zpopmax a 1')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zpopmax a 1')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zpopmax a 1')
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zpopmin a 1')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member10'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '10.0'.bytes

        when:
        reply = zGroup.execute('zpopmax a 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zpopmax a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zpopmax >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.execute('zpopmax a')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
    }

    def 'test zrandmember'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrandmember a 1 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrandmember a')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrandmember a 1 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrandmember a')
        then:
        reply == NilReply.INSTANCE

        when:
        rz.add(100, 'member100')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrandmember a 1 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '100.0'.bytes

        when:
        reply = zGroup.execute('zrandmember a')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'member100'.bytes

        when:
        reply = zGroup.execute('zrandmember a 2 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        rz.remove('member100')
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrandmember a 5 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        reply = zGroup.execute('zrandmember a -3 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 6

        when:
        reply = zGroup.execute('zrandmember a -3 _withscores')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrandmember a 0 withscores')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zrandmember a a withscores')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zrandmember >key 0 withscores')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.execute('zrandmember a withscores')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrandmember a 0 withscores x')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test zrange'() {
        given:
        def dstKeyBytes = 'dst'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // limit count
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 3 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        // not withscores
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 3 byscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 byscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit offset
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 1 0 byscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        // limit offset
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 3 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a (4 (1 byscore rev limit 0 0 byscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        def tmpData5 = new byte[5][]
        tmpData5[1] = dstKeyBytes
        tmpData5[2] = 'a'.bytes
        zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 byscore')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2
        inMemoryGetSet.getBuf(slot, dstKeyBytes, 0, 0L) != null

        when:
        zGroup.execute('zrange a (1 (5 byscore byscore limit 0 2 byscore')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        zGroup.execute('zrange a 1.1 1.2 byscore rev limit 0 0 byscore')
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a 1.1 1.2 byscore byscore limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrange a [1.1 [1.2 byscore byscore limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrange a [1.1 [1.a byscore byscore limit 0 0 byscore')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zrange a [1.a [1.2 byscore byscore limit 0 0 byscore')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = zGroup.execute('zrange a -inf +inf byscore byscore limit 0 2 byscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        // limit count
        reply = zGroup.execute('zrange a 2 1 byscore byscore limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit count
        reply = zGroup.execute('zrange a 1 2 byscore rev limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        // by index
        when:
        reply = zGroup.execute('zrange a 0 1 byindex byindex limit 0 0 byindex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrange a -3 -1 byindex byindex limit 0 0 byindex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 3

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 3

        when:
        reply = zGroup.execute('zrange a 1 0 byindex byindex limit 0 0 byindex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 2 byindex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 1 2 byindex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 4 2 byindex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 2 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 0 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 8

        when:
        reply = zGroup.execute('zrange a 3 6 byindex rev limit 0 2 byindex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        // bylex
        when:
        reply = zGroup.execute('zrange a (member1 (member4 bylex bylex limit 0 2 bylex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrange a (member4 (member1 bylex bylex limit 0 2 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a - + bylex bylex limit 0 0 bylex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 10

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        when:
        reply = zGroup.execute('zrange a ( + bylex bylex limit 0 0 bylex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a - ( bylex bylex limit 0 0 bylex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a [member1 [member4 bylex bylex limit 0 0 bylex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        reply = zGroup.execute('zrange a [member10 [member11 bylex bylex limit 0 0 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 3 2 bylex')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 3 2 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 4

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 0 0 withscores')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 16

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 10 2 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.zrange(zGroup.data, dstKeyBytes)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex rev limit 10 2 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrange a member1 [member8 bylex rev limit 10 2 bylex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit 10 2 bylex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit -1 2 bylex')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit a 2 bylex')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit 0 a bylex')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit 0 0 withscores_')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit 0 0 limit')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a a member4 byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zrange a 1 a byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = zGroup.execute('zrange >key 1 a byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zrangebylex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrangebylex a (1 (4 limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrangebylex a (1 (4')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrangebylex a (1 (4 x')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test zrangebyscore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrangebyscore a (1 (4 limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrangebyscore a (1 (4')
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrangestore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrangestore dst a 1 4 limit 0 0')
        then:
        reply == IntegerReply.REPLY_0
    }

    def 'test zrevrange'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrevrange a 1 4 withscores')
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrevrangebylex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrevrangebylex a (1 (4 limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrevrangebyscore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrevrangebyscore a (1 (4 limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY
    }

    def 'test zrank'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrank a member0 withscore_')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrank a member0 withscore_')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrank a member0 withscore_')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = zGroup.execute('zrevrank a member0 withscore_')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof IntegerReply
        ((IntegerReply) ((MultiBulkReply) reply).replies[0]).integer == 0
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == '0.1'.bytes

        when:
        reply = zGroup.execute('zrank a member1 withscore_')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zrank a member1 withscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrank a >key withscore')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zrank >key member1 withscore')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zrem'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zrem a member0 member1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrem a member0 member1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.2, 'member1')
        rz.add(0.3, 'member2')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrem a member0 member1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = zGroup.execute('zrem a member0 member1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrem a >key member1')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zrem >key member0 member1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zremrangebyscore'() {
        given:
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
        inMemoryGetSet.put('a', cv)
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
        inMemoryGetSet.put('a', cv)
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
        inMemoryGetSet.put('a', cv)
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 10

        // by rank
        when:
        data4[2] = '1'.bytes
        data4[3] = '4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 4

        when:
        data4[2] = '-3'.bytes
        data4[3] = '-1'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 3

        when:
        data4[3] = '-11'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
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
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data4[2] = '[member1'.bytes
        data4[3] = '[member4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
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
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zscore a member0')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zscore a member0')
        then:
        reply == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zscore a member0')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '0.1'.bytes

        when:
        reply = zGroup.execute('zscore a member1')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zscore a >key')
        then:
        reply == ErrorReply.ZSET_MEMBER_LENGTH_TO_LONG

        when:
        reply = zGroup.execute('zscore >key member0')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
