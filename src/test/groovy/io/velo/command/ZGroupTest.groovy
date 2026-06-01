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
zscan
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

    def 'test handle - format errors'() {
        given:
        def zGroup = new ZGroup(null, null, null)
        zGroup.from(BaseCommand.mockAGroup())

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + multiKeyCmdList3 + ['zrangestore', 'zintercard']

        when:
        def replyList = allCmdList.collect { zGroup.execute(it) }
        then:
        replyList.every { it == ErrorReply.FORMAT }

        expect:
        zGroup.execute('zzz') == NilReply.INSTANCE
    }

    private RedisZSet fromMem(InMemoryGetSet inMemoryGetSet, String key) {
        def buf = inMemoryGetSet.getBuf(slot, key, 0, 0L)
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
        (reply as IntegerReply).integer == 2
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 0
        fromMem(inMemoryGetSet, 'a').get('member1').score() == 1

        when:
        reply = zGroup.execute('zadd a 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

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
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zadd a nx 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a xx 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

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
        (reply as IntegerReply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        rz.add(10, 'member0')
        rz.add(11, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        rz.add(0.1, 'member0')
        rz.add(0.1, 'member1')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 0 member0 1 member1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = zGroup.execute('zadd a incr 0 member0 1 member1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = zGroup.execute('zadd a ch 1 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as IntegerReply).integer == 1

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
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zlexcount a (member1 (member4')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zlexcount a [member1 [member4')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

        when:
        reply = zGroup.execute('zcount a 2 3')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zcount a -inf +inf')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

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
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member0'.bytes

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        data5[4] = 'withscores'.bytes
        data10[9] = 'withscores'.bytes
        zGroup.data = data5
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member0'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == '0.1'.bytes

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 2

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        zGroup.data = data5
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        data10[8] = 'min'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        data10[8] = 'max'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        data10[8] = 'min'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        data10[8] = 'max'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        (reply as MultiBulkReply).replies.length == 5

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 5

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 5

        when:
        zGroup.data = data10
        reply = zGroup.zdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 11

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 11

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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == MultiBulkReply.EMPTY
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && (result as MultiBulkReply).replies.length == 5
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 5
        }.result

        when:
        data10[9] = 'withscores'.bytes
        zGroup.data = data10
        reply = zGroup.zdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && (result as MultiBulkReply).replies.length == 10
        }.result

        when:
        zGroup.addDstKeyBytesForStore(dstKeyBytes)
        reply = zGroup.zdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 10
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
        (reply as BulkReply).raw == '1.0'.bytes

        when:
        reply = zGroup.execute('zincrby a 1 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == '2.0'.bytes

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
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zintercard 2 a b limit 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = zGroup.execute('zintercard 2 a b limit 3')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

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
        (reply as AsyncReply).settablePromise.whenResult { result ->
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
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 2
        }.result

        when:
        reply = zGroup.execute('zintercard 2 a b limit 1')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 1
        }.result

        when:
        reply = zGroup.execute('zintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 2
        }.result

        when:
        rzB.clear()
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.execute('zintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = zGroup.execute('zintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
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

    def 'test zunionstore maintains sorted order after score mutation'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        def cvA = Mock.prepareCompressedValueList(1)[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzA = new RedisZSet()
        rzA.add(1.0, 'a')
        rzA.add(2.0, 'b')
        rzA.add(3.0, 'c')
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)

        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        rzB.add(100.0, 'a')
        rzB.add(4.0, 'd')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)

        def reply = zGroup.execute('zunionstore dst 2 a b')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

        when:
        def resultCv = inMemoryGetSet.getBuf(slot, 'dst', 0, 0L).cv()
        def resultRz = RedisZSet.decode(resultCv.compressedData)
        def membersInOrder = resultRz.getSet().collect { it.member() }
        def scoresInOrder = resultRz.getSet().collect { it.score() }
        then:
        membersInOrder == ['b', 'c', 'd', 'a']
        scoresInOrder == [2.0, 3.0, 4.0, 101.0]
    }

    def 'test zinterstore maintains sorted order after score mutation'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when:
        def cvA = Mock.prepareCompressedValueList(1)[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzA = new RedisZSet()
        rzA.add(1.0, 'a')
        rzA.add(2.0, 'b')
        rzA.add(3.0, 'c')
        cvA.compressedData = rzA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)

        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        rzB.add(100.0, 'a')
        rzB.add(200.0, 'b')
        rzB.add(4.0, 'd')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)

        def reply = zGroup.execute('zinterstore dst 2 a b')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        def resultCv = inMemoryGetSet.getBuf(slot, 'dst', 0, 0L).cv()
        def resultRz = RedisZSet.decode(resultCv.compressedData)
        def membersInOrder = resultRz.getSet().collect { it.member() }
        def scoresInOrder = resultRz.getSet().collect { it.score() }
        then:
        membersInOrder == ['a', 'b']
        scoresInOrder == [101.0, 202.0]
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
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zmscore a member0 member1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        rz.add(0.1, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zmscore a member0 member1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == '0.1'.bytes
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

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
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member100'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == '100.0'.bytes

        when:
        rz.add(100, 'member100')
        rz.add(10, 'member10')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zpopmin a 1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member10'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == '10.0'.bytes

        when:
        reply = zGroup.execute('zpopmax a 0')
        then:
        reply == ErrorReply.VALUE_NOT_POSITIVE

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
        (reply as MultiBulkReply).replies.length == 2
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
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member100'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == '100.0'.bytes

        when:
        reply = zGroup.execute('zrandmember a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'member100'.bytes

        when:
        reply = zGroup.execute('zrandmember a 2 withscores')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

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
        (reply as MultiBulkReply).replies.length == 10

        when:
        reply = zGroup.execute('zrandmember a -3 withscores')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 6

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
        def dstKey = 'dst'

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
        reply = zGroup.zrange(zGroup.data, dstKey)
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
        reply = zGroup.zrange(zGroup.data, dstKey)
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
        reply == MultiBulkReply.EMPTY

        when:
        // limit count
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 3 withscores')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 4

        when:
        // not withscores
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 3 byscore')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        // limit count
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        // limit offset with count > 0
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 1 1 byscore')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        // limit offset beyond range
        reply = zGroup.execute('zrange a (1 (4 byscore byscore limit 3 1 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def tmpData5 = new byte[5][]
        tmpData5[1] = dstKey
        tmpData5[2] = 'a'.bytes
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a (4 (1 byscore rev limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.execute('zrange a (1 (4 byscore byscore limit 0 2 byscore')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        zGroup.execute('zrange a (1 (5 byscore byscore limit 0 2 byscore')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        zGroup.execute('zrange a 1.1 1.2 byscore rev limit 0 0 byscore')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
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
        (reply as MultiBulkReply).replies.length == 2

        when:
        // limit count
        reply = zGroup.execute('zrange a 2 1 byscore byscore limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // limit count
        reply = zGroup.execute('zrange a 1 2 byscore rev limit 0 0 byscore')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        // by index + limit is syntax error (Redis only allows LIMIT with BYSCORE/BYLEX)
        when:
        reply = zGroup.execute('zrange a 0 1 byindex byindex limit 0 0 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a -3 -1 byindex byindex limit 0 0 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 1 0 byindex byindex limit 0 0 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 2 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 1 2 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 4 2 byindex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 2 withscores')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 3 6 byindex rev limit 0 2 byindex')
        then:
        reply == ErrorReply.SYNTAX

        // bylex
        when:
        reply = zGroup.execute('zrange a (member1 (member4 bylex bylex limit 0 2 bylex')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zrange a (member4 (member1 bylex bylex limit 0 2 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a - + bylex bylex limit 0 0 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

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
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a [member10 [member11 bylex bylex limit 0 0 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 3 2 bylex')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 3 2 withscores')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 4

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 0 0 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrange a [member1 [member8 bylex bylex limit 10 2 bylex')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData5, zGroup.slotNumber)
        reply = zGroup.zrange(zGroup.data, dstKey)
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
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange a 1 a byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zrange >key 1 a byindex byindex limit 0 0 withscores')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test zrange limit 0 0 returns empty'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        10.times {
            rz.add(it as double, 'member' + it)
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)

        when: 'BYSCORE LIMIT 0 0 should return empty'
        def reply = zGroup.execute('zrange a -inf +inf byscore limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY

        when: 'BYLEX LIMIT 0 0 should return empty'
        reply = zGroup.execute('zrange a - + bylex limit 0 0')
        then:
        reply == MultiBulkReply.EMPTY

        when: 'BYINDEX with LIMIT should be syntax error (Redis only allows LIMIT with BYSCORE/BYLEX)'
        reply = zGroup.execute('zrange a 0 -1 limit 0 0')
        then:
        reply == ErrorReply.SYNTAX

        when: 'BYSCORE LIMIT 0 -1 should return all members (negative count = no limit)'
        reply = zGroup.execute('zrange a -inf +inf byscore limit 0 -1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10

        when: 'BYSCORE LIMIT 0 3 should return 3 members'
        reply = zGroup.execute('zrange a -inf +inf byscore limit 0 3')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3

        when: 'BYSCORE LIMIT 2 0 should return empty (count=0)'
        reply = zGroup.execute('zrange a -inf +inf byscore limit 2 0')
        then:
        reply == MultiBulkReply.EMPTY

        when: 'no LIMIT, BYINDEX returns all members'
        reply = zGroup.execute('zrange a 0 -1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10

        when: 'no LIMIT, BYSCORE returns all members'
        reply = zGroup.execute('zrange a -inf +inf byscore')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10
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
        reply == ErrorReply.SYNTAX
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

    def 'test zrevrangebylex with data'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'a')
        rz.add(2.0, 'b')
        rz.add(3.0, 'c')
        rz.add(4.0, 'd')
        rz.add(5.0, 'e')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)

        when: 'ZREVRANGEBYLEX + - returns all members reversed'
        def reply = zGroup.execute('zrevrangebylex a + -')
        then:
        reply instanceof MultiBulkReply
        def members1 = (reply as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members1 == ['e', 'd', 'c', 'b', 'a']

        when: 'ZRANGE + - REV BYLEX returns all members reversed'
        def reply2 = zGroup.execute('zrange a + - rev bylex')
        then:
        reply2 instanceof MultiBulkReply
        def members2 = (reply2 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members2 == ['e', 'd', 'c', 'b', 'a']

        when: 'ZREVRANGEBYLEX [d [b returns d,c,b'
        def reply3 = zGroup.execute('zrevrangebylex a [d [b')
        then:
        reply3 instanceof MultiBulkReply
        def members3 = (reply3 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members3 == ['d', 'c', 'b']

        when: 'ZRANGE [d [b REV BYLEX returns d,c,b'
        def reply4 = zGroup.execute('zrange a [d [b rev bylex')
        then:
        reply4 instanceof MultiBulkReply
        def members4 = (reply4 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members4 == ['d', 'c', 'b']

        when: 'ZRANGE [b [d REV BYLEX returns empty (min<max in REV)'
        def reply5 = zGroup.execute('zrange a [b [d rev bylex')
        then:
        reply5 == MultiBulkReply.EMPTY

        when: 'ZREVRANGEBYLEX + (c returns e,d (exclusive c)'
        def reply6 = zGroup.execute('zrevrangebylex a + (c')
        then:
        reply6 instanceof MultiBulkReply
        def members6 = (reply6 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members6 == ['e', 'd']

        when: 'ZREVRANGEBYLEX [b + returns empty (b < +, wrong direction)'
        def reply7 = zGroup.execute('zrevrangebylex a [b +')
        then:
        reply7 == MultiBulkReply.EMPTY

        when: 'ZREVRANGEBYLEX + [b returns e,d,c,b (+ > b, correct direction)'
        def reply8 = zGroup.execute('zrevrangebylex a + [b')
        then:
        reply8 instanceof MultiBulkReply
        def members8 = (reply8 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members8 == ['e', 'd', 'c', 'b']

        when: 'ZREVRANGEBYLEX [d - returns d,c,b,a (from d down to -)'
        def reply9 = zGroup.execute('zrevrangebylex a [d -')
        then:
        reply9 instanceof MultiBulkReply
        def members9 = (reply9 as MultiBulkReply).replies.collect { new String((it as BulkReply).raw) }
        members9 == ['d', 'c', 'b', 'a']
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

    def 'test zrangestore rev limit'() {
        given:
        def dstKey = 'dst'
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'a')
        rz.add(2.0, 'b')
        rz.add(3.0, 'c')
        rz.add(4.0, 'd')
        rz.add(5.0, 'e')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)

        def tmpData = new byte[5][]
        tmpData[1] = dstKey.getBytes()
        tmpData[2] = 'a'.getBytes()

        when: 'ZRANGESTORE BYSCORE REV LIMIT 1 1 — range 5 (1, skip e, store d'
        zGroup.execute('zrange a 5 (1 byscore rev limit 1 1')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData, zGroup.slotNumber)
        def reply = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'verify stored member is d (not c)'
        def storedCv = inMemoryGetSet.getBuf(slot, dstKey, 0, 0L)
        def storedRz = RedisZSet.decode(storedCv.cv().compressedData)
        then:
        storedRz.contains('d')
        !storedRz.contains('c')
        !storedRz.contains('e')

        when: 'ZRANGESTORE BYSCORE REV LIMIT 0 2 — no skip, store e,d'
        zGroup.execute('zrange a 5 (1 byscore rev limit 0 2')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData, zGroup.slotNumber)
        def replyBs2 = zGroup.zrange(zGroup.data, dstKey)
        then:
        replyBs2 instanceof IntegerReply
        (replyBs2 as IntegerReply).integer == 2

        when: 'verify BYSCORE stored e,d'
        def storedCvBs2 = inMemoryGetSet.getBuf(slot, dstKey, 0, 0L)
        def storedRzBs2 = RedisZSet.decode(storedCvBs2.cv().compressedData)
        then:
        storedRzBs2.contains('e')
        storedRzBs2.contains('d')
        !storedRzBs2.contains('c')

        when: 'ZRANGESTORE BYLEX REV LIMIT 1 2 — skip e, store d,c'
        zGroup.execute('zrange a + - bylex rev limit 1 2')
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zrangestore', tmpData, zGroup.slotNumber)
        def reply2 = zGroup.zrange(zGroup.data, dstKey)
        then:
        reply2 instanceof IntegerReply
        (reply2 as IntegerReply).integer == 2
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
        (reply as IntegerReply).integer == 0

        when:
        reply = zGroup.execute('zrevrank a member0 withscore_')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[0]).integer == 0
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == '0.1'.bytes

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
        (reply as IntegerReply).integer == 2

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
        (reply as IntegerReply).integer == 2

        when:
        data4[2] = '[1'.bytes
        data4[3] = '[4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

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
        (reply as IntegerReply).integer == 10

        // by rank
        when:
        data4[2] = '1'.bytes
        data4[3] = '4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

        when:
        data4[2] = '-3'.bytes
        data4[3] = '-1'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3

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
        (reply as IntegerReply).integer == 2

        when:
        data4[2] = '[member1'.bytes
        data4[3] = '[member4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 4

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
        (reply as BulkReply).raw == '0.1'.bytes

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

    def 'test zscan'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when: // key does not exist
        inMemoryGetSet.remove(slot, 'a')
        def reply = zGroup.execute('zscan a 0')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when: // key exists but zset is empty
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zscan a 0')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when: // zset with 3 members, scan all at once
        rz.add(1.0, 'x')
        rz.add(2.0, 'y')
        rz.add(3.0, 'z')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zscan a 0')
        then:
        reply instanceof MultiBulkReply
        def scanReply = reply as MultiBulkReply
        scanReply.replies.length == 2
        scanReply.replies[0] == BulkReply.ZERO
        // members with scores: x 1.0 y 2.0 z 3.0
        def membersReply = scanReply.replies[1] as MultiBulkReply
        membersReply.replies.length == 6
        new String((membersReply.replies[0] as BulkReply).raw) == 'x'
        new String((membersReply.replies[1] as BulkReply).raw) == '1.0'
        new String((membersReply.replies[2] as BulkReply).raw) == 'y'
        new String((membersReply.replies[3] as BulkReply).raw) == '2.0'
        new String((membersReply.replies[4] as BulkReply).raw) == 'z'
        new String((membersReply.replies[5] as BulkReply).raw) == '3.0'

        when: // COUNT 2 (partial page)
        reply = zGroup.execute('zscan a 0 count 2')
        then:
        reply instanceof MultiBulkReply
        def scanReply2 = reply as MultiBulkReply
        scanReply2.replies.length == 2
        // next cursor should not be 0 (not at end yet)
        def nextCursor2 = new String((scanReply2.replies[0] as BulkReply).raw)
        nextCursor2 != '0'
        def membersReply2 = scanReply2.replies[1] as MultiBulkReply
        // 2 members with scores = 4 elements
        membersReply2.replies.length == 4
        new String((membersReply2.replies[0] as BulkReply).raw) == 'x'
        new String((membersReply2.replies[1] as BulkReply).raw) == '1.0'
        new String((membersReply2.replies[2] as BulkReply).raw) == 'y'
        new String((membersReply2.replies[3] as BulkReply).raw) == '2.0'

        when: // multi-page pagination loop
        rz.add(4.0, 'm')
        rz.add(5.0, 'n')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        def allPairs = []
        def nextCursor = '0'
        int pageCount = 0
        while (true) {
            reply = zGroup.execute("zscan a $nextCursor count 2")
            assert reply instanceof MultiBulkReply
            def page = reply as MultiBulkReply
            nextCursor = new String((page.replies[0] as BulkReply).raw)
            def pageMembers = (page.replies[1] as MultiBulkReply).replies
            for (int i = 0; i < pageMembers.length; i += 2) {
                allPairs.add(new String((pageMembers[i] as BulkReply).raw))
            }
            pageCount++
            if (nextCursor == '0') break
            if (pageCount > 20) assert false: 'infinite loop in zscan pagination'
        }
        then:
        allPairs.size() == rz.size()
        allPairs.toSet().size() == allPairs.size()
        pageCount >= 3

        when: // MATCH x*
        rz.remove('m')
        rz.remove('n')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zscan a 0 match x*')
        then: // only 'x' matches
        reply instanceof MultiBulkReply
        def scanReply3 = reply as MultiBulkReply
        (scanReply3.replies[1] as MultiBulkReply).replies.length == 2
        new String(((scanReply3.replies[1] as MultiBulkReply).replies[0] as BulkReply).raw) == 'x'
        new String(((scanReply3.replies[1] as MultiBulkReply).replies[1] as BulkReply).raw) == '1.0'

        when: // MATCH with no results
        reply = zGroup.execute('zscan a 0 match nomatch*')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when: // error: missing cursor
        reply = zGroup.execute('zscan a')
        then:
        reply == ErrorReply.FORMAT

        when: // error: cursor not a number
        reply = zGroup.execute('zscan a notanumber')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: // error: key too long
        reply = zGroup.execute('zscan >key 0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: // error: match without pattern
        reply = zGroup.execute('zscan a 0 match')
        then:
        reply == ErrorReply.SYNTAX

        when: // error: count without value
        reply = zGroup.execute('zscan a 0 count')
        then:
        reply == ErrorReply.SYNTAX

        when: // error: count not a number
        reply = zGroup.execute('zscan a 0 count notanumber')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: // error: count <= 0
        reply = zGroup.execute('zscan a 0 count -1')
        then:
        reply == ErrorReply.SYNTAX

        when: // error: count 0
        reply = zGroup.execute('zscan a 0 count 0')
        then:
        reply == ErrorReply.SYNTAX

        when: // error: unknown option
        reply = zGroup.execute('zscan a 0 unknownoption')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test zmpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when: 'format error - too few args'
        def reply = zGroup.execute('zmpop')
        then:
        reply == ErrorReply.FORMAT

        when: 'format error - missing direction'
        reply = zGroup.execute('zmpop 1 myzset')
        then:
        reply == ErrorReply.FORMAT

        when: 'numkeys not integer'
        reply = zGroup.execute('zmpop abc myzset MIN')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'numkeys zero'
        reply = zGroup.execute('zmpop 0 myzset MIN')
        then:
        reply == ErrorReply.RANGE_OUT_OF_INDEX

        when: 'wrong direction'
        reply = zGroup.execute('zmpop 1 myzset UP')
        then:
        reply == ErrorReply.SYNTAX

        when: 'count not integer'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT abc')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'count zero rejected'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT 0')
        then:
        reply == ErrorReply.VALUE_NOT_POSITIVE

        when: 'count negative'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT -1')
        then:
        reply == ErrorReply.VALUE_NOT_POSITIVE

        when: 'syntax error - extra args'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT 1 extra')
        then:
        reply == ErrorReply.SYNTAX

        when: 'syntax error - bad option'
        reply = zGroup.execute('zmpop 1 myzset MIN BADOPTION 5')
        then:
        reply == ErrorReply.SYNTAX

        when: 'key too long'
        reply = zGroup.execute('zmpop 1 >key MIN')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'all keys missing'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        reply = zGroup.execute('zmpop 2 a b MIN')
        then:
        reply == NilReply.INSTANCE

        when: 'single key, pop MIN, default count 1'
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        rz.add(1.0, 'one')
        rz.add(2.0, 'two')
        rz.add(3.0, 'three')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'myzset', 0, cv)
        reply = zGroup.execute('zmpop 1 myzset MIN')
        def mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        mb.replies.length == 2
        (mb.replies[0] as BulkReply).raw == 'myzset'.bytes
        def valuesReply = mb.replies[1] as MultiBulkReply
        valuesReply.replies.length == 2
        (valuesReply.replies[0] as BulkReply).raw == 'one'.bytes
        (valuesReply.replies[1] as BulkReply).raw == '1.0'.bytes

        when: 'single key, pop MAX'
        reply = zGroup.execute('zmpop 1 myzset MAX')
        mb = reply as MultiBulkReply
        def valuesReply2 = mb.replies[1] as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        valuesReply2.replies.length == 2
        (valuesReply2.replies[0] as BulkReply).raw == 'three'.bytes
        (valuesReply2.replies[1] as BulkReply).raw == '3.0'.bytes

        when: 'single key, COUNT 2 (only 1 element left)'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT 2')
        mb = reply as MultiBulkReply
        def valuesReply3 = mb.replies[1] as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        valuesReply3.replies.length == 2
        (valuesReply3.replies[0] as BulkReply).raw == 'two'.bytes
        (valuesReply3.replies[1] as BulkReply).raw == '2.0'.bytes

        when: 'zset now empty, key removed'
        reply = zGroup.execute('zmpop 1 myzset MIN')
        then:
        reply == NilReply.INSTANCE

        when: 'first key empty, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvB = Mock.prepareCompressedValueList(1)[0]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzB = new RedisZSet()
        rzB.add(10.0, 'x')
        rzB.add(20.0, 'y')
        cvB.compressedData = rzB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = zGroup.execute('zmpop 2 a b MIN COUNT 3')
        mb = reply as MultiBulkReply
        def valuesReply4 = mb.replies[1] as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        (mb.replies[0] as BulkReply).raw == 'b'.bytes
        valuesReply4.replies.length == 4
        (valuesReply4.replies[0] as BulkReply).raw == 'x'.bytes
        (valuesReply4.replies[1] as BulkReply).raw == '10.0'.bytes
        (valuesReply4.replies[2] as BulkReply).raw == 'y'.bytes
        (valuesReply4.replies[3] as BulkReply).raw == '20.0'.bytes

        when: 'wrong type - key is not a zset'
        inMemoryGetSet.remove(slot, 'mylist')
        def cvL = Mock.prepareCompressedValueList(1)[0]
        cvL.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvL.compressedData = new byte[0]
        inMemoryGetSet.put(slot, 'mylist', 0, cvL)
        reply = zGroup.execute('zmpop 1 mylist MIN')
        then:
        reply == ErrorReply.WRONG_TYPE

        when: 'format error - numkeys claims more keys than provided'
        reply = zGroup.execute('zmpop 3 a b MIN')
        then:
        reply == ErrorReply.FORMAT

        when: 'same-slot - first key empty zset skipped, second has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvEmptySlot = Mock.prepareCompressedValueList(1)[0]
        cvEmptySlot.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzEmptySlot = new RedisZSet()
        cvEmptySlot.compressedData = rzEmptySlot.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvEmptySlot)
        def cvBSlot = Mock.prepareCompressedValueList(1)[0]
        cvBSlot.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzBSlot = new RedisZSet()
        rzBSlot.add(7.0, 'seven')
        cvBSlot.compressedData = rzBSlot.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvBSlot)
        reply = zGroup.execute('zmpop 2 a b MIN')
        then:
        reply instanceof MultiBulkReply
        def mbSlot = reply as MultiBulkReply
        (mbSlot.replies[0] as BulkReply).raw == 'b'.bytes
        def vrSlot = mbSlot.replies[1] as MultiBulkReply
        vrSlot.replies.length == 2
        (vrSlot.replies[0] as BulkReply).raw == 'seven'.bytes
        (vrSlot.replies[1] as BulkReply).raw == '7.0'.bytes

        when: 'cross-slot - all keys missing'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
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
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        def asyncResult = (reply as AsyncReply).settablePromise.getResult()
        then:
        reply instanceof AsyncReply
        asyncResult == NilReply.INSTANCE

        when: 'cross-slot - first key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvX = Mock.prepareCompressedValueList(1)[0]
        cvX.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzX = new RedisZSet()
        rzX.add(1.0, 'one')
        rzX.add(2.0, 'two')
        cvX.compressedData = rzX.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvX)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        asyncResult = (reply as AsyncReply).settablePromise.getResult()
        then:
        reply instanceof AsyncReply
        asyncResult instanceof MultiBulkReply
        def mbCs = asyncResult as MultiBulkReply
        mbCs.replies.length == 2
        (mbCs.replies[0] as BulkReply).raw == 'a'.bytes
        def vrCs = mbCs.replies[1] as MultiBulkReply
        vrCs.replies.length == 2
        (vrCs.replies[0] as BulkReply).raw == 'one'.bytes
        (vrCs.replies[1] as BulkReply).raw == '1.0'.bytes

        when: 'cross-slot - first empty, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvY = Mock.prepareCompressedValueList(1)[0]
        cvY.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzY = new RedisZSet()
        rzY.add(10.0, 'x')
        rzY.add(20.0, 'y')
        rzY.add(30.0, 'z')
        cvY.compressedData = rzY.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvY)
        reply = zGroup.execute('zmpop 2 a b MAX COUNT 2')
        eventloopCurrent.run()
        asyncResult = (reply as AsyncReply).settablePromise.getResult()
        def mbCs2 = asyncResult as MultiBulkReply
        def vrCs2 = mbCs2.replies[1] as MultiBulkReply
        then:
        reply instanceof AsyncReply
        asyncResult instanceof MultiBulkReply
        mbCs2.replies.length == 2
        (mbCs2.replies[0] as BulkReply).raw == 'b'.bytes
        vrCs2.replies.length == 4
        (vrCs2.replies[0] as BulkReply).raw == 'z'.bytes
        (vrCs2.replies[1] as BulkReply).raw == '30.0'.bytes
        (vrCs2.replies[2] as BulkReply).raw == 'y'.bytes
        (vrCs2.replies[3] as BulkReply).raw == '20.0'.bytes

        when: 'cross-slot - first key empty zset, second key has data'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvEmpty = Mock.prepareCompressedValueList(1)[0]
        cvEmpty.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzEmpty = new RedisZSet()
        cvEmpty.compressedData = rzEmpty.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvEmpty)
        def cvY2 = Mock.prepareCompressedValueList(1)[0]
        cvY2.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzY2 = new RedisZSet()
        rzY2.add(5.0, 'm')
        cvY2.compressedData = rzY2.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvY2)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        asyncResult = (reply as AsyncReply).settablePromise.getResult()
        def mbCs3 = asyncResult as MultiBulkReply
        def vrCs3 = mbCs3.replies[1] as MultiBulkReply
        then:
        reply instanceof AsyncReply
        asyncResult instanceof MultiBulkReply
        (mbCs3.replies[0] as BulkReply).raw == 'b'.bytes
        vrCs3.replies.length == 2
        (vrCs3.replies[0] as BulkReply).raw == 'm'.bytes
        (vrCs3.replies[1] as BulkReply).raw == '5.0'.bytes

        when: 'cross-slot - wrong type'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvBad = Mock.prepareCompressedValueList(1)[0]
        cvBad.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvBad.compressedData = new byte[0]
        inMemoryGetSet.put(slot, 'a', 0, cvBad)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        asyncResult = (reply as AsyncReply).settablePromise.getResult()
        then:
        reply instanceof AsyncReply
        asyncResult == ErrorReply.WRONG_TYPE
    }
}
