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
        sListList1.size() == singleKeyCmdList1.size()
        sListList1.every { it.size() == 1 }
        sListList11.size() == singleKeyCmdList1.size()
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
        def sListList2222 = multiKeyCmdList2.collect {
            _ZGroup.parseSlots(it, data6, slotNumber)
        }
        then:
        // a single key is enough for routing, the extra args are rejected by the command body
        sListList2222.every { it.size() == 1 }

        when:
        data6[1] = '0'.bytes
        def sListListZero = multiKeyCmdList2.collect {
            _ZGroup.parseSlots(it, data6, slotNumber)
        }
        then:
        // zero keys is still an error
        sListListZero.every { it.size() == 0 }

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
        def sListList3333 = multiKeyCmdList3.collect {
            _ZGroup.parseSlots(it, data7, slotNumber)
        }
        then:
        // a single source key is enough for routing, the extra args are rejected by the command body
        sListList3333.every { it.size() == 1 }

        when:
        data7[2] = '0'.bytes
        def sListListZero2 = multiKeyCmdList3.collect {
            _ZGroup.parseSlots(it, data7, slotNumber)
        }
        then:
        // zero keys is still an error
        sListListZero2.every { it.size() == 0 }

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
        zGroup.execute('zzz') == ErrorReply.UNKNOWN_COMMAND('zzz')
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
        reply == ErrorReply.INVALID_FLOAT

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
        // GT only updates an existing member when the new score is greater than the current one
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 5 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // GT updates an existing member when the new score is greater than the current one
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt 20 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 20

        when:
        // LT does not update an existing member when the new score is greater than the current one
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 20 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // LT updates an existing member when the new score is less than the current one
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a lt 5 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 5

        when:
        // CH counts the members whose score changed
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt ch 20 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 20

        when:
        // CH does not count a member whose update was vetoed by GT
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a gt ch 5 member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // INCR applies the increment before the GT comparison
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a incr gt 2 member0')
        then:
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 12

        when:
        // INCR with a negative increment is vetoed by GT
        rz.add(10, 'member0')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zadd a incr gt -5 member0')
        then:
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        reply = zGroup.execute('zadd a incr 0 member0 1 member1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        // INCR on an existing member replies the resulting score
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '10'

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '0'

        when:
        // INCR replies the updated score of the member
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.execute('zadd a incr 5 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '5'
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 5

        when:
        // INCR on an existing member replies the incremented score
        reply = zGroup.execute('zadd a incr 5 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '10'
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // INCR with a zero increment still replies the unchanged score
        reply = zGroup.execute('zadd a incr 0 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '10'

        when:
        // INCR on a missing member with XX is vetoed and replies nil
        reply = zGroup.execute('zadd a xx incr 5 member1')
        then:
        reply == NilReply.INSTANCE

        when:
        // INCR on an existing member with NX is vetoed and replies nil
        reply = zGroup.execute('zadd a nx incr 5 member0')
        then:
        reply == NilReply.INSTANCE
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // NX and XX are mutually exclusive
        reply = zGroup.execute('zadd a nx xx 2 member0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        // GT and LT are mutually exclusive
        reply = zGroup.execute('zadd a gt lt 2 member0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        // NX and GT are mutually exclusive
        reply = zGroup.execute('zadd a nx gt 2 member0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        // NX and LT are mutually exclusive
        reply = zGroup.execute('zadd a nx lt 2 member0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        // INCR with GT is a valid combination, the resulting score is vetoed here
        reply = zGroup.execute('zadd a incr gt -5 member0')
        then:
        reply == NilReply.INSTANCE
        fromMem(inMemoryGetSet, 'a').get('member0').score() == 10

        when:
        // NaN in any case spelling is not a valid score
        reply = zGroup.execute('zadd a NaN member2')
        then:
        reply == ErrorReply.INVALID_FLOAT

        when:
        reply = zGroup.execute('zadd a +NaN member2')
        then:
        reply == ErrorReply.INVALID_FLOAT

        when:
        reply = zGroup.execute('zadd a -NaN member2')
        then:
        reply == ErrorReply.INVALID_FLOAT

        when:
        // the inf spellings are valid scores
        reply = zGroup.execute('zadd a inf member2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = zGroup.execute('zscore a member2')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == 'inf'

        when:
        reply = zGroup.execute('zadd a -INFINITY member3')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = zGroup.execute('zscore a member3')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '-inf'

        when:
        // parseScore accepts every redis inf spelling and rejects NaN
        def scores = [
                ['inf', Double.POSITIVE_INFINITY],
                ['+inf', Double.POSITIVE_INFINITY],
                ['infinity', Double.POSITIVE_INFINITY],
                ['+infinity', Double.POSITIVE_INFINITY],
                ['INF', Double.POSITIVE_INFINITY],
                ['-inf', Double.NEGATIVE_INFINITY],
                ['-infinity', Double.NEGATIVE_INFINITY],
                ['-INFINITY', Double.NEGATIVE_INFINITY],
                ['1.5', 1.5d],
        ]
        then:
        scores.every {
            def parsed = _ZGroup.parseScore(it[0] as String)
            parsed != null && parsed == it[1]
        }
        ['NaN', '+NaN', '-NaN', 'nan', 'a'].every { _ZGroup.parseScore(it) == null }

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
        // the - and + sentinels select all members
        reply = zGroup.execute('zlexcount a - +')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

        when:
        reply = zGroup.execute('zlexcount a - [member4')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 5

        when:
        reply = zGroup.execute('zlexcount a [member4 +')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 6

        when:
        reply = zGroup.execute('zlexcount a + +')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = zGroup.execute('zlexcount a [member4 -')
        then:
        reply == IntegerReply.REPLY_0

        when:
        // sentinel aware lex range order check
        def lexRanges = [
                ['-', '+', true],
                ['-', '-', true],
                ['-', 'member0', true],
                ['+', '+', false],
                ['+', 'member0', false],
                ['member0', '+', true],
                ['member0', '-', false],
                ['member0', 'member1', true],
                ['member0', 'member0', true],
                ['member1', 'member0', false],
        ]
        then:
        lexRanges.every { _ZGroup.isLexRangeBefore(it[0] as String, it[1] as String) == it[2] }

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
        reply == ErrorReply.INVALID_FLOAT

        when:
        reply = zGroup.execute('zcount a 1 a')
        then:
        reply == ErrorReply.INVALID_FLOAT

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'member0'

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'member0'
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == '0.1'

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
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[1] = '1'.bytes
        reply = zGroup.zdiff(false, false)
        then:
        // numkeys=1 is valid, the trailing extra args are not withscores
        reply == ErrorReply.SYNTAX

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
        reply == ErrorReply.INVALID_FLOAT

        when:
        // NaN is not a valid weight
        data7[6] = 'NaN'.bytes
        reply = zGroup.zdiff(true, false)
        then:
        reply == ErrorReply.INVALID_FLOAT

        when:
        // the inf spellings are valid weights
        data7[6] = 'inf'.bytes
        reply = zGroup.zdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result instanceof MultiBulkReply && (result as MultiBulkReply).replies.length == 5
        }.result

        when:
        // a single key is a valid number of keys for the set operations
        def data1 = new byte[3][]
        data1[1] = '1'.bytes
        data1[2] = 'a'.bytes
        zGroup.data = data1
        zGroup.crossRequestWorker = false
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zdiff', data1, zGroup.slotNumber)
        reply = zGroup.zdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10

        when:
        // store form with a single source key, weights applied to the copy
        def dataStore = new byte[6][]
        dataStore[1] = 'dst2'.bytes
        dataStore[2] = '1'.bytes
        dataStore[3] = 'a'.bytes
        dataStore[4] = 'weights'.bytes
        dataStore[5] = '2'.bytes
        zGroup.data = dataStore
        zGroup.slotWithKeyHashListParsed = _ZGroup.parseSlots('zunionstore', dataStore, zGroup.slotNumber)
        reply = zGroup.zdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

        when:
        def resultRz = fromMem(inMemoryGetSet, 'dst2')
        then:
        resultRz.getSet().collect { it.member() } == ['member0', 'member1', 'member2', 'member3', 'member4', 'member5', 'member6', 'member7', 'member8', 'member9']
        resultRz.getSet().collect { it.score() } == [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0]

        when:
        // a single missing source key with weights replies 0
        inMemoryGetSet.remove(slot, 'a')
        reply = zGroup.zdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // a too long destination key is rejected
        reply = zGroup.execute('zunionstore >key 2 a b')
        then:
        reply == ErrorReply.KEY_TOO_LONG

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
        (reply as BulkReply).asString() == '1'

        when:
        reply = zGroup.execute('zincrby a 1 member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == '2'

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
        reply == ErrorReply.INVALID_FLOAT

        when:
        // NaN in any case spelling is not a valid increment
        reply = zGroup.execute('zincrby a NaN member0')
        then:
        reply == ErrorReply.INVALID_FLOAT

        when:
        // the inf spellings are valid increments
        reply = zGroup.execute('zincrby a inf member0')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == 'inf'

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
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zintercard 2 a b limit -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        // a single key is a valid number of keys
        reply = zGroup.execute('zintercard 1 a limit 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 3

        when:
        reply = zGroup.execute('zintercard 1 a b limit 0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = zGroup.execute('zintercard a a b limit 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == '0.1'
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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'member100'
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == '100'

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'member10'
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == '10'

        when:
        // count 0 replies an empty array
        reply = zGroup.execute('zpopmax a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zpopmin a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        // a negative count is still an error
        reply = zGroup.execute('zpopmax a -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zpopmax a a')
        then:
        reply == ErrorReply.INVALID_INTEGER

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
}
