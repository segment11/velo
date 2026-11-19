package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.TypeMismatchException
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.persist.OneSlot
import io.velo.reply.*
import io.velo.type.RedisHashKeys
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class ZGroupPart2Test extends Specification {
    final short slot = 0
    def _ZGroup = new ZGroup(null, null, null)

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
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == 'member100'
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == '100'

        when:
        reply = zGroup.execute('zrandmember a')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() == 'member100'

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
        // count 0 replies an empty array
        reply = zGroup.execute('zrandmember a 0')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrandmember a 0 withscores')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = zGroup.execute('zrandmember a a withscores')
        then:
        reply == ErrorReply.INVALID_INTEGER

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
        // a too long destination key is rejected
        reply = zGroup.execute('zrangestore >key a 0 -1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = zGroup.execute('zrangestore dst a 0 -1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

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
        reply == new ErrorReply('min or max is not a float')

        when:
        reply = zGroup.execute('zrange a [1.a [1.2 byscore byscore limit 0 0 byscore')
        then:
        reply == new ErrorReply('min or max is not a float')

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
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = zGroup.execute('zrange a (member1 member4 bylex rev limit 0 a bylex')
        then:
        reply == ErrorReply.INVALID_INTEGER

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
        def members1 = (reply as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members1 == ['e', 'd', 'c', 'b', 'a']

        when: 'ZRANGE + - REV BYLEX returns all members reversed'
        def reply2 = zGroup.execute('zrange a + - rev bylex')
        then:
        reply2 instanceof MultiBulkReply
        def members2 = (reply2 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members2 == ['e', 'd', 'c', 'b', 'a']

        when: 'ZREVRANGEBYLEX [d [b returns d,c,b'
        def reply3 = zGroup.execute('zrevrangebylex a [d [b')
        then:
        reply3 instanceof MultiBulkReply
        def members3 = (reply3 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members3 == ['d', 'c', 'b']

        when: 'ZRANGE [d [b REV BYLEX returns d,c,b'
        def reply4 = zGroup.execute('zrange a [d [b rev bylex')
        then:
        reply4 instanceof MultiBulkReply
        def members4 = (reply4 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members4 == ['d', 'c', 'b']

        when: 'ZRANGE [b [d REV BYLEX returns empty (min<max in REV)'
        def reply5 = zGroup.execute('zrange a [b [d rev bylex')
        then:
        reply5 == MultiBulkReply.EMPTY

        when: 'ZREVRANGEBYLEX + (c returns e,d (exclusive c)'
        def reply6 = zGroup.execute('zrevrangebylex a + (c')
        then:
        reply6 instanceof MultiBulkReply
        def members6 = (reply6 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members6 == ['e', 'd']

        when: 'ZREVRANGEBYLEX [b + returns empty (b < +, wrong direction)'
        def reply7 = zGroup.execute('zrevrangebylex a [b +')
        then:
        reply7 == MultiBulkReply.EMPTY

        when: 'ZREVRANGEBYLEX + [b returns e,d,c,b (+ > b, correct direction)'
        def reply8 = zGroup.execute('zrevrangebylex a + [b')
        then:
        reply8 instanceof MultiBulkReply
        def members8 = (reply8 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
        members8 == ['e', 'd', 'c', 'b']

        when: 'ZREVRANGEBYLEX [d - returns d,c,b,a (from d down to -)'
        def reply9 = zGroup.execute('zrevrangebylex a [d -')
        then:
        reply9 instanceof MultiBulkReply
        def members9 = (reply9 as MultiBulkReply).replies.collect { (it as BulkReply).asString() }
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
        def reply = zGroup.execute('zrank a member0')
        then:
        reply == NilReply.INSTANCE

        when:
        // a missing member answers a null array when the score is requested too
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply == MultiBulkReply.NULL

        when:
        // an unknown 4th argument is a syntax error
        reply = zGroup.execute('zrank a member0 withscore_')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        var rz = new RedisZSet()
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrank a member0')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply == MultiBulkReply.NULL

        when:
        rz.add(0.1, 'member0')
        rz.add(0.2, 'withscore')
        cv.compressedData = rz.encode()
        inMemoryGetSet.put('a', cv)
        reply = zGroup.execute('zrank a member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = zGroup.execute('zrevrank a member0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        // a member named "withscore" is a plain member, the option is positional
        reply = zGroup.execute('zrank a withscore')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = zGroup.execute('zrank a member0 withscore')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof IntegerReply
        ((IntegerReply) (reply as MultiBulkReply).replies[0]).integer == 0
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((reply as MultiBulkReply).replies[1] as BulkReply).asString() == '0.1'

        when:
        reply = zGroup.execute('zrank a member1')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = zGroup.execute('zrank a member1 withscore')
        then:
        reply == MultiBulkReply.NULL

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
        reply == new ErrorReply('min or max is not a float')

        when:
        data4[2] = '[1'.bytes
        data4[3] = '[a'.bytes
        reply = zGroup.zremrangebyscore(true, false, false)
        then:
        reply == new ErrorReply('min or max is not a float')

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
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = '1'.bytes
        data4[3] = 'a'.bytes
        reply = zGroup.zremrangebyscore(false, false, true)
        then:
        reply == ErrorReply.INVALID_INTEGER

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
        // the - and + sentinels select all members
        data4[2] = '-'.bytes
        data4[3] = '+'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

        when:
        data4[2] = '-'.bytes
        data4[3] = '[member4'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 5

        when:
        // the + min sentinel selects nothing
        data4[2] = '+'.bytes
        data4[3] = '+'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        // the - max sentinel selects nothing
        data4[2] = '[member4'.bytes
        data4[3] = '-'.bytes
        // reset 10 members
        inMemoryGetSet.put('a', cv)
        reply = zGroup.zremrangebyscore(false, true, false)
        then:
        reply == IntegerReply.REPLY_0

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
        (reply as BulkReply).asString() == '0.1'

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

    def 'test zmpop'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        and: 'the redis shape nests one array per member/score pair'
        def memberPairs = { pairReply ->
            def arr = pairReply as MultiBulkReply
            arr.replies.collect { pair ->
                def onePair = pair as MultiBulkReply
                onePair.replies.length == 2
                [(onePair.replies[0] as BulkReply).asString(), (onePair.replies[1] as BulkReply).asString()]
            }
        }

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
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'numkeys zero'
        reply = zGroup.execute('zmpop 0 myzset MIN')
        then:
        reply == new ErrorReply('numkeys should be greater than 0')

        when: 'wrong direction'
        reply = zGroup.execute('zmpop 1 myzset UP')
        then:
        reply == ErrorReply.SYNTAX

        when: 'count not integer'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT abc')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'count zero rejected'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT 0')
        then:
        reply == new ErrorReply('count should be greater than 0')

        when: 'count negative'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT -1')
        then:
        reply == new ErrorReply('count should be greater than 0')

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
        (mb.replies[0] as BulkReply).asString() == 'myzset'
        memberPairs(mb.replies[1]) == [['one', '1']]

        when: 'single key, pop MAX'
        reply = zGroup.execute('zmpop 1 myzset MAX')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        memberPairs(mb.replies[1]) == [['three', '3']]

        when: 'single key, COUNT 2 (only 1 element left)'
        reply = zGroup.execute('zmpop 1 myzset MIN COUNT 2')
        mb = reply as MultiBulkReply
        then:
        reply instanceof MultiBulkReply
        memberPairs(mb.replies[1]) == [['two', '2']]

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
        then:
        reply instanceof MultiBulkReply
        (mb.replies[0] as BulkReply).asString() == 'b'
        memberPairs(mb.replies[1]) == [['x', '10'], ['y', '20']]

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
        (mbSlot.replies[0] as BulkReply).asString() == 'b'
        memberPairs(mbSlot.replies[1]) == [['seven', '7']]

        when: 'cross-slot - all keys missing'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        def eventloopThread = Thread.start {
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
        asyncResult = reply
        then:
        reply instanceof MultiBulkReply
        asyncResult instanceof MultiBulkReply
        def mbCs = asyncResult as MultiBulkReply
        mbCs.replies.length == 2
        (mbCs.replies[0] as BulkReply).asString() == 'a'
        memberPairs(mbCs.replies[1]) == [['one', '1']]

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
        then:
        reply instanceof AsyncReply
        asyncResult instanceof MultiBulkReply
        mbCs2.replies.length == 2
        (mbCs2.replies[0] as BulkReply).asString() == 'b'
        memberPairs(mbCs2.replies[1]) == [['z', '30'], ['y', '20']]

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
        then:
        reply instanceof AsyncReply
        asyncResult instanceof MultiBulkReply
        (mbCs3.replies[0] as BulkReply).asString() == 'b'
        memberPairs(mbCs3.replies[1]) == [['m', '5']]

        when: 'cross-slot - wrong type'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def cvBad = Mock.prepareCompressedValueList(1)[0]
        cvBad.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvBad.compressedData = new byte[0]
        inMemoryGetSet.put(slot, 'a', 0, cvBad)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        asyncResult = reply
        then:
        reply == ErrorReply.WRONG_TYPE
        asyncResult == ErrorReply.WRONG_TYPE
        when: 'an empty first key still reports a wrong-type secondary key'
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.put(slot, 'b', 0, cvBad)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        then:
        (reply as AsyncReply).settablePromise.getResult() == ErrorReply.WRONG_TYPE

        when: 'empty secondary zsets are skipped after the first key was checked locally'
        inMemoryGetSet.put(slot, 'b', 0, cvEmpty)
        reply = zGroup.execute('zmpop 2 a b MIN')
        eventloopCurrent.run()
        then:
        (reply as AsyncReply).settablePromise.getResult() == NilReply.INSTANCE

        when: 'a decisive first key never reads a failing foreign slot'
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())
        localPersist.addOneSlot((short) 1, eventloop)
        def localReads = new AtomicInteger()
        def foreignReads = new AtomicInteger()
        def guardedStore = new InMemoryGetSet() {
            @Override
            OneSlot.BufOrCompressedValue getBuf(short readSlot, String key, int bucketIndex, long keyHash) {
                if (readSlot == 1) {
                    foreignReads.incrementAndGet()
                    throw new IllegalStateException('unused foreign slot failed')
                }
                localReads.incrementAndGet()
                return super.getBuf(readSlot, key, bucketIndex, keyHash)
            }
        }
        def guardedGroup = new ZGroup(null, null, null)
        guardedGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 2, (short) 2))
        guardedGroup.byPassGetSet = guardedStore
        guardedGroup.crossRequestWorker = true
        def firstKey = (1..100).collect { 'local-zmpop-' + it }.find { BaseCommand.slot(it, 2).slot() == 0 }
        def foreignKey = (1..100).collect { 'remote-zmpop-' + it }.find { BaseCommand.slot(it, 2).slot() == 1 }
        def localCv = Mock.prepareCompressedValueList(1)[0]
        localCv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def localZSet = new RedisZSet()
        localZSet.add(1.0, 'one')
        localZSet.add(2.0, 'two')
        localZSet.add(3.0, 'three')
        localCv.compressedData = localZSet.encode()
        guardedStore.put((short) 0, firstKey, 0, localCv)
        reply = guardedGroup.execute("zmpop 2 $firstKey $foreignKey MIN COUNT 2")
        eventloopCurrent.run()
        then:
        reply instanceof MultiBulkReply
        ((reply as MultiBulkReply).replies[0] as BulkReply).asString() == firstKey
        memberPairs((reply as MultiBulkReply).replies[1]) == [['one', '1'], ['two', '2']]
        localReads.get() == 1
        foreignReads.get() == 0
        ZGroup.getRedisZSet(BaseCommand.slot(firstKey, 2), guardedGroup).size() == 1

        when: 'the last local member is popped with MAX and the key is deleted'
        localReads.set(0)
        reply = guardedGroup.execute("zmpop 2 $firstKey $foreignKey MAX")
        then:
        reply instanceof MultiBulkReply
        memberPairs((reply as MultiBulkReply).replies[1]) == [['three', '3']]
        localReads.get() == 1
        foreignReads.get() == 0
        guardedGroup.getCv(BaseCommand.slot(firstKey, 2)) == null

        when: 'a wrong-type first key also skips the foreign slot'
        guardedStore.put((short) 0, firstKey, 0, cvBad)
        reply = guardedGroup.execute("zmpop 2 $firstKey $foreignKey MIN")
        then:
        reply == ErrorReply.WRONG_TYPE
        foreignReads.get() == 0

        cleanup:
        eventloop?.breakEventloop()
        eventloopThread?.join(5000)
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
        (membersReply.replies[0] as BulkReply).asString() == 'x'
        (membersReply.replies[1] as BulkReply).asString() == '1'
        (membersReply.replies[2] as BulkReply).asString() == 'y'
        (membersReply.replies[3] as BulkReply).asString() == '2'
        (membersReply.replies[4] as BulkReply).asString() == 'z'
        (membersReply.replies[5] as BulkReply).asString() == '3'

        when: // COUNT 2 (partial page)
        reply = zGroup.execute('zscan a 0 count 2')
        then:
        reply instanceof MultiBulkReply
        def scanReply2 = reply as MultiBulkReply
        scanReply2.replies.length == 2
        // next cursor should not be 0 (not at end yet)
        def nextCursor2 = (scanReply2.replies[0] as BulkReply).asString()
        nextCursor2 != '0'
        def membersReply2 = scanReply2.replies[1] as MultiBulkReply
        // 2 members with scores = 4 elements
        membersReply2.replies.length == 4
        (membersReply2.replies[0] as BulkReply).asString() == 'x'
        (membersReply2.replies[1] as BulkReply).asString() == '1'
        (membersReply2.replies[2] as BulkReply).asString() == 'y'
        (membersReply2.replies[3] as BulkReply).asString() == '2'

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
            nextCursor = (page.replies[0] as BulkReply).asString()
            def pageMembers = (page.replies[1] as MultiBulkReply).replies
            for (int i = 0; i < pageMembers.length; i += 2) {
                allPairs.add((pageMembers[i] as BulkReply).asString())
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
        ((scanReply3.replies[1] as MultiBulkReply).replies[0] as BulkReply).asString() == 'x'
        ((scanReply3.replies[1] as MultiBulkReply).replies[1] as BulkReply).asString() == '1'

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
        reply == new ErrorReply('invalid cursor')

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
        reply == ErrorReply.INVALID_INTEGER

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

    def 'test getRedisZSetOrSet'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        and:
        def slotWithKeyHash = BaseCommand.slot('a', (short) 1)

        when: 'the key does not exist'
        def rzMissing = ZGroup.getRedisZSetOrSet(slotWithKeyHash, zGroup)
        then:
        rzMissing == null

        when: 'the key holds a sorted set'
        def cvZSet = Mock.prepareCompressedValueList(1)[0]
        cvZSet.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rzSource = new RedisZSet()
        rzSource.add(1.5, 'm1')
        rzSource.add(2.5, 'm2')
        cvZSet.compressedData = rzSource.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvZSet)
        def rzFromZSet = ZGroup.getRedisZSetOrSet(slotWithKeyHash, zGroup)
        then:
        rzFromZSet != null
        rzFromZSet.size() == 2
        rzFromZSet.contains('m1')
        rzFromZSet.contains('m2')
        rzFromZSet.get('m1').score() == 1.5d

        when: 'the key holds a set, every member gets the score 1.0 like redis'
        def cvSet = Mock.prepareCompressedValueList(1)[0]
        cvSet.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        rhk.add('m1')
        rhk.add('m2')
        cvSet.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvSet)
        def rzFromSet = ZGroup.getRedisZSetOrSet(slotWithKeyHash, zGroup)
        then:
        rzFromSet != null
        rzFromSet.size() == 2
        rzFromSet.contains('m1')
        rzFromSet.contains('m2')
        rzFromSet.get('m1').score() == 1.0d
        rzFromSet.get('m2').score() == 1.0d

        when: 'the key holds another type'
        def cvString = new CompressedValue()
        cvString.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvString.compressedData = 'hello'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cvString)
        def rzWrongType = ZGroup.getRedisZSetOrSet(slotWithKeyHash, zGroup)
        then:
        def ex = thrown(TypeMismatchException)
        ex.message.contains('Expect sp type array=[-8192, -4096]')
    }
}
