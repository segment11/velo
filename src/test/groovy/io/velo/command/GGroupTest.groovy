package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisGeo
import spock.lang.Specification

import java.time.Duration

class GGroupTest extends Specification {
    def _GGroup = new GGroup(null, null, null)
    final short slot = 0

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sGetList = _GGroup.parseSlots('get', data2, slotNumber)
        def sGetBitList = _GGroup.parseSlots('getbit', data2, slotNumber)
        def sGetDelList = _GGroup.parseSlots('getdel', data2, slotNumber)
        def sGetExList = _GGroup.parseSlots('getex', data2, slotNumber)
        def sGetRangeList = _GGroup.parseSlots('getrange', data2, slotNumber)
        def sGetSetList = _GGroup.parseSlots('getset', data2, slotNumber)
        def sGeoaddList = _GGroup.parseSlots('geoadd', data2, slotNumber)
        def sGeosearchstoreList = _GGroup.parseSlots('geosearchstore', data2, slotNumber)
        def sList = _GGroup.parseSlots('gxxx', data2, slotNumber)
        then:
        sGetList.size() == 1
        sGetBitList.size() == 1
        sGetDelList.size() == 1
        sGetExList.size() == 1
        sGetRangeList.size() == 1
        sGetSetList.size() == 1
        sGeoaddList.size() == 1
        sGeosearchstoreList.size() == 0
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        sGeosearchstoreList = _GGroup.parseSlots('geosearchstore', data4, slotNumber)
        then:
        sGeosearchstoreList.size() == 2

        when:
        def data1 = new byte[1][]
        sGetList = _GGroup.parseSlots('get', data1, slotNumber)
        sGeoaddList = _GGroup.parseSlots('geoadd', data1, slotNumber)
        then:
        sGetList.size() == 0
        sGeoaddList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def gGroup = new GGroup('getbit', data1, null)
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getdel'
        reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getex'
        reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getrange'
        reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'getset'
        reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        gGroup.cmd = 'geoadd'
        reply = gGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        gGroup.data = data2
        then:
        ['georadius', 'georadius_ro', 'georadiusbymember', 'georadiusbymember_ro'].every {
            gGroup.cmd = it
            reply = gGroup.handle()
            reply == ErrorReply.NOT_SUPPORT
        }

        when:
        gGroup.cmd = 'zzz'
        reply = gGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test getbit'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getbit', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = gGroup.execute('getbit a 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'foobar'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.execute('getbit a 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = gGroup.execute('getbit a 1')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = gGroup.execute('getbit a 48')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = gGroup.execute('getbit a ' + 1024 * 1024)
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = gGroup.execute('getbit a -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = gGroup.execute('getbit a _')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        def data3 = new byte[3][]
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        data3[2] = '0'.bytes
        gGroup.data = data3
        reply = gGroup.getbit()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = gGroup.execute('getbit a 0 1')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test getdel'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getdel', data2, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = _GGroup.parseSlots('getdel', data2, gGroup.slotNumber)
        def reply = gGroup.getdel()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.getdel()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cv.compressedData
    }

    def 'test getex'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getex', data2, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        gGroup.slotWithKeyHashListParsed = _GGroup.parseSlots('getex', data2, gGroup.slotNumber)
        def reply = gGroup.getex()
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.getex()
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cv.compressedData

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = gGroup.getex()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'persist'.bytes
        gGroup.data = data3
        cv.expireAt = System.currentTimeMillis() + 1000 * 60
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.getex()
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt == CompressedValue.NO_EXPIRE

        when:
        data3[2] = 'persist_'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'ex'.bytes
        data4[3] = '60'.bytes
        gGroup.data = data4
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt > System.currentTimeMillis()

        when:
        data4[3] = 'a'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data4[3] = '-1'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data4[2] = 'px'.bytes
        data4[3] = '60000'.bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt > System.currentTimeMillis()

        when:
        data4[2] = 'pxat'.bytes
        data4[3] = (System.currentTimeMillis() + 1000 * 60).toString().bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt.toString().bytes == data4[3]

        when:
        data4[2] = 'exat'.bytes
        data4[3] = ((System.currentTimeMillis() / 1000).intValue() + 60).toString().bytes
        reply = gGroup.getex()
        bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, cv.keyHash)
        then:
        bufOrCv.cv().expireAt > System.currentTimeMillis()

        when:
        data4[2] = 'xx'.bytes
        reply = gGroup.getex()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data5 = new byte[5][]
        data5[1] = 'a'.bytes
        data5[2] = 'ex'.bytes
        data5[3] = '60'.bytes
        data5[4] = 'xx'.bytes
        gGroup.data = data5
        reply = gGroup.getex()
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test getrange'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getrange', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('getrange a 0 1')
        then:
        reply == NilReply.INSTANCE

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'abc'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.execute('getrange a 0 1')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'ab'.bytes

        when:
        reply = gGroup.execute('getrange a 0 a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = gGroup.execute('getrange a 2 1')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 0
    }

    def 'test getset'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('getset', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('getset a value')
        then:
        reply == NilReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, 0L).cv().compressedData == 'value'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'abc'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = gGroup.execute('getset a value2')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'abc'.bytes

        when:
        def bufOrCv = inMemoryGetSet.getBuf(slot, 'a'.bytes, 0, 0L)
        then:
        bufOrCv.cv().compressedData == 'value2'.bytes
    }

    def 'test geoadd'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('geoadd', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('geoadd a nx xx ch 1.0 2.0 m0 2.0 3.0 m1')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        reply = gGroup.execute('geoadd a nx 1.0 2.0 m0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = gGroup.execute('geoadd a xx 1.0 2.0 m2')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = gGroup.execute('geoadd a 1.0 2.0 m0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        reply = gGroup.execute('geoadd a ch 10.0 20.0 m0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = gGroup.execute('geoadd a ch 10.0 30.0 m0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = gGroup.execute('geoadd a ch 20.0 30.0 m0')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        reply = gGroup.execute('geoadd a nx 1.a 2.0 m0')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = gGroup.execute('geoadd a nx 1.0 2.0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geoadd a nx xx ch')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geoadd a')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test geodist'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('geodist', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('geodist xxx m0 m1')
        then:
        reply == NilReply.INSTANCE

        when:
        gGroup.execute('geoadd xxx 13.361389 38.115556 m00 15.087269 37.502669 m11')
        reply = gGroup.execute('geodist xxx m0 m1')
        then:
        reply == NilReply.INSTANCE

        when:
        gGroup.execute('geoadd xxx 13.361389 38.115556 m0')
        reply = gGroup.execute('geodist xxx m0 m1')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = gGroup.execute('geodist xxx m1 m0')
        then:
        reply == NilReply.INSTANCE

        when:
        gGroup.execute('geoadd xxx 15.087269 37.502669 m1')
        reply = gGroup.execute('geodist xxx m0 m1')
        then:
        reply instanceof BulkReply

        when:
        reply = gGroup.execute('geodist xxx m0 m1 KM')
        then:
        reply instanceof BulkReply

        when:
        reply = gGroup.execute('geodist xxx m0 m1 M')
        then:
        reply instanceof BulkReply

        when:
        reply = gGroup.execute('geodist xxx m0 m1 MI')
        then:
        reply instanceof BulkReply

        when:
        reply = gGroup.execute('geodist xxx m0 m1 FT')
        then:
        reply instanceof BulkReply

        when:
        reply = gGroup.execute('geodist xxx m0 m1 XX')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geodist xxx m0 m1 XX XX')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test geohash and geopos'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('geohash', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('geohash xxx m0 m1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.every {
            it == NilReply.INSTANCE
        }

        when:
        gGroup.execute('geoadd xxx 13.361389 38.115556 m0 15.087269 37.502669 m1')
        reply = gGroup.execute('geohash xxx m0 m1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.every {
            it instanceof BulkReply
        }

        when:
        reply = gGroup.execute('geohash xxx m0 m1 m2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[2] == NilReply.INSTANCE

        when:
        reply = gGroup.execute('geopos xxx m0 m1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.every {
            it instanceof MultiBulkReply
        }

        when:
        reply = gGroup.execute('geohash xxx')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test geosearch and geosearchstore'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def gGroup = new GGroup('geosearch', null, null)
        gGroup.byPassGetSet = inMemoryGetSet
        gGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = gGroup.execute('geosearch xxx fromlonlat 15 13 frommember mmm bybox 400 400 km asc desc withdist withhash withcoord count 2')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        gGroup.execute('geoadd xxx 13.361389 38.115556 m0 15.087269 37.502669 m1 20 40 out_one 15 37 mmm')
        reply = gGroup.execute('geosearch xxx fromlonlat 15 37 frommember mmm! bybox 400 400 km asc desc withdist withhash withcoord count 2')
        reply = gGroup.execute('geosearch xxx fromlonlat 15 37 frommember mmm bybox 400 400 km asc withdist withhash withcoord count 2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        // only return member
        reply = gGroup.execute('geosearch xxx fromlonlat 15 37 frommember mmm bybox 100 100 asc count 1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        reply = gGroup.execute('geosearchstore yyy xxx fromlonlat 15 37 bybox 400 400 km desc count 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

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
        gGroup.crossRequestWorker = true
        reply = gGroup.execute('geosearchstore yyy xxx fromlonlat 15 37 bybox 400 400 km desc count 2')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.whenResult { result ->
            result instanceof IntegerReply && ((IntegerReply) result).integer == 2
        }.result

        when:
        reply = gGroup.execute('geosearch xxx fromlonlat')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx fromlonlat a b')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = gGroup.execute('geosearch xxx fromlonlat 15 37')
        then:
        // need byradius or bybox
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx frommember')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx byradius')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx byradius a')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = gGroup.execute('geosearch xxx byradius 100 km bybox')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx byradius 100 km bybox a b')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = gGroup.execute('geosearch xxx byradius 100 km bybox 1 1 km')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = gGroup.execute('geosearch xxx count')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearch xxx count -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = gGroup.execute('geosearch xxx count a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = gGroup.execute('geosearch xxx 333')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = gGroup.execute('geosearchstore xxx')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = gGroup.execute('geosearch_store yyy xxx')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def dstRg = new RedisGeo()
        // removed
        def sss = gGroup.slot('yyy'.bytes)
        gGroup.saveRedisGeo(dstRg, 'yyy'.bytes, sss)
        then:
        inMemoryGetSet.getBuf(slot, 'yyy'.bytes, sss.bucketIndex(), sss.keyHash()) == null

        cleanup:
        eventloop.breakEventloop()
    }
}
