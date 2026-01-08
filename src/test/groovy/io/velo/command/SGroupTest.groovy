package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.*
import io.velo.acl.AclUsers
import io.velo.acl.RPubSub
import io.velo.mock.InMemoryGetSet
import io.velo.persist.*
import io.velo.repl.LeaderSelector
import io.velo.reply.*
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class SGroupTest extends Specification {
    final short slot = 0
    def _SGroup = new SGroup(null, null, null)

    def singleKeyCmdList1 = '''
set
setbit
setex
setnx
setrange
strlen
substr
sadd
scard
sismember
smembers
smismember
sort
sort_ro
spop
srandmember
srem
'''.readLines().collect { it.trim() }.findAll { it }

    def multiKeyCmdList2 = '''
sdiff
sdiffstore
sinter
sinterstore
sunion
sunionstore
'''.readLines().collect { it.trim() }.findAll { it }

    def 'test parse slot'() {
        given:
        def data4 = new byte[4][]
        def data1 = new byte[1][]
        int slotNumber = 128

        and:
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        when:
        LocalPersist.instance.addOneSlotForTest2(slot)
        def sSintercardList = _SGroup.parseSlots('sintercard', data4, slotNumber)
        def sSmoveList = _SGroup.parseSlots('smove', data4, slotNumber)
        def sScanList = _SGroup.parseSlots('scan', data4, slotNumber)
        def sList = _SGroup.parseSlots('sxxx', data4, slotNumber)
        then:
        sSintercardList.size() == 2
        sSmoveList.size() == 2
        sScanList.size() == 0
        sList.size() == 0

        when:
        def sDiffList = _SGroup.parseSlots('sdiff', data1, slotNumber)
        sSintercardList = _SGroup.parseSlots('sintercard', data1, slotNumber)
        sSmoveList = _SGroup.parseSlots('smove', data1, slotNumber)
        then:
        sDiffList.size() == 0
        sSintercardList.size() == 0
        sSmoveList.size() == 0

        when:
        def sListList1 = singleKeyCmdList1.collect {
            _SGroup.parseSlots(it, data4, slotNumber)
        }
        then:
        sListList1.size() == 17
        sListList1.every { it.size() == 1 }

        when:
        def sListList11 = singleKeyCmdList1.collect {
            _SGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList11.size() == 17
        sListList11.every { it.size() == 0 }

        when:
        def sListList2 = multiKeyCmdList2.collect {
            _SGroup.parseSlots(it, data4, slotNumber)
        }
        then:
        sListList2.size() == 6
        sListList2.every { it.size() > 1 }

        when:
        def sListList22 = multiKeyCmdList2.collect {
            _SGroup.parseSlots(it, data1, slotNumber)
        }
        then:
        sListList22.size() == 6
        sListList22.every { it.size() == 0 }
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def sGroup = new SGroup('set', data1, null)
        sGroup.from(BaseCommand.mockAGroup())

        def allCmdList = singleKeyCmdList1 + multiKeyCmdList2 + ['sintercard', 'smove', 'scan']

        when:
        sGroup.data = data1
        def sAllList = allCmdList.collect {
            sGroup.cmd = it
            sGroup.handle()
        }
        then:
        sAllList.every {
            it == ErrorReply.FORMAT
        }

        when:
        sGroup.cmd = 'sentinel'
        def reply = sGroup.handle()
        then:
        reply == ErrorReply.NOT_SUPPORT

        when:
        sGroup.cmd = 'save'
        def data2 = new byte[2][]
        sGroup.data = data2
        reply = sGroup.handle()
        then:
        reply == OKReply.INSTANCE

        when:
        sGroup.data = data1
        sGroup.cmd = 'select'
        reply = sGroup.handle()
        then:
        reply == ErrorReply.NOT_SUPPORT

        when:
        sGroup.cmd = 'subscribe'
        reply = sGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        sGroup.cmd = 'slaveof'
        reply = sGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        sGroup.cmd = 'zzz'
        reply = sGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test save'() {
        given:
        def data1 = new byte[1][]
        def sGroup = new SGroup('save', data1, null)
        sGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = sGroup.execute('save')
        then:
        reply == OKReply.INSTANCE
    }

    def 'test scan'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def socket = SocketInspectorTest.mockTcpSocket()
        def veloUserData = SocketInspector.createUserDataIfNotSet(socket)

        def sGroup = new SGroup('scan', null, socket)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        and:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        ConfForSlot.global.confBucket.bucketsPerSlot = 4096
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def scanCursor = new ScanCursor((short) 0, 0, (short) 0, (short) 0, (byte) 0)
        def reply = sGroup.execute('scan . match key:* count 10 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        reply = sGroup.execute('scan . match key:* count 10 type hash') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply

        when:
        reply = sGroup.execute('scan . match key:* count 10 type list') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply

        when:
        reply = sGroup.execute('scan . match key:* count 10 type set') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply

        when:
        reply = sGroup.execute('scan . match key:* count 10 type zset') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply

        when:
        reply = sGroup.execute('scan . match key:* count 10 type xxx') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof ErrorReply

        when:
        def shortValueList = Mock.prepareShortValueList(10, 0)
        def keyLoader = oneSlot.keyLoader
        keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        reply = sGroup.execute('scan . match key:* count 10 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 10
        ScanCursor.fromLong(veloUserData.lastScanAssignCursor).keyBucketsSkipCount() == 10

        when:
        // reset
        veloUserData.lastScanAssignCursor = 0L
        // count 5
        reply = sGroup.execute('scan . match key:* count 5 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 5
        ScanCursor.fromLong(veloUserData.lastScanAssignCursor).keyBucketsSkipCount() == 5

        when:
        // add wal key values
        def shortValueList2 = Mock.prepareShortValueList(20, 0)[10..-1]
        def wal = oneSlot.getWalByGroupIndex(0)
        for (shortV in shortValueList2) {
            wal.put(true, shortV.key(), shortV)
        }
        // reset
        veloUserData.lastScanAssignCursor = 0L
        reply = sGroup.execute('scan . match key:* count 5 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 5
        !ScanCursor.fromLong(veloUserData.lastScanAssignCursor).walIterateEnd
        ScanCursor.fromLong(veloUserData.lastScanAssignCursor).walSkipCount() == 5

        when:
        veloUserData.lastScanAssignCursor = 0L
        // wal 10 + key buckets 5
        reply = sGroup.execute('scan . match key:* count 15 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 15
        ScanCursor.fromLong(veloUserData.lastScanAssignCursor).walIterateEnd
        ScanCursor.fromLong(veloUserData.lastScanAssignCursor).keyBucketsSkipCount() == 5

        when:
        veloUserData.lastScanAssignCursor = 0L
        // key buckets clear, only left wal key values
        keyLoader.flush()
        reply = sGroup.execute('scan . match key:* count 15 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 10

        when:
        veloUserData.lastScanAssignCursor = 0L
        // wal not done
        ConfForSlot.global.confWal.onceScanMaxLoopCount = 1
        reply = sGroup.execute('scan . match key:* count 15 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 10

        when:
        ConfForSlot.global.confWal.onceScanMaxLoopCount = 1024
        veloUserData.lastScanAssignCursor = 0L
        // wal key values clear
        wal.clear()
        reply = sGroup.execute('scan . match key:* count 15 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[1] == MultiBulkReply.EMPTY

        when:
        // scan from key buckets
        def scanCursorFromKeyBuckets = new ScanCursor((short) 0, 0, ScanCursor.ONE_WAL_SKIP_COUNT_ITERATE_END, (short) 0, (byte) 0)
        veloUserData.lastScanAssignCursor = scanCursorFromKeyBuckets.toLong()
        reply = sGroup.execute('scan . match key:* count 10 type string') { data ->
            data[1] = scanCursorFromKeyBuckets.toLong().toString().bytes
        }
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[1] == MultiBulkReply.EMPTY

        when:
        // count, invalid integer
        reply = sGroup.execute('scan . match key:* count a type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('scan . match key:* count -1 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('scan . match key:* xxx 10 type string') { data ->
            data[1] = scanCursor.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.SYNTAX

        when:
        veloUserData.lastScanAssignCursor = 0L
        reply = sGroup.execute('scan 1 match key:* xxx 10 type string')
        then:
        reply instanceof ErrorReply

        when:
        reply = sGroup.execute('scan a match key:* xxx 10 type string')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        def scanCursor1 = new ScanCursor((short) 0, 0, (short) 0, (short) 0, (byte) 0)
        reply = sGroup.execute('scan . match key:* xxx 10 type') { data ->
            data[1] = scanCursor1.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('scan . match key:* xxx 10 count') { data ->
            data[1] = scanCursor1.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('scan . match match xxx 10 match') { data ->
            data[1] = scanCursor1.toLong().toString().bytes
        }
        then:
        reply == ErrorReply.SYNTAX

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test sentinel'() {
        given:
        def sGroup = new SGroup(null, null, null)

        when:
        def reply = sGroup.execute('sentinel zzz')
        then:
        reply == ErrorReply.NOT_SUPPORT
    }

    def 'test set'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('set a value')
        def slotWithKeyHash = sGroup.slotWithKeyHashListParsed[0]
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().compressedData == 'value'.bytes

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.execute('set a value nx')
        then:
        reply == OKReply.INSTANCE

        when:
        // set nx again
        reply = sGroup.execute('set a value nx')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = sGroup.execute('set a value xx')
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.execute('set a value xx')
        then:
        reply == NilReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.execute('set a value keepttl')
        then:
        reply == OKReply.INSTANCE

        when:
        // keepttl set again
        reply = sGroup.execute('set a value keepttl')
        then:
        reply == OKReply.INSTANCE

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.execute('set a value get')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = sGroup.execute('set a value get')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == 'value'.bytes

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('set a value get')
        then:
        reply == ErrorReply.NOT_STRING

        when:
        reply = sGroup.execute('set a value ex 10')
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().expireAt > System.currentTimeMillis() + 9000

        when:
        reply = sGroup.execute('set a value px 10000')
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().expireAt > System.currentTimeMillis() + 9000

        when:
        reply = sGroup.execute('set a value exat ' + ((System.currentTimeMillis() / 1000).intValue() + 10))
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().expireAt > System.currentTimeMillis() + 9000

        when:
        reply = sGroup.execute('set a value pxat ' + (System.currentTimeMillis() + 10000))
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().expireAt > System.currentTimeMillis() + 9000

        when:
        reply = sGroup.execute('set a value pxat -1')
        then:
        reply == OKReply.INSTANCE
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().expireAt == CompressedValue.NO_EXPIRE

        when:
        reply = sGroup.execute('set a value pxat a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // skip syntax check
        reply = sGroup.execute('set a value zz')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = sGroup.execute('set a value ex')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('set >key value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = sGroup.execute('set a >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test setbit'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('setbit', null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('setbit a 0 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.execute('setbit a 0 1')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = sGroup.execute('setbit a 0 0')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = sGroup.execute('setbit a 0 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'foobar'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('setbit a 1 0')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = sGroup.execute('setbit a 1 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.execute('setbit a 0 2')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('setbit a 0 10')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('setbit a ' + 1024 * 1024 + ' 1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('setbit a -1 1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('setbit a _ 1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('setbit >key 0 10')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test setex'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '10'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('setex', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = _SGroup.parseSlots('setex', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.handle()
        then:
        reply == OKReply.INSTANCE
    }

    def 'test setnx'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('setnx a value')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = sGroup.execute('setnx a value')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.execute('setnx >key value')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test setrange'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('setrange a 1 value')

        def slotWithKeyHash = sGroup.slotWithKeyHashListParsed[0]

        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 6
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().compressedData[1..-1] == 'value'.bytes

        when:
        reply = sGroup.execute('setrange a 1 value')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 6

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = '1234567890'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('setrange a 1 value')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().compressedData == '1value7890'.bytes

        when:
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('setrange a 0 value')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10
        inMemoryGetSet.getBuf(slot, 'a', slotWithKeyHash.bucketIndex(), slotWithKeyHash.keyHash())
                .cv().compressedData == 'value67890'.bytes

        when:
        reply = sGroup.execute('setrange a -1 value')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('setrange a a value')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('setrange >key 1 value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = sGroup.execute('setrange a 1 >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test strlen'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('strlen a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = '1234567890'.bytes

        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('strlen a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 10

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        cv.compressedData = new byte[4]
        ByteBuffer.wrap(cv.compressedData).putInt(123456)
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('strlen a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 6
    }

    def 'test select'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = sGroup.execute('select 1')
        then:
        reply == ErrorReply.NOT_SUPPORT
//        reply == OKReply.INSTANCE
    }

    def 'test sadd'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('sadd a 1 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        rhk.add('1')
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('sadd a 1 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        rhk.remove('1')
        RedisHashKeys.HASH_MAX_SIZE.times {
            rhk.add(it.toString())
        }
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('sadd a -1 -2')
        then:
        reply == ErrorReply.SET_SIZE_TO_LONG

        when:
        reply = sGroup.execute('sadd >key 1 2')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = sGroup.execute('sadd a >key 2')
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG
    }

    def 'test scard'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('scard a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        rhk.add('1')
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('scard a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = sGroup.execute('scard >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test sdiff'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sdiff', data3, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = _SGroup.parseSlots('sdiff', data3, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.sdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        boolean wrongTypeException = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        try {
            sGroup.sdiff(false, false)
        } catch (TypeMismatchException e) {
            println e.message
            wrongTypeException = true
        }
        then:
        wrongTypeException

        when:
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        // empty set
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()

        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiff(false, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        rhkA.add('1')
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.sdiff(false, true)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(false, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == '2'.bytes

        when:
        reply = sGroup.sdiff(true, false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == '1'.bytes

        when:
        rhkB.remove('1')
        rhkB.add('3')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiff(true, false)
        then:
        reply == MultiBulkReply.EMPTY

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
        sGroup.crossRequestWorker = true
        reply = sGroup.sdiff(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == MultiBulkReply.EMPTY
        }.result

        when:
        reply = sGroup.sdiff(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof MultiBulkReply) && (result as MultiBulkReply).replies.length == 3
        }.result

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiff(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sdiffstore'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'dst'.bytes
        data4[2] = 'a'.bytes
        data4[3] = 'b'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sdiffstore', data4, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        sGroup.slotWithKeyHashListParsed = _SGroup.parseSlots('sdiffstore', data4, sGroup.slotNumber)
        inMemoryGetSet.remove(slot, 'dst')
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.sdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        boolean wrongTypeException = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        try {
            reply = sGroup.sdiffstore(false, false)
        } catch (TypeMismatchException e) {
            println e.message
            wrongTypeException = true
        }
        then:
        wrongTypeException

        when:
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        // empty set
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.sdiffstore(false, true)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(false, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        RedisHashKeys.decode(inMemoryGetSet.getBuf(slot, 'dst', 0, 0L).cv().compressedData).contains('2')

        when:
        reply = sGroup.sdiffstore(true, false)
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        RedisHashKeys.decode(inMemoryGetSet.getBuf(slot, 'dst', 0, 0L).cv().compressedData).contains('1')

        when:
        rhkB.remove('1')
        rhkB.add('3')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.sdiffstore(true, false)
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
        sGroup.crossRequestWorker = true
        reply = sGroup.sdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && (result as IntegerReply).integer == 3
        }.result

        when:
        rhkA.remove('1')
        rhkA.remove('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.sdiffstore(false, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(true, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.sdiffstore(false, true)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && (result as IntegerReply).integer == 3
        }.result

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.sdiffstore(false, false)
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            reply == IntegerReply.REPLY_0
        }.result

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'dst'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = sGroup.sdiffstore(false, false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sintercard'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.execute('sintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('sintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('sintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvB = cvList[1]
        cvB.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkB = new RedisHashKeys()
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.execute('sintercard 2 a b limit 0')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkB.add('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.execute('sintercard 2 a b limit 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = sGroup.execute('sintercard 2 a b limit 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = sGroup.execute('sintercard 2 a b limit 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

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
        sGroup.crossRequestWorker = true
        reply = sGroup.execute('sintercard 2 a b limit 2')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && (result as IntegerReply).integer == 1
        }.result

        when:
        reply = sGroup.execute('sintercard 2 a b limit 1')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && (result as IntegerReply).integer == 1
        }.result

        when:
        reply = sGroup.execute('sintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            (result instanceof IntegerReply) && (result as IntegerReply).integer == 1
        }.result

        when:
        rhkB.remove('1')
        cvB.compressedData = rhkB.encode()
        inMemoryGetSet.put(slot, 'b', 0, cvB)
        reply = sGroup.execute('sintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        inMemoryGetSet.remove(slot, 'b')
        reply = sGroup.execute('sintercard 2 a b limit 0')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_0
        }.result

        when:
        reply = sGroup.execute('sintercard 2 a >key limit 0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = sGroup.execute('sintercard a a b limit 0')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('sintercard 1 a b limit 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = sGroup.execute('sintercard 2 a b limit a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('sintercard 2 a b limit_x 0')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sintercard 2 a b limit')
        then:
        reply == ErrorReply.SYNTAX

        when:
        inMemoryGetSet.remove(slot, 'a')
        reply = sGroup.execute('sintercard 3 a b c')
        then:
        reply == IntegerReply.REPLY_0

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test sismember'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('sismember a 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('2')
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('sismember a 1')
        then:
        reply == IntegerReply.REPLY_1

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('sismember a 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = sGroup.execute('sismember a >key')
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        reply = sGroup.execute('sismember >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test smembers'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('smembers a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smembers a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smembers a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.execute('smembers >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test smismember'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('smismember a 1 2')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smismember a 1 2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_0

        when:
        rhkA.remove('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smismember a 1 2')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.execute('smismember a >key 2')
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        reply = sGroup.execute('smismember >key 1 2')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test sort'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup('sort', null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('sort a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.execute('sort a by weight_* desc alpha limit 0 1 get #')
        then:
        // sort by pattern not support yet
        reply instanceof ErrorReply

        when:
        reply = sGroup.execute('sort a by')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sort a limit')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sort a get')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sort a store')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sort a xxx')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = sGroup.execute('sort a limit a 10')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('sort a limit 0 a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('sort a limit -1 10')
        then:
        reply instanceof ErrorReply

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhk = new RedisHashKeys()
        10.times {
            rhk.add(String.valueOf(it))
        }
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('sort a limit 1 10')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 9

        when:
        reply = sGroup.execute('sort a limit 1 5 alpha')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        def rl = new RedisList()
        10.times {
            rl.addLast(String.valueOf(it).bytes)
        }
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('sort a limit 1 10')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 9

        when:
        reply = sGroup.execute('sort a limit 1 5 alpha')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        def rz = new RedisZSet()
        10.times {
            rz.add(it, String.valueOf(it))
        }
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply = sGroup.execute('sort a limit 1 10')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 9

        when:
        reply = sGroup.execute('sort a limit 1 5 alpha')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        // test store
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        inMemoryGetSet.remove(slot, 'a')
        def reply2 = sGroup.execute('sort a store dst')
        then:
        reply2 == IntegerReply.REPLY_0

        when:
        reply2 = sGroup.execute('sort_ro a store dst')
        then:
        // sort_ro not support store
        reply2 instanceof ErrorReply

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        cv.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply2 = sGroup.execute('sort a store dst')
        then:
        reply2 instanceof AsyncReply
        ((AsyncReply) reply2).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 10
        }.result

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cv.compressedData = rl.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply2 = sGroup.execute('sort a store dst')
        then:
        reply2 instanceof AsyncReply
        ((AsyncReply) reply2).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 10
        }.result

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        cv.compressedData = rz.encode()
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply2 = sGroup.execute('sort a store dst')
        then:
        reply2 instanceof AsyncReply
        ((AsyncReply) reply2).settablePromise.whenResult { result ->
            result instanceof IntegerReply && (result as IntegerReply).integer == 10
        }.result

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        inMemoryGetSet.put(slot, 'a', 0, cv)
        reply2 = sGroup.execute('sort a store dst')
        then:
        reply2 == ErrorReply.WRONG_TYPE

        when:
        reply = sGroup.execute('sort >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test smove'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        inMemoryGetSet.remove(slot, 'b')
        def reply = sGroup.execute('smove a b 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        rhkA.add('11')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smove a b 1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.remove('11')
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smove a b 1')
        then:
        reply == IntegerReply.REPLY_1

        when:
        rhkA.add('2')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smove a b 1')
        then:
        reply == IntegerReply.REPLY_1

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
        sGroup.crossRequestWorker = true
        rhkA.remove('2')
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        inMemoryGetSet.remove(slot, 'b')
        reply = sGroup.execute('smove a b 1')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('smove a b 1')
        eventloopCurrent.run()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == IntegerReply.REPLY_1
        }.result

        when:
        reply = sGroup.execute('smove a b >key')
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        reply = sGroup.execute('smove >key b 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = sGroup.execute('smove a >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test srandmember'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('srandmember a 1')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.execute('srandmember a')
        then:
        reply == NilReply.INSTANCE

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('srandmember a 1')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = sGroup.execute('srandmember a')
        then:
        reply == NilReply.INSTANCE

        when:
        10.times {
            rhkA.add(it.toString())
        }
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('srandmember a 1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        reply = sGroup.execute('srandmember a')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).asString() as int < 10

        when:
        reply = sGroup.execute('srandmember a 11')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 10

        when:
        reply = sGroup.execute('srandmember a -5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        reply = sGroup.execute('spop a 5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        // pop all
        reply = sGroup.execute('spop a 5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        reply = sGroup.execute('srandmember a a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = sGroup.execute('srandmember >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test srem'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def sGroup = new SGroup(null, null, null)
        sGroup.byPassGetSet = inMemoryGetSet
        sGroup.from(BaseCommand.mockAGroup())

        when:
        inMemoryGetSet.remove(slot, 'a')
        def reply = sGroup.execute('srem a 1 2')
        then:
        reply == IntegerReply.REPLY_0

        when:
        def cvList = Mock.prepareCompressedValueList(1)
        def cvA = cvList[0]
        cvA.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        def rhkA = new RedisHashKeys()
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('srem a 1 2')
        then:
        reply == IntegerReply.REPLY_0

        when:
        rhkA.add('1')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('srem a 1 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        rhkA.add('1')
        rhkA.add('2')
        rhkA.add('3')
        cvA.compressedData = rhkA.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvA)
        reply = sGroup.execute('srem a 1 2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        reply = sGroup.execute('srem a 1 >key')
        then:
        reply == ErrorReply.SET_MEMBER_LENGTH_TO_LONG

        when:
        reply = sGroup.execute('srem >key 1 2')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test subscribe'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'b'.bytes
        data4[3] = 'c'.bytes

        def socket = SocketInspectorTest.mockTcpSocket()

        def sGroup = new SGroup('subscribe', data4, socket)
        sGroup.from(BaseCommand.mockAGroup())

        and:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()

        when:
        LocalPersist.instance.socketInspector = new SocketInspector()
        def reply = sGroup.subscribe(false)
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3 * 3

        when:
        aclUsers.upInsert('default') { u ->
            u.addRPubSub(true, RPubSub.fromLiteral('&special_channel'))
        }
        reply = sGroup.subscribe(false)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        aclUsers.upInsert('default') { u ->
            u.on = false
        }
        reply = sGroup.subscribe(false)
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        def data1 = new byte[1][]
        sGroup.data = data1
        reply = sGroup.subscribe(false)
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test slaveof'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:7380'
        LocalPersist.instance.socketInspector = new SocketInspector()

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def leaderSelector = LeaderSelector.instance
        def testListenAddress = 'localhost:7379'
        leaderSelector.masterAddressLocalMocked = testListenAddress

        and:
        def data3 = new byte[3][]
        data3[1] = 'no'.bytes
        data3[2] = '7379'.bytes

        def sGroup = new SGroup('slaveof', data3, null)
        sGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = sGroup.slaveof()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when:
        data3[1] = 'localhost'.bytes
        reply = sGroup.slaveof()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data3[1] = '127.0.0.1'.bytes
        data3[2] = '-1'.bytes
        reply = sGroup.slaveof()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data3[2] = 'a'.bytes
        reply = sGroup.slaveof()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        data3[2] = '7379'.bytes
        reply = sGroup.slaveof()
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
