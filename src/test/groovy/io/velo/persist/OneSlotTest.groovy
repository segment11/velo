package io.velo.persist

import io.activej.common.function.RunnableEx
import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.*
import io.velo.monitor.BigKeyTopK
import io.velo.repl.Binlog
import io.velo.repl.ReplPairTest
import io.velo.repl.incremental.XWalV
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class OneSlotTest extends Specification {
    final short slot = 0
    final short slotNumber = 1

    def 'test mock'() {
        given:
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)

        expect:
        oneSlot.allKeyCount == 0

        when:
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        oneSlot.metaChunkSegmentIndex.cleanUp()

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def oneSlot2 = new OneSlot(slot, eventloopCurrent)
        oneSlot2.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def call = oneSlot2.asyncCall(() -> 1)
        def run = oneSlot2.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.result
        run != null

        when:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        def oneSlot3 = new OneSlot(slot, eventloop)
        Thread.start {
            eventloop.run()
        }
        call = oneSlot3.asyncCall(() -> 1)
        run = oneSlot3.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        call.whenResult { result -> result == 1 }.result
        run != null

        when:
        def oneSlot4 = new OneSlot(slot)
        oneSlot4.collect()
        then:
        oneSlot4.slot() == slot

        cleanup:
        oneSlot2.flush()
        oneSlot2.cleanUp()
        eventloop.breakEventloop()
    }

    def 'test init'() {
        given:
        def persistConfig = Config.create()
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber)

        Consts.slotDir.deleteDir()

        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)
        def oneSlot1 = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)
        println oneSlot.toString()
        println oneSlot1.toString()

        expect:
        oneSlot.slot() == slot
        oneSlot1.slot() == slot
        oneSlot.snowFlake == snowFlake
        oneSlot.masterUuid > 0
        !oneSlot.isAsSlave()
        oneSlot.getReplPairAsSlave(11L) == null
        oneSlot.getOnlyOneReplPairAsSlave() == null
        oneSlot.firstReplPairAsMaster == null
        oneSlot.avgTtlInSecond == 0

        when:
        def persistConfig2 = Config.create().with('volumeDirsBySlot',
                '/tmp/data0:0-31,/tmp/data1:32-63,/tmp/data2:64-95,/tmp/data3:96-127')
        new File('/tmp/data0').mkdirs()
        new File('/tmp/data1').mkdirs()
        new File('/tmp/data2').mkdirs()
        new File('/tmp/data3').mkdirs()
        def tmpTestSlotNumber = (short) 128
        ConfVolumeDirsForSlot.initFromConfig(persistConfig2, tmpTestSlotNumber)
        def oneSlot0 = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig2)
        def oneSlot32 = new OneSlot((byte) 32, tmpTestSlotNumber, snowFlake, Consts.persistDir, persistConfig2)
        def oneSlot32_ = new OneSlot((byte) 32, tmpTestSlotNumber, snowFlake, Consts.persistDir, persistConfig2)
        then:
        oneSlot0.slot() == slot
        oneSlot32.slot() == (byte) 32
        oneSlot32_.slot() == (byte) 32
        oneSlot0.slotDir.absolutePath == '/tmp/data0/slot-0'
        oneSlot32.slotDir.absolutePath == '/tmp/data1/slot-32'

        when:
        // test big key top k init
        oneSlot.dynConfig.update(BigKeyTopK.KEY_IN_DYN_CONFIG, '100')
        oneSlot.initBigKeyTopK(10)
        oneSlot.monitorBigKeyByValueLength('test'.bytes, 1024)
        then:
        oneSlot.bigKeyTopK != null
        oneSlot.bigKeyTopK.size() == 1

        when:
        oneSlot.handleWhenCvExpiredOrDeleted('', null, null)
        oneSlot.handlersRegisteredList << new HandlerWhenCvExpiredOrDeleted() {
            @Override
            void handleWhenCvExpiredOrDeleted(@NotNull String key, @Nullable CompressedValue shortStringCv, @Nullable PersistValueMeta pvm) {
                println "test handle when cv expired or deleted, key: $key, cv: $shortStringCv, pvm: $pvm"
            }
        }
        oneSlot.handleWhenCvExpiredOrDeleted('', null, null)
        then:
        1 == 1

        when:
        oneSlot.ttlTotalInSecond = 100
        oneSlot.putCountTotal = 1
        then:
        oneSlot.avgTtlInSecond == 100

        when:
        def f = oneSlot.walLazyReadFromFile()
        f.join()
        then:
        f.get()

        cleanup:
        oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemory = true
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def cvList = Mock.prepareCompressedValueList(2)
        def cvAsShortValue = cvList[0]
        def cv = cvList[1]
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[100]
        cv.compressedLength = 100
        100.times {
            def keyForShortValue = "key:$it"
            def key = "key:${it + 10000}"
            def sKeyForShortValue = BaseCommand.slot(keyForShortValue.bytes, slotNumber)
            def sKey = BaseCommand.slot(key.bytes, slotNumber)
            oneSlot.put(keyForShortValue, sKeyForShortValue.bucketIndex(), cvAsShortValue)
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.loadFromLastSavedFileWhenPureMemory()
        oneSlot.writeToSavedFileWhenPureMemory()
        oneSlot.loadFromLastSavedFileWhenPureMemory()
        then:
        oneSlot.allKeyCount == 200

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }

    def 'test repl pair'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, Config.create())

        when:
        // test repl pair
        def replPairAsMaster0 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster0.slaveUuid = 11L
        def replPairAsMaster1 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster1.slaveUuid = 12L
        // add 12L first
        oneSlot.replPairs.add(replPairAsMaster1)
        oneSlot.doTask(0)
        oneSlot.replPairs.add(replPairAsMaster0)
        oneSlot.doTask(0)
        def replPairAsSlave0 = oneSlot.createReplPairAsSlave('localhost', 6379)
        def replPairAsSlave1 = oneSlot.createReplPairAsSlave('localhost', 6379)
        replPairAsSlave0.sendBye = true
        replPairAsSlave1.masterBinlogCurrentFileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)
        oneSlot.doTask(0)
        then:
        oneSlot.replPairs.size() == 4
        oneSlot.delayNeedCloseReplPairs.size() == 0
        oneSlot.firstReplPairAsMaster != null
        oneSlot.getReplPairAsMaster(11L) != null
        oneSlot.getReplPairAsSlave(11L) == null
        oneSlot.isAsSlave()
        oneSlot.slaveReplPairListSelfAsMaster.size() == 2
        replPairAsSlave1.masterBinlogCurrentFileIndexAndOffset != null

        when:
        replPairAsMaster0.sendBye = true
        then:
        oneSlot.slaveReplPairListSelfAsMaster.size() == 1

        when:
        replPairAsSlave0.sendBye = true
        replPairAsSlave1.sendBye = false
        oneSlot.removeReplPairAsSlave()
        then:
        oneSlot.delayNeedCloseReplPairs.size() == 1

        when:
        oneSlot.doTask(0)
        then:
        // will remain in 10s
        oneSlot.delayNeedCloseReplPairs.size() == 1

        when:
        Thread.sleep(11 * 1000)
        oneSlot.doTask(0)
        then:
        oneSlot.delayNeedCloseReplPairs.size() == 0

        when:
        // clear all
        oneSlot.replPairs.clear()
        oneSlot.delayNeedCloseReplPairs.clear()
        oneSlot.doTask(0)
        // add 2 as slaves
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.replPairs.add(replPairAsSlave1)
        replPairAsSlave0.addToFetchBigStringUuid(1L)
        oneSlot.doTask(0)
        oneSlot.doTask(1)
        replPairAsSlave0.sendBye = false
        replPairAsSlave1.sendBye = false
        oneSlot.removeReplPairAsSlave()
        then:
        oneSlot.replPairs.size() == 2
        oneSlot.delayNeedCloseReplPairs.size() == 2

        when:
        Thread.sleep(11 * 1000)
        oneSlot.doTask(0)
        oneSlot.doTask(1)
        then:
        oneSlot.delayNeedCloseReplPairs.size() == 0

        when:
        // clear all
        oneSlot.replPairs.clear()
        oneSlot.delayNeedCloseReplPairs.clear()
        then:
        oneSlot.getReplPairAsMaster(11L) == null

        when:
        oneSlot.replPairs.add(replPairAsMaster0)
        replPairAsMaster0.sendBye = true
        then:
        oneSlot.getReplPairAsMaster(11L) == null

        when:
        replPairAsMaster0.sendBye = false
        then:
        oneSlot.getReplPairAsMaster(11L) != null

        when:
        oneSlot.metaChunkSegmentIndex = new MetaChunkSegmentIndex(slot, oneSlot.slotDir)
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsMaster1)
        replPairAsMaster1.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)
        oneSlot.collect()
        then:
        oneSlot.getReplPairAsMaster(11L) == null
        replPairAsMaster1.slaveLastCatchUpBinlogFileIndexAndOffset != null

        when:
        replPairAsSlave0.sendBye = false
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.collect()
        then:
        oneSlot.getReplPairAsMaster(11L) == null
        oneSlot.getReplPairAsSlave(oneSlot.masterUuid) != null
        oneSlot.onlyOneReplPairAsSlave != null

        when:
        oneSlot.replPairs.clear()
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        then:
        oneSlot.replPairs.size() == 2

        when:
        // already exist one as master
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        then:
        oneSlot.replPairs.size() == 2

        cleanup:
        oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test eventloop'() {
        given:
        def persistConfig = Config.create()
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, slotNumber)

        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot, slotNumber, snowFlake, Consts.persistDir, persistConfig)

        and:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, slotNumber, null, Config.create())
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        oneSlot.netWorkerEventloop = eventloop
        oneSlot.requestHandler = requestHandler
        Thread.sleep(100)
        oneSlot.threadIdProtectedForSafe = eventloop.eventloopThread.threadId()

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        when:
        def p = oneSlot.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        p.whenComplete(RunnableEx.of(() -> {
            println 'complete async run'
            true
        })).result

        when:
        def p2 = oneSlot.asyncCall {
            println 'async call'
            1
        }
        eventloopCurrent.run()
        then:
        p2.whenComplete((i, e) -> {
            println 'complete async call'
            i == 1
        }).result

        when:
        int[] array = [0]
        oneSlot.delayRun(100, () -> {
            println 'delay run'
            array[0] = 1
        })
        eventloopCurrent.run()
        Thread.sleep(100)
        then:
        array[0] == 1

        when:
        oneSlot.netWorkerEventloop = eventloopCurrent
        oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def p11 = oneSlot.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        p11 != null

        when:
        def p22 = oneSlot.asyncCall {
            println 'async call'
            1
        }
        eventloopCurrent.run()
        then:
        p22.whenComplete((i, e) -> {
            println 'complete async call'
            i == 1
        }).result

        cleanup:
        eventloop.breakEventloop()
        Consts.persistDir.deleteDir()
    }

    static List<String> batchPut(OneSlot oneSlot, int n = 300, int length = 10, int bucketIndex = 0, int slotNumber = 1) {
        // refer KeyHashTest
        // mock key list and bucket index is 0
        // 300 keys will cause wal refresh to key buckets file
        ConfForSlot.global.confWal.shortValueSizeTrigger = 100
        def bucketIndex0KeyList = Mock.prepareTargetBucketIndexKeyList(n, bucketIndex)
        def random = new Random()
        for (key in bucketIndex0KeyList) {
            def s = BaseCommand.slot(key.bytes, slotNumber)
            def cv = new CompressedValue()
            cv.keyHash = s.keyHash()
            cv.compressedData = new byte[length]
            cv.compressedLength = length
            cv.uncompressedLength = length
            cv.seq = oneSlot.snowFlake.nextId()

            if (random.nextInt(10) == 1) {
                cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
                def bytes = new byte[4]
                ByteBuffer.wrap(bytes).putInt(random.nextInt(10000))
                cv.compressedData = bytes
            }

            // 10% expired
            cv.expireAt = random.nextInt(10) == 1 ? CompressedValue.EXPIRE_NOW : CompressedValue.NO_EXPIRE
            oneSlot.put(key, s.bucketIndex(), cv)
        }
        bucketIndex0KeyList
    }

    def 'test dyn config and big string files and kv lru'() {
        given:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def oneSlot2 = localPersist.oneSlot((byte) 1)

        expect:
        oneSlot.bigStringFiles != null
        oneSlot.bigStringDir != null
        oneSlot.clearKvInTargetWalGroupIndexLRU(0) == 0
        oneSlot.clearKvInTargetWalGroupIndexLRU(1) == 0
        oneSlot.dynConfig != null
        !oneSlot.readonly
        oneSlot.canRead
        oneSlot.updateDynConfig(BigKeyTopK.KEY_IN_DYN_CONFIG, '100')
        !oneSlot.updateDynConfig('xxx', 'xxx')
        oneSlot.updateDynConfig('testKey', '1')
        !oneSlot.updateDynConfig('testKey2', '1')

        when:
        oneSlot.readonly = true
        oneSlot.canRead = false
        then:
        oneSlot.readonly
        !oneSlot.canRead

        when:
        oneSlot.resetAsMaster()
        then:
        oneSlot.canRead
        !oneSlot.readonly

        when:
        oneSlot.resetAsSlave('localhost', 6379)
        then:
        !oneSlot.canRead
        oneSlot.readonly

        when:
        // just for log
        oneSlot.lruClearedCount = 9
        then:
        oneSlot.clearKvInTargetWalGroupIndexLRU(0) == 0

        when:
        oneSlot.readonly = false
        oneSlot.canRead = true
        def bucketIndex0KeyList = batchPut(oneSlot)
        // so read must be from key buckets file
        oneSlot.getWalByGroupIndex(0).clear()
        oneSlot.getWalByBucketIndex(0).clear()
        oneSlot.getWalByBucketIndex(1).clear()
        for (key in bucketIndex0KeyList) {
            def s = BaseCommand.slot(key.bytes, slotNumber)
            oneSlot.get(key.bytes, s.bucketIndex(), s.keyHash(), s.keyHash32())
        }
        then:
        oneSlot.kvByWalGroupIndexLRUCountTotal() > 0
        oneSlot.clearKvInTargetWalGroupIndexLRU(0) > 0

        when:
        def bigStringKey = 'kerry-test-big-string-key'
        def sBigString = BaseCommand.slot(bigStringKey.bytes, slotNumber)
        def cvBigString = Mock.prepareCompressedValueList(1)[0]
        cvBigString.keyHash = sBigString.keyHash()
        def rBigString = oneSlot.get(bigStringKey.bytes, sBigString.bucketIndex(), sBigString.keyHash(), sBigString.keyHash32())
        then:
        rBigString == null

        when:
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString)
        rBigString = oneSlot.get(bigStringKey.bytes, sBigString.bucketIndex(), sBigString.keyHash(), sBigString.keyHash32())
        then:
        rBigString != null

        when:
        oneSlot.globalGauge.collect()
        oneSlot.collect()
        oneSlot.kvLRUHitTotal = 1
        oneSlot.createReplPairAsSlave('localhost', 6379)
        oneSlot.collect()
        then:
        1 == 1

        when:
        oneSlot.doMockWhenCreateReplPairAsSlave = true
        oneSlot.createReplPairAsSlave('localhost', 6379)
        then:
        1 == 1

        when:
        def n = oneSlot.warmUp()
        then:
        n >= 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test meta and key loader and task'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        println oneSlot.taskChain

        expect:
        oneSlot.metaChunkSegmentFlagSeq != null
        oneSlot.metaChunkSegmentIndex != null
        oneSlot.keyLoader != null
        oneSlot.taskChain != null
        oneSlot.walKeyCount == 0
        oneSlot.allKeyCount == 0
        oneSlot.chunkWriteSegmentIndexInt == 0

        when:
        oneSlot.setMetaChunkSegmentIndexInt(0)
        oneSlot.setChunkSegmentIndexFromMeta()
        then:
        oneSlot.chunk.segmentIndex == 0

        when:
        oneSlot.setMetaChunkSegmentIndexInt(1, true)
        then:
        oneSlot.chunk.segmentIndex == 1

        when:
        boolean exception = false
        try {
            oneSlot.setMetaChunkSegmentIndexInt(-1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            oneSlot.setMetaChunkSegmentIndexInt(oneSlot.chunk.maxSegmentIndex + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        oneSlot.doTask(0)
        oneSlot.taskChain.doTask(1)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test put and get and remove'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
//        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        println 'in memory size estimate: ' + oneSlot.estimate(new StringBuilder())

        and:
        def key = 'key'
        def sKey = BaseCommand.slot(key.bytes, slotNumber)

        def v = Mock.prepareValueList(1)[0]
        def xWalV = new XWalV(v, true, 0)
        oneSlot.appendBinlog(xWalV)

        expect:
        oneSlot.binlog != null

        when:
        boolean exception = false
        try {
            oneSlot.getExpireAt(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        then:
        oneSlot.getExpireAt(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) == null

        when:
        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.compressedData = new byte[10]
        cv.compressedLength = 10
        cv.uncompressedLength = 10
        cv.expireAt = System.currentTimeMillis()
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.getExpireAt(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) == cv.expireAt
        oneSlot.get(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) != null

        when:
        oneSlot.removeDelay(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.getExpireAt(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) == null
        oneSlot.get(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) == null

        when:
        def bucketIndex0KeyList = batchPut(oneSlot)
        oneSlot.getWalByBucketIndex(0).clear()
        // get to lru
        def firstKey = bucketIndex0KeyList[0]
        def sFirstKey = BaseCommand.slot(firstKey.bytes, slotNumber)
        2.times {
            oneSlot.get(firstKey.bytes, sFirstKey.bucketIndex(), sFirstKey.keyHash(), sFirstKey.keyHash32())
        }
        println 'in memory size estimate: ' + oneSlot.estimate(new StringBuilder())
        then:
        oneSlot.getExpireAt(firstKey.bytes, sFirstKey.bucketIndex(), sFirstKey.keyHash(), sFirstKey.keyHash32()) != null

        when:
        oneSlot.clearKvInTargetWalGroupIndexLRU(0)
        then:
        oneSlot.getExpireAt(firstKey.bytes, sFirstKey.bucketIndex(), sFirstKey.keyHash(), sFirstKey.keyHash32()) != null

        when:
        def notExistKey = 'not-exist-key'
        def sNotExistKey = BaseCommand.slot(notExistKey.bytes, slotNumber)
        then:
        oneSlot.get(notExistKey.bytes, sNotExistKey.bucketIndex(), sNotExistKey.keyHash(), sNotExistKey.keyHash32()) == null

        when:
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.compressedLength = cv.compressedData.length
        cv.uncompressedLength = cv.compressedData.length
        cv.expireAt = CompressedValue.NO_EXPIRE
        // make sure do persist
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        then:
        oneSlot.get(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) != null

        when:
        oneSlot.readonly = true
        exception = false
        try {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        } catch (ReadonlyException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        oneSlot.readonly = false
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        cv.compressedData = new byte[4]
        oneSlot.put(key, sKey.bucketIndex(), cv)
        def buf = oneSlot.get(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        then:
        buf != null
        CompressedValue.decode(buf.buf(), key.bytes, sKey.keyHash()).compressedData.length == 4

        when:
        2000.times {
            oneSlot.removeDelay(key, sKey.bucketIndex(), sKey.keyHash())
        }
        then:
        oneSlot.get(key.bytes, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32()) == null
        !oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        !oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())

        when:
        // cv is int -> short string
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())

        when:
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.compressedLength = cv.compressedData.length
        cv.uncompressedLength = cv.compressedData.length
        cv.expireAt = CompressedValue.NO_EXPIRE
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())

        when:
        cv.expireAt = System.currentTimeMillis() + 1000
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        Thread.sleep(1000 + 1)
        then:
        // remove from key loader, already expired
        !oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        !oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        !oneSlot.exists(key + 'not-exist', sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        !oneSlot.remove(key + 'not-exist', sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash(), sKey.keyHash32())

        cleanup:
        oneSlot.flush()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test direct methods call'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        expect:
        !oneSlot.hasData(0, 10)

        when:
        def bytesForMerge = oneSlot.preadForMerge(0, 10)
        def bytesForRepl = oneSlot.preadForRepl(0)
        then:
        bytesForMerge == null
        bytesForRepl == null

        when:
        def mockBytesFromMaster = new byte[oneSlot.chunk.chunkSegmentLength]
        Arrays.fill(mockBytesFromMaster, (byte) 1)
        oneSlot.writeChunkSegmentsFromMasterExists(mockBytesFromMaster, 0, 1)
        oneSlot.setSegmentMergeFlag(0, Chunk.Flag.new_write.flagByte(), 1L, 0)
        def bytesOneSegment = oneSlot.preadForMerge(0, 1)
        def bytesNSegments = oneSlot.preadForRepl(0)
        then:
        bytesOneSegment == mockBytesFromMaster
        bytesNSegments.length == oneSlot.chunk.chunkSegmentLength * FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD

        when:
        boolean exception = false
        mockBytesFromMaster = new byte[oneSlot.chunk.chunkSegmentLength + 1]
        try {
            oneSlot.writeChunkSegmentsFromMasterExists(mockBytesFromMaster, 0, 1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        oneSlot.getSegmentMergeFlag(0)
        oneSlot.getSegmentMergeFlagBatch(0, 1)
        exception = false
        try {
            oneSlot.getSegmentMergeFlag(-1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            oneSlot.getSegmentMergeFlagBatch(-1, 1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            oneSlot.getSegmentMergeFlag(oneSlot.chunk.maxSegmentIndex + 1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            oneSlot.getSegmentMergeFlagBatch(0, oneSlot.chunk.maxSegmentIndex + 1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        oneSlot.getSegmentSeqListBatchForRepl(0, 1)
        oneSlot.updateSegmentMergeFlag(0, Chunk.Flag.merged.flagByte(), 1L)
        List<Long> segmentSeqList = [1L]
        oneSlot.setSegmentMergeFlagBatch(0, 1, Chunk.Flag.merged.flagByte(), segmentSeqList, 0)
        then:
        1 == 1

        when:
        ArrayList<Integer> needMergeSegmentIndexList = [0]
        oneSlot.doMergeJob(needMergeSegmentIndexList)
        oneSlot.doMergeJobWhenServerStart(needMergeSegmentIndexList)
        oneSlot.persistMergingOrMergedSegmentsButNotPersisted()
        oneSlot.getMergedSegmentIndexEndLastTime()
        then:
        1 == 1

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test before persist wal read for merge'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk

        def ext = new OneSlot.BeforePersistWalExtFromMerge([], [])
//        def ext2 = new OneSlot.BeforePersistWalExt2FromMerge([], [])
        expect:
        ext.isEmpty()

        when:
        final int walGroupIndex = 0
        chunk.initSegmentIndexWhenFirstStart(0)
        def e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        e == null

        when:
        Debug.instance.logMerge = true
        oneSlot.logMergeCount = 999
        e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        e == null

        when:
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.setSegmentMergeFlag(0, Chunk.Flag.new_write.flagByte(), 1L, walGroupIndex)
        oneSlot.logMergeCount = 999
        e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        // no segment bytes read
        e.isEmpty()

        when:
        // last N
        oneSlot.setSegmentMergeFlag(chunk.maxSegmentIndex - 10, Chunk.Flag.new_write.flagByte(), 1L, walGroupIndex)
        oneSlot.setSegmentMergeFlag(chunk.maxSegmentIndex - 9, Chunk.Flag.new_write.flagByte(), 1L, walGroupIndex)
        e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        e.isEmpty()

        when:
        final String testMergedKey = 'xh!0_test-merged-key'
        def cv = new CompressedValue()
        cv.keyHash = KeyHash.hash(testMergedKey.bytes)
        oneSlot.chunkMergeWorker.addMergedSegment(0, 1)
        oneSlot.chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, testMergedKey, 0, 0))
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.Flag.merged.flagByte(), 1L, walGroupIndex)
        chunk.initSegmentIndexWhenFirstStart(1)
        10.times {
            batchPut(oneSlot, 100, 100, 0, slotNumber)
        }
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.logMergeCount = 999
        e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        !e.isEmpty()

        when:
        // trigger persist wal
        5.times {
            batchPut(oneSlot, 100, 100, 1, slotNumber)
        }
        then:
        oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(1).flagByte() == Chunk.Flag.merged_and_persisted.flagByte()
        oneSlot.chunkMergeWorker.isMergedSegmentSetEmpty()

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test check merged but not persist'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk
        def chunkMergeWorker = oneSlot.chunkMergeWorker

        when:
        final int walGroupIndex = 0
        chunk.initSegmentIndexWhenFirstStart(chunk.maxSegmentIndex - 10)
        oneSlot.setSegmentMergeFlag(chunk.maxSegmentIndex - 10, Chunk.Flag.reuse.flagByte(), 1L, walGroupIndex)
        oneSlot.setSegmentMergeFlag(chunk.maxSegmentIndex - 9, Chunk.Flag.merged_and_persisted.flagByte(), 1L, walGroupIndex)
        oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(true)
        then:
        1 == 1

        when:
        oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(false)
        then:
        1 == 1

        when:
        oneSlot.setSegmentMergeFlag(chunk.maxSegmentIndex - 8, Chunk.Flag.new_write.flagByte(), 1L, walGroupIndex)
        oneSlot.checkNotMergedAndPersistedNextRangeSegmentIndexTooNear(true)
        then:
        1 == 1

        when:
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        String testMergedKey = 'xh!0_test-merged-key'
        def cv = new CompressedValue()
        cv.keyHash = KeyHash.hash(testMergedKey.bytes)
        chunkMergeWorker.addMergedSegment(1, 1)
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, testMergedKey, 0, 1))
        chunk.initSegmentIndexWhenFirstStart(0)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(100, 1)
        chunk.initSegmentIndexWhenFirstStart(0)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(0, 1)
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedCvList()
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(chunk.halfSegmentNumber + 1, 1)
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, testMergedKey, 0, chunk.halfSegmentNumber + 1))
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(chunk.halfSegmentNumber + 100, 1)
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(chunk.halfSegmentNumber - 1, 1)
        chunk.initSegmentIndexWhenFirstStart(chunk.halfSegmentNumber)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedCvList()
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(0, 1)
        chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(cv, testMergedKey, 0, 0))
        chunk.initSegmentIndexWhenFirstStart(chunk.maxSegmentIndex - 1)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(100, 1)
        chunk.initSegmentIndexWhenFirstStart(chunk.maxSegmentIndex - 1)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        when:
        chunkMergeWorker.clearMergedSegmentSet()
        chunkMergeWorker.addMergedSegment(0, 1)
        chunk.initSegmentIndexWhenFirstStart(chunk.maxSegmentIndex - 100)
        oneSlot.checkFirstMergedButNotPersistedSegmentIndexTooNear()
        then:
        1 == 1

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test init chunk flag fail'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        Chunk.ONCE_PREPARE_SEGMENT_COUNT.times {
            oneSlot.setSegmentMergeFlag(it, Chunk.Flag.merged.flagByte(), 1L, 0)
        }
        oneSlot.cleanUp()

        when:
        // load again
        boolean exception = false
        try {
            LocalPersistTest.prepareLocalPersist()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        Consts.persistDir.deleteDir()
    }

    def 'test lru in memory size'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        println 'lru in memory size: ' + oneSlot.inMemorySizeOfLRU()

        expect:
        oneSlot.randomKeyInLRU(0) == null

        when:
        def cvList = Mock.prepareCompressedValueList(100)
        for (cv in cvList) {
            def encoded = cv.encode()
            oneSlot.putKvInTargetWalGroupIndexLRU(0, 'key:' + cv.seq, encoded)
            oneSlot.putKvInTargetWalGroupIndexLRU(1, 'key:' + cv.seq, encoded)
        }
        println 'lru in memory size: ' + oneSlot.inMemorySizeOfLRU()
        then:
        oneSlot.kvByWalGroupIndexLRUCountTotal() == 2 * (ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize / Wal.calcWalGroupNumber()).intValue()
        oneSlot.randomKeyInLRU(0) != null

        when:
        ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize *= 4
        oneSlot.initLRU(true)
        for (cv in cvList) {
            def encoded = cv.encode()
            oneSlot.putKvInTargetWalGroupIndexLRU(0, 'key:' + cv.seq, encoded)
            oneSlot.putKvInTargetWalGroupIndexLRU(1, 'key:' + cv.seq, encoded)
        }
        then:
        oneSlot.kvByWalGroupIndexLRUCountTotal() == 200

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test pure memory mode change chunk segment flag'() {
        given:
        ConfForGlobal.pureMemory = true
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def chunk = oneSlot.chunk

        when:
        chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        oneSlot.setSegmentMergeFlag(0, Chunk.Flag.merged.flagByte(), 1L, 0)
        ArrayList<Long> seqList = [1L]
        oneSlot.setSegmentMergeFlagBatch(0, 1, Chunk.Flag.merged.flagByte(), seqList, 0)
        then:
        chunk.preadOneSegment(0) != null

        when:
        oneSlot.setSegmentMergeFlag(0, Chunk.Flag.merged_and_persisted.flagByte(), 1L, 0)
        oneSlot.setSegmentMergeFlagBatch(0, 1, Chunk.Flag.merged_and_persisted.flagByte(), seqList, 0)
        then:
        chunk.preadOneSegment(0) == null

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }

    def 'test run index handler'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        println oneSlot.threadIdProtectedForSafe

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        boolean runResult = false
        oneSlot.submitIndexJobRun('word0', (indexHandler) -> {
            println 'index job run'
        }).whenComplete { r, e ->
            println 'index job run complete'
            oneSlot.submitIndexJobDone()
            runResult = true
        }
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        runResult

        when:
        runResult = false
        oneSlot.pendingSubmitIndexJobRunCount = -1
        oneSlot.submitIndexJobRun('word0', (indexHandler) -> {
            println 'index job run again'
        }).whenComplete { r, e ->
            println 'index job run again complete'
            oneSlot.submitIndexJobDone()
            runResult = true
        }
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        runResult

        when:
        runResult = false
        oneSlot.submitIndexToTargetWorkerJobRun((byte) 0, (indexHandler) -> {
            println 'index worker 0 job run again'
        }).whenComplete { r, e ->
            println 'index worker 0 job run again complete'
            oneSlot.submitIndexJobDone()
            runResult = true
        }
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        runResult

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }
}
