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

    def 'test healthy warn collection'() {
        given:
        Consts.slotDir.deleteDir()
        def keyLoader = new KeyLoader(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, new SnowFlake(1, 1))

        expect:
        new OneSlot(slot).collectHealthWarnings().isEmpty()
        keyLoader.calcKeyCountSkew() == null

        when:
        keyLoader.initFds()
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)

        then:
        oneSlot.collectHealthWarnings().isEmpty()

        when:
        keyLoader.updateKeyCountBatch(0, 0, [(short) (KeyBucket.INIT_CAPACITY * KeyLoader.MAX_SPLIT_NUMBER)] as short[])
        def warnings = oneSlot.collectHealthWarnings()

        then:
        warnings.size() == 1
        warnings[0].contains('key_bucket_key_count_max=432')

        when:
        keyLoader.updateKeyCountBatch(0, 0, [(short) KeyBucket.INIT_CAPACITY] as short[])
        warnings = oneSlot.collectHealthWarnings()

        then:
        warnings.size() == 1
        warnings[0].contains('key_bucket_skew_ratio_max_to_avg=')

        when:
        oneSlot.chunk = new Chunk(slot, Consts.slotDir, oneSlot)
        def warnUsedSegmentCount = (int) Math.ceil((oneSlot.chunk.maxSegmentIndex + 1) * OneSlot.HEALTH_WARN_CHUNK_SEGMENT_FILL_RATE)
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlagBatch(0, warnUsedSegmentCount, Chunk.SEGMENT_FLAG_HAS_DATA, null, 0)
        warnings = oneSlot.collectHealthWarnings()

        then:
        warnings.size() == 2
        warnings.any { it.contains('chunk_segment_fill_rate=') }
        warnings.any { it.contains('key_bucket_skew_ratio_max_to_avg=') }

        cleanup:
        keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        oneSlot.metaChunkSegmentIndex.cleanUp()
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
        def volumeDirs = ['/tmp/data0', '/tmp/data1', '/tmp/data2', '/tmp/data3'].collect { new File(it) }
        volumeDirs.each {
            it.deleteDir()
            it.mkdirs()
        }
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
        oneSlot.monitorBigKeyByValueLength('test', 2048)
        then:
        oneSlot.bigKeyTopK != null
        oneSlot.bigKeyTopK.size() == 1

        when: 'runtime DynConfig update to k=20 should recreate the tracker'
        oneSlot.initBigKeyTopK(20)
        for (i in 0..<20) {
            oneSlot.monitorBigKeyByValueLength('big:' + i, 4096 + i)
        }
        then:
        oneSlot.bigKeyTopK.size() == 20

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
        volumeDirs?.each { it.deleteDir() }
    }

    def 'test repl pair'() {
        given:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        // test repl pair
        def replPairAsMaster0 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster0.slaveUuid = 11L
        def replPairAsMaster1 = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster1.slaveUuid = 12L
        // add 12L first
        oneSlot.replPairs.add(replPairAsMaster1)
        oneSlot.doTask(100)
        oneSlot.replPairs.add(replPairAsMaster0)
        oneSlot.doTask(100)
        def replPairAsSlave0 = oneSlot.createReplPairAsSlave('localhost', 6379)
        def replPairAsSlave1 = oneSlot.createReplPairAsSlave('localhost', 6379)
        replPairAsSlave0.sendBye = true
        replPairAsSlave1.masterBinlogCurrentFileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)
        oneSlot.doTask(100)
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
        oneSlot.doTask(100)
        then:
        // will remain in 10s
        oneSlot.delayNeedCloseReplPairs.size() == 1

        when:
        Thread.sleep(11 * 1000)
        oneSlot.doTask(100)
        then:
        oneSlot.delayNeedCloseReplPairs.size() == 0

        when:
        // clear all
        oneSlot.replPairs.clear()
        oneSlot.delayNeedCloseReplPairs.clear()
        oneSlot.doTask(100)
        // add 2 as slaves
        oneSlot.replPairs.add(replPairAsSlave0)
        oneSlot.replPairs.add(replPairAsSlave1)
        replPairAsSlave0.addToFetchBigStringId(1L, 0, 1L, 'a')
        oneSlot.doTask(100)
        oneSlot.doTask(200)
        replPairAsSlave0.sendBye = false
        replPairAsSlave1.sendBye = false
        oneSlot.removeReplPairAsSlave()
        then:
        oneSlot.replPairs.size() == 2
        oneSlot.delayNeedCloseReplPairs.size() == 2

        when:
        Thread.sleep(11 * 1000)
        oneSlot.doTask(100)
        oneSlot.doTask(200)
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
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test reset as slave removes master side repl pairs'() {
        given:
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        // simulate this node is a master with one downstream replica
        def replPairAsMaster = ReplPairTest.mockAsMaster(oneSlot.masterUuid)
        replPairAsMaster.slaveUuid = 11L
        oneSlot.replPairs.add(replPairAsMaster)
        // an already byed master pair must be skipped, not double closed
        def replPairAsMasterByed = ReplPairTest.mockOne((short) 0, true, 'localhost', 6390)
        replPairAsMasterByed.masterUuid = oneSlot.masterUuid
        replPairAsMasterByed.slaveUuid = 12L
        replPairAsMasterByed.sendBye = true
        oneSlot.replPairs.add(replPairAsMasterByed)
        // a slave pair must be left untouched by removeReplPairAsMaster
        def replPairAsSlave = ReplPairTest.mockAsSlave(oneSlot.masterUuid, 13L)
        oneSlot.replPairs.add(replPairAsSlave)
        then:
        oneSlot.getReplPairAsMasterList().size() == 1
        !replPairAsMaster.isSendBye()

        when:
        // demote this node to slave of another master
        oneSlot.resetAsSlave('localhost', 6379)
        then:
        // the old master side pair must be byed and no longer active
        oneSlot.getReplPairAsMasterList().isEmpty()
        replPairAsMaster.isSendBye()
        oneSlot.delayNeedCloseReplPairs.contains(replPairAsMaster)
        // the already byed master pair is not added again to the delay close list
        oneSlot.delayNeedCloseReplPairs.count { it == replPairAsMasterByed } == 0
        // the new slave pair is created
        oneSlot.getOnlyOneReplPairAsSlave() != null

        cleanup:
        localPersist.cleanUp()
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
        oneSlot.slotWorkerEventloop = eventloop
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
        oneSlot.slotWorkerEventloop = eventloopCurrent
        oneSlot.threadIdProtectedForSafe = Thread.currentThread().threadId()
        def p11 = oneSlot.asyncRun { println 'async run' }
        eventloopCurrent.run()
        then:
        p11 != null

        when:
        def n = 0
        oneSlot.asyncExecute {
            println 'async submit'
            n = 1
        }
        eventloopCurrent.run()
        then:
        n == 1

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
        int i = 0
        for (key in bucketIndex0KeyList) {
            def s = BaseCommand.slot(key, slotNumber)
            def cv = new CompressedValue()
            cv.keyHash = s.keyHash()
            cv.compressedData = new byte[length]
            cv.seq = oneSlot.snowFlake.nextId()

            if (random.nextInt(10) == 1) {
                cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
                def bytes = new byte[4]
                ByteBuffer.wrap(bytes).putInt(random.nextInt(10000))
                cv.compressedData = bytes
            }

            // 10% expired except the first one
            if (i > 0) {
                cv.expireAt = random.nextInt(10) == 1 ? CompressedValue.EXPIRE_NOW : CompressedValue.NO_EXPIRE
            }
            oneSlot.put(key, s.bucketIndex(), cv)

            i++
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
        // not include global metrics if not the first slot
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
        oneSlot.updateDynConfig('testKey', '1')

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
            def s = BaseCommand.slot(key, slotNumber)
            oneSlot.get(key, s.bucketIndex(), s.keyHash())
        }
        then:
        oneSlot.kvByWalGroupIndexLRUCountTotal() > 0
        oneSlot.clearKvInTargetWalGroupIndexLRU(0) > 0

        when:
        def bigStringKey = 'kerry-test-big-string-key'
        def sBigString = BaseCommand.slot(bigStringKey, slotNumber)
        def cvBigString = Mock.prepareCompressedValueList(1)[0]
        cvBigString.keyHash = sBigString.keyHash()
        def rBigString = oneSlot.get(bigStringKey, sBigString.bucketIndex(), sBigString.keyHash())
        then:
        rBigString == null

        when:
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString)
        rBigString = oneSlot.get(bigStringKey, sBigString.bucketIndex(), sBigString.keyHash())
        then:
        rBigString != null

        when:
        def cvBigString2 = Mock.prepareCompressedValueList(1)[0]
        cvBigString2.keyHash = sBigString.keyHash()
        cvBigString2.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString2)
        rBigString = oneSlot.get(bigStringKey, sBigString.bucketIndex(), sBigString.keyHash())
        then:
        rBigString != null

        when:
        def cvBigString3 = Mock.prepareCompressedValueList(1)[0]
        cvBigString3.seq = oneSlot.snowFlake.nextId()
        cvBigString3.expireAt = System.currentTimeMillis() + 10000
        cvBigString3.keyHash = sBigString.keyHash()
        cvBigString3.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvBigString3.setCompressedDataAsBigString(cvBigString3.seq, Dict.SELF_ZSTD_DICT_SEQ)
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString3)
        rBigString = oneSlot.get(bigStringKey, sBigString.bucketIndex(), sBigString.keyHash())
        then:
        rBigString != null
        CompressedValue.decode(rBigString.buf(), bigStringKey.bytes, sBigString.keyHash()).seq == cvBigString3.seq

        when:
        def inspector = new SocketInspector()
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Eventloop[] eventloopArray = [eventloopCurrent]
        inspector.initByNetWorkerEventloopArray(eventloopArray, eventloopArray)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector
        oneSlot.extCvListCheckCountTotal = 1
        oneSlot.extCvValidCountTotal = 1
        oneSlot.extCvInvalidCountTotal = 1
        oneSlot.clearGlobalMetricsCollect()
        oneSlot.addGlobalMetricsCollect()
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
        def t = oneSlot.walLazyReadFromFile().get()
        def n = oneSlot.warmUp().get()
        then:
        t
        n >= 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test reset as slave no stream for extra scale-up slot'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when: 'reset as slave without opening a repl stream (extra 2N slot)'
        oneSlot.doMockWhenCreateReplPairAsSlave = true
        oneSlot.resetAsSlave('localhost', 6379, false)
        then:
        oneSlot.readonly
        !oneSlot.canRead
        // NO slave ReplPair was created
        oneSlot.getOnlyOneReplPairAsSlave() == null

        when: 'reset as slave WITH opening a repl stream (stream slot)'
        oneSlot.resetAsSlave('localhost', 6379, true)
        then:
        oneSlot.readonly
        !oneSlot.canRead
        // a slave ReplPair WAS created
        oneSlot.getOnlyOneReplPairAsSlave() != null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test big string uuid is not key hash'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def forcedKeyHash = 123456789L
        def key1 = 'big-string-key-1'
        def key2 = 'big-string-key-2'

        and:
        def cv1 = new CompressedValue()
        cv1.seq = oneSlot.snowFlake.nextId()
        cv1.keyHash = forcedKeyHash
        cv1.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]

        def cv2 = new CompressedValue()
        cv2.seq = oneSlot.snowFlake.nextId()
        cv2.keyHash = forcedKeyHash
        cv2.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]

        when:
        oneSlot.put(key1, bucketIndex, cv1)
        oneSlot.put(key2, bucketIndex, cv2)

        then:
        def uuid1 = oneSlot.bigStringFiles.bigStringUuidByKey.get(key1)
        def uuid2 = oneSlot.bigStringFiles.bigStringUuidByKey.get(key2)
        uuid1 != null
        uuid2 != null
        uuid1 != forcedKeyHash
        uuid2 != forcedKeyHash
        uuid1 != uuid2
        oneSlot.bigStringFiles.getBigStringFileIdList(bucketIndex).size() == 2
        new File(oneSlot.bigStringFiles.bigStringDir, bucketIndex + '/' + uuid1 + '_' + forcedKeyHash).exists()
        new File(oneSlot.bigStringFiles.bigStringDir, bucketIndex + '/' + uuid2 + '_' + forcedKeyHash).exists()

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test overwrite same big string key deletes stale uuid on first cleanup tick'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'kerry-test-big-string-overwrite-key'

        and:
        def cv1 = new CompressedValue()
        cv1.seq = oneSlot.snowFlake.nextId()
        cv1.keyHash = 222222222L
        cv1.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv1)
        def firstUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)

        def cv2 = new CompressedValue()
        cv2.seq = oneSlot.snowFlake.nextId()
        cv2.keyHash = 222222222L
        cv2.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv2)
        def currentUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)

        when:
        def count = oneSlot.intervalDeleteOverwriteBigStringFiles(bucketIndex)

        then:
        firstUuid != currentUuid
        count == 0
        oneSlot.bigStringFiles.getBigStringFileIdList(bucketIndex)*.uuid() == [currentUuid]

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test overwrite big string with normal long value enqueues stale file for deletion'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'big-str-oversize-key'
        def s = BaseCommand.slot(key, slotNumber)
        def keyHash = s.keyHash()

        and: 'write a big-string value (oversized triggers big-string path via isPersistLengthOverSegmentLength)'
        def cvBig = new CompressedValue()
        cvBig.seq = oneSlot.snowFlake.nextId()
        cvBig.keyHash = keyHash
        cvBig.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cvBig)
        def bigStringUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)
        assert bigStringUuid != null

        when: 'overwrite with a normal long value (not short, not big-string, not oversized)'
        def cvNormal = new CompressedValue()
        cvNormal.seq = oneSlot.snowFlake.nextId()
        cvNormal.keyHash = keyHash
        cvNormal.compressedData = ('x' * 200).bytes
        oneSlot.put(key, bucketIndex, cvNormal)

        then: 'old big-string file should be enqueued for immediate deletion'
        !oneSlot.delayToDeleteBigStringFileIds.isEmpty()
        oneSlot.delayToDeleteBigStringFileIds.first.uuid() == bigStringUuid

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test overwrite short big string meta deletes stale uuid on first cleanup tick'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'short-big-string-overwrite-key'
        def keyHash = 333333333L

        and:
        def firstUuid = 9001L
        oneSlot.bigStringFiles.writeBigStringBytes(firstUuid, bucketIndex, keyHash, 'first'.bytes)
        def cv1 = new CompressedValue()
        cv1.seq = oneSlot.snowFlake.nextId()
        cv1.keyHash = keyHash
        cv1.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv1.setCompressedDataAsBigString(firstUuid, CompressedValue.NULL_DICT_SEQ)
        oneSlot.put(key, bucketIndex, cv1)

        def secondUuid = 9002L
        oneSlot.bigStringFiles.writeBigStringBytes(secondUuid, bucketIndex, keyHash, 'second'.bytes)
        def cv2 = new CompressedValue()
        cv2.seq = oneSlot.snowFlake.nextId()
        cv2.keyHash = keyHash
        cv2.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv2.setCompressedDataAsBigString(secondUuid, CompressedValue.NULL_DICT_SEQ)
        oneSlot.put(key, bucketIndex, cv2)

        when:
        def count = oneSlot.intervalDeleteOverwriteBigStringFiles(bucketIndex)

        then:
        count == 0
        oneSlot.bigStringFiles.getBigStringFileIdList(bucketIndex)*.uuid() == [secondUuid]

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
        oneSlot.updateChunkSegmentIndexFromMeta()
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
        oneSlot.doTask(100)
        oneSlot.taskChain.doTask(200)
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
        def sKey = BaseCommand.slot(key, slotNumber)

        def v = Mock.prepareValueList(1)[0]
        def xWalV = new XWalV(v, true)
        oneSlot.appendBinlog(xWalV)
        oneSlot.binlog = oneSlot.binlog

        expect:
        oneSlot.binlog != null

        when:
        boolean exception = false
        try {
            oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash())
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        then:
        oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash()) == null

        when:
        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.compressedData = new byte[10]
        cv.expireAt = System.currentTimeMillis() + 1000
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash()) == cv.expireAt
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) != null

        when:
        def expiredWalKey = 'expired-wal-key'
        def sExpiredWalKey = BaseCommand.slot(expiredWalKey, slotNumber)
        def expiredWalCv = new CompressedValue()
        expiredWalCv.keyHash = sExpiredWalKey.keyHash()
        expiredWalCv.compressedData = new byte[10]
        expiredWalCv.expireAt = System.currentTimeMillis() - 1
        oneSlot.put(expiredWalKey, sExpiredWalKey.bucketIndex(), expiredWalCv)
        then:
        oneSlot.getExpireAt(expiredWalKey, sExpiredWalKey.bucketIndex(), sExpiredWalKey.keyHash()) == null

        when:
        def expiredLruKey = 'expired-lru-key'
        def sExpiredLruKey = BaseCommand.slot(expiredLruKey, slotNumber)
        def expiredLruCv = new CompressedValue()
        expiredLruCv.keyHash = sExpiredLruKey.keyHash()
        expiredLruCv.compressedData = new byte[10]
        expiredLruCv.expireAt = System.currentTimeMillis() - 1
        oneSlot.putKvInTargetWalGroupIndexLRU(Wal.calcWalGroupIndex(sExpiredLruKey.bucketIndex()),
                expiredLruKey, expiredLruCv.encode())
        then:
        oneSlot.getExpireAt(expiredLruKey, sExpiredLruKey.bucketIndex(), sExpiredLruKey.keyHash()) == null

        when:
        oneSlot.removeDelay(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash()) == null
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) == null

        when:
        def bucketIndex0KeyList = batchPut(oneSlot)
        oneSlot.getWalByBucketIndex(0).clear()
        // get to lru
        def firstKey = bucketIndex0KeyList[0]
        def sFirstKey = BaseCommand.slot(firstKey, slotNumber)
        2.times {
            oneSlot.get(firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash())
        }
        println 'in memory size estimate: ' + oneSlot.estimate(new StringBuilder())
        then:
        oneSlot.getExpireAt(firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash()) != null

        when:
        oneSlot.clearKvInTargetWalGroupIndexLRU(0)
        then:
        oneSlot.getExpireAt(firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash()) != null

        when:
        def notExistKey = 'not-exist-key'
        def sNotExistKey = BaseCommand.slot(notExistKey, slotNumber)
        then:
        oneSlot.get(notExistKey, sNotExistKey.bucketIndex(), sNotExistKey.keyHash()) == null

        when:
        def testPvm = new PersistValueMeta()
        def keyBytes = oneSlot.getOnlyKeyBytesFromSegment(testPvm)
        then:
        keyBytes == null

        when:
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.expireAt = CompressedValue.NO_EXPIRE
        // hot key always in wal cache
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        then:
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) == null

        when:
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).isOnRewrite = false
        // make sure do persist
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        then:
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) != null

        when:
        def keyLoader = oneSlot.keyLoader
        def valueBytesWithExpireAtAndSeq = keyLoader.getValueXByKey(sKey.bucketIndex(), key, sKey.keyHash())
        then:
        valueBytesWithExpireAtAndSeq != null
        PersistValueMeta.isPvm(valueBytesWithExpireAtAndSeq.valueBytes())

        when:
        def testPvm2 = PersistValueMeta.decode(valueBytesWithExpireAtAndSeq.valueBytes())
        def keyBytes2 = oneSlot.getOnlyKeyBytesFromSegment(testPvm2)
        then:
        keyBytes2 == key.bytes

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
        def buf = oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        buf != null
        CompressedValue.decode(buf.buf(), key.bytes, sKey.keyHash()).compressedData.length == 4

        when:
        2000.times {
            oneSlot.removeDelay(key, sKey.bucketIndex(), sKey.keyHash())
        }
        then:
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) == null
        !oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash())
        !oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())

        when:
        // cv is int -> short string
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())

        when:
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.expireAt = CompressedValue.NO_EXPIRE
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())

        when:
        cv.expireAt = System.currentTimeMillis() + 1000
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        Thread.sleep(1000 + 1)
        then:
        // remove from key loader, already expired
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()) == null
        !oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash())
        !oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())
        !oneSlot.exists(key + 'not-exist', sKey.bucketIndex(), sKey.keyHash())
        !oneSlot.remove(key + 'not-exist', sKey.bucketIndex(), sKey.keyHash())

        when:
        cv.expireAt = CompressedValue.NO_EXPIRE
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        then:
        oneSlot.exists(key, sKey.bucketIndex(), sKey.keyHash())
        oneSlot.remove(key, sKey.bucketIndex(), sKey.keyHash())

        cleanup:
        oneSlot.resetWritePositionAfterBulkLoad()
        oneSlot.flush()
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test put if seq bigger'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def key = 'seq-guard-key'
        def sKey = BaseCommand.slot(key, slotNumber)
        def keyHashGiven = sKey.keyHash()
        def makeCv = { long seq, byte marker, long keyHash = keyHashGiven ->
            def cv = new CompressedValue()
            cv.seq = seq
            cv.keyHash = keyHash
            cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
            cv.expireAt = CompressedValue.NO_EXPIRE
            cv.compressedData = [marker] as byte[]
            cv
        }

        when:
        oneSlot.put(key, sKey.bucketIndex(), makeCv(100L, (byte) 100))
        def stalePut = oneSlot.putIfSeqBigger(key, sKey.bucketIndex(), makeCv(99L, (byte) 99), true)
        def equalPut = oneSlot.putIfSeqBigger(key, sKey.bucketIndex(), makeCv(100L, (byte) 101), true)
        def higherPut = oneSlot.putIfSeqBigger(key, sKey.bucketIndex(), makeCv(101L, (byte) 101), true)
        def result = CompressedValue.decode(oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash()).buf(), key.bytes, sKey.keyHash())

        def persistedKey = 'seq-guard-persisted-key'
        def sPersistedKey = BaseCommand.slot(persistedKey, slotNumber)
        def persistedCurrent = new CompressedValue()
        persistedCurrent.seq = 200L
        persistedCurrent.keyHash = sPersistedKey.keyHash()
        persistedCurrent.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        persistedCurrent.expireAt = CompressedValue.NO_EXPIRE
        persistedCurrent.compressedData = [(byte) 10] as byte[]
        oneSlot.keyLoader.putValueByKey(sPersistedKey.bucketIndex(), persistedKey, sPersistedKey.keyHash(),
                persistedCurrent.expireAt, persistedCurrent.seq, persistedCurrent.encodeAsShortString())
        def stalePersistedPut = oneSlot.putIfSeqBigger(persistedKey, sPersistedKey.bucketIndex(), makeCv(199L, (byte) 11, sPersistedKey.keyHash()), true)
        def equalPersistedPut = oneSlot.putIfSeqBigger(persistedKey, sPersistedKey.bucketIndex(), makeCv(200L, (byte) 12, sPersistedKey.keyHash()), true)
        def newerPersistedPut = oneSlot.putIfSeqBigger(persistedKey, sPersistedKey.bucketIndex(),
                makeCv(201L, (byte) 13, sPersistedKey.keyHash()), true)
        oneSlot.readonly = true
        boolean readonlyRejected = false
        try {
            oneSlot.putIfSeqBigger(key, sKey.bucketIndex(), makeCv(102L, (byte) 102), false)
        } catch (ReadonlyException ignored) {
            readonlyRejected = true
        } finally {
            oneSlot.readonly = false
        }

        then:
        !stalePut
        !equalPut
        higherPut
        result.seq == 101L
        result.compressedData[0] == (byte) 101
        !stalePersistedPut
        !equalPersistedPut
        newerPersistedPut
        readonlyRejected

        cleanup:
        oneSlot.resetWritePositionAfterBulkLoad()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test kvLRUHitTotal only counts true LRU hits not WAL hits'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def key = 'wal-vs-lru-metric-key'
        def sKey = BaseCommand.slot(key, slotNumber)
        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.compressedData = new byte[10]
        cv.expireAt = CompressedValue.NO_EXPIRE

        when: 'put to WAL, then read - this is a WAL hit, not LRU hit'
        oneSlot.kvLRUHitTotal = 0
        oneSlot.kvLRUMissTotal = 0
        oneSlot.kvLRUCvEncodedLengthTotal = 0
        oneSlot.kvWalHitTotal = 0
        oneSlot.kvWalCvEncodedLengthTotal = 0
        oneSlot.put(key, sKey.bucketIndex(), cv)
        oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.kvWalHitTotal == 1
        oneSlot.kvLRUHitTotal == 0

        when: 'second WAL hit via get()'
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.kvWalHitTotal == 2
        oneSlot.kvLRUHitTotal == 0

        when: 'clear WAL, put into LRU manually, then read - true LRU hit'
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()
        oneSlot.kvLRUHitTotal = 0
        oneSlot.kvLRUMissTotal = 0
        oneSlot.kvLRUCvEncodedLengthTotal = 0
        oneSlot.kvWalHitTotal = 0
        oneSlot.kvWalCvEncodedLengthTotal = 0
        oneSlot.putKvInTargetWalGroupIndexLRU(Wal.calcWalGroupIndex(sKey.bucketIndex()), key, cv.encode())
        oneSlot.getExpireAt(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.kvLRUHitTotal == 1
        oneSlot.kvWalHitTotal == 0

        when: 'true LRU hit via get()'
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        oneSlot.kvLRUHitTotal == 2
        oneSlot.kvWalHitTotal == 0

        when: 'verify collect() exports both WAL and LRU metrics'
        oneSlot.kvWalHitTotal = 5
        oneSlot.kvWalCvEncodedLengthTotal = 100
        oneSlot.metaChunkSegmentFlagSeq.markerSoftDropCountTotal = 2
        def metrics = oneSlot.collect()
        then:
        metrics['slot_kv_lru_hit_total'] == 2.0
        metrics['slot_kv_wal_hit_total'] == 5.0
        metrics['slot_kv_wal_cv_encoded_length_avg'] == 20.0
        metrics['segment_marker_soft_drop_count_total'] == 2.0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test flush clears kv lru cache so post-flush get returns null'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and: 'put many keys to trigger persist to key loader, then clear WAL so get reads from LRU'
        ConfForSlot.global.confWal.shortValueSizeTrigger = 100
        def bucketIndex0KeyList = batchPut(oneSlot, 300, 10, 0, slotNumber)
        def firstKey = bucketIndex0KeyList[0]
        def sFirstKey = BaseCommand.slot(firstKey, slotNumber)

        when: 'clear WAL so subsequent get will read from LRU cache (not WAL)'
        oneSlot.getWalByBucketIndex(0).clear()

        and: 'get the key to populate LRU cache from key loader'
        def resultBeforeFlush = oneSlot.get(firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash())
        then: 'key should be readable from LRU after being loaded from key loader'
        resultBeforeFlush != null

        when: 'flush the slot'
        oneSlot.flush()

        and: 'get the key after flush - should return null since slot data was cleared'
        def resultAfterFlush = oneSlot.get(firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash())

        then: 'post-flush get should return null (bug: currently returns stale LRU value)'
        resultAfterFlush == null

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test flush resets WAL write positions so post-flush writes are not lost on reload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and: 'set high triggers to avoid auto-persist during test'
        ConfForSlot.global.confWal.shortValueSizeTrigger = 100000
        ConfForSlot.global.confWal.valueSizeTrigger = 100000

        when: 'put keys via direct WAL access to build up write position'
        def wal = oneSlot.getWalByGroupIndex(0)
        def vList = Mock.prepareValueList(10, 0)
        wal.put(false, vList[0].key(), vList[0])
        wal.put(false, vList[1].key(), vList[1])
        def posAfterPuts = wal.writePosition
        then: 'write position should be non-zero after puts'
        posAfterPuts > 0

        when: 'flush the slot'
        oneSlot.flush()
        def posAfterFlush = wal.writePosition
        then: 'write position should be 0 after flush (bug: currently still non-zero)'
        posAfterFlush == 0

        cleanup:
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
        def bytesForMerge = oneSlot.readForMerge(0, 10)
        def bytesForRepl = oneSlot.readForRepl(0)
        then:
        bytesForMerge == null
        bytesForRepl == null

        when:
        def exception = false
        try {
            oneSlot.getSegmentMergeFlag(oneSlot.chunk.maxSegmentIndex + 1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        oneSlot.updateSegmentMergeFlag(0, Chunk.SEGMENT_FLAG_HAS_DATA, 1L)
        List<Long> segmentSeqList = [1L]
        oneSlot.setSegmentMergeFlagBatch(0, 1,
                Chunk.SEGMENT_FLAG_HAS_DATA, segmentSeqList, 0)
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
        def metaChunkSegmentFlagSeq = oneSlot.metaChunkSegmentFlagSeq

        def ext = new OneSlot.BeforePersistWalExtFromMerge([], [], [], -1)
        expect:
        ext.isEmpty()

        when:
        final int walGroupIndex = 0
        def e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        e == null

        when:
        metaChunkSegmentFlagSeq.isOverHalfSegmentNumberForFirstReuseLoop = true
        metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, 0, (short) 10)
        oneSlot.setSegmentMergeFlagBatch(0, 10, Chunk.SEGMENT_FLAG_HAS_DATA,
                (0..<10).collect { 1L }, walGroupIndex)
        e = oneSlot.readSomeSegmentsBeforePersistWal(walGroupIndex)
        then:
        e != null

        cleanup:
        oneSlot.cleanUp()
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

    def 'test interval delete overwrite big string files'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        def future = oneSlot.initCheck()
        future.get()

        when:
        oneSlot.intervalDeleteOverwriteBigStringFiles()
        then:
        oneSlot.deleteOverwriteBigStringFilesLastBucketIndex == 1

        when:
        oneSlot.deleteOverwriteBigStringFilesLastBucketIndex = oneSlot.keyLoader.bucketsPerSlot - 1
        oneSlot.delayToDeleteBigStringFileIds << new BigStringFiles.IdWithKey(1234L, 0, 1234L, "")
        oneSlot.intervalDeleteOverwriteBigStringFiles()
        then:
        oneSlot.deleteOverwriteBigStringFilesLastBucketIndex == 0

        when:
        oneSlot.bigStringFiles.writeBigStringBytes(1234L, 0, 1234L, '1234'.bytes)
        oneSlot.bigStringFiles.writeBigStringBytes(2345L, 0, 2345L, '2345'.bytes)
        oneSlot.bigStringFiles.bigStringUuidByKey.put('1234', 1234L)
        oneSlot.bigStringFiles.bigStringUuidSet.add(1234L)
        oneSlot.intervalDeleteOverwriteBigStringFiles()
        then:
        oneSlot.delayToDeleteBigStringFileIds.size() == 1

        when:
        oneSlot.bigStringFiles.deleteAllBigStringFiles()
        oneSlot.delayToDeleteBigStringFileIds.clear()
        oneSlot.bigStringFiles.writeBigStringBytes(1234L, 0, 1234L, '1234'.bytes)
        oneSlot.bigStringFiles.writeBigStringBytes(2345L, 0, 2345L, '2345'.bytes)
        oneSlot.bigStringFiles.writeBigStringBytes(3456L, 0, 3456L, '3456'.bytes)
        oneSlot.getWalByBucketIndex(0).clear()
        oneSlot.bigStringFiles.bigStringUuidByKey.put('2345', 2345L)
        oneSlot.bigStringFiles.bigStringUuidSet.add(2345L)
        def sKey = BaseCommand.slot('1234', slotNumber)
        def cv = new CompressedValue()
        cv.seq = 1234L
        cv.keyHash = sKey.keyHash()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(1234L, CompressedValue.NULL_DICT_SEQ)
        oneSlot.keyLoader.putValueByKey(0, '1234', sKey.keyHash(), 0L, 1234L, cv.encode())
        oneSlot.bigStringFiles.bigStringUuidByKey.put('1234', 1234L)
        oneSlot.bigStringFiles.bigStringUuidSet.add(1234L)
        oneSlot.deleteOverwriteBigStringFilesLastBucketIndex = 0
        oneSlot.intervalDeleteOverwriteBigStringFiles()
        then:
        oneSlot.delayToDeleteBigStringFileIds.size() == 1
        oneSlot.delayToDeleteBigStringFileIds.first.uuid() == 3456L

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test binlog not appended and needPutV not recovered when doPersist throws'() {
        given:
        def originalSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd
        def originalFdPerChunk = ConfForSlot.global.confChunk.fdPerChunk
        def originalValueSizeTrigger = ConfForSlot.global.confWal.valueSizeTrigger

        ConfForSlot.global.confChunk.segmentNumberPerFd = 8
        ConfForSlot.global.confChunk.fdPerChunk = (byte) 1
        // high trigger so persist is triggered by WAL buffer overflow, not value count
        ConfForSlot.global.confWal.valueSizeTrigger = 10000

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        // enable binlog so assertions are non-vacuous
        oneSlot.dynConfig.setBinlogOn(true)

        and:
        def bucketIndex = 0
        def targetKeyList = Mock.prepareTargetBucketIndexKeyList(100, bucketIndex)
        def random = new Random()

        // do some successful writes to fill segments
        for (int i = 0; i < 50; i++) {
            def key = targetKeyList[i]
            def cv = new CompressedValue()
            cv.keyHash = KeyHash.hash(key.bytes)
            cv.compressedData = new byte[100]
            random.nextBytes(cv.compressedData)
            cv.seq = oneSlot.snowFlake.nextId()
            oneSlot.put(key, bucketIndex, cv)
        }

        // record binlog offset before failure — should be > 0 since successful writes were binlogged
        def binlogOffsetBefore = oneSlot.binlog.currentReplOffset()
        println "Binlog on=${oneSlot.dynConfig.isBinlogOn()}, offset before failure writes=${binlogOffsetBefore}"

        // mark ALL segments as HAS_DATA (non-reusable) so next persist will fail
        def maxSegIndex = oneSlot.chunk.maxSegmentIndex
        for (int i = 0; i <= maxSegIndex; i++) {
            oneSlot.setSegmentMergeFlag(i, Chunk.SEGMENT_FLAG_HAS_DATA, 0L, 0)
        }
        def flagSeq = oneSlot.metaChunkSegmentFlagSeq
        for (def bitSet : flagSeq.segmentCanReuseBitSet) {
            bitSet.clear()
        }

        def wal = oneSlot.getWalByGroupIndex(0)

        when:
        // fill WAL buffer until it overflows — the overflowing put returns needPutV != null
        // then doPersist() is called and putValueToWal() throws SegmentOverflowException
        boolean exceptionThrown = false
        String failedKey = null
        long binlogOffsetRightBeforeFail = 0L
        for (int i = 50; i < 100; i++) {
            def key = targetKeyList[i]
            def cv = new CompressedValue()
            cv.keyHash = KeyHash.hash(key.bytes)
            cv.compressedData = new byte[2000]
            random.nextBytes(cv.compressedData)
            cv.seq = oneSlot.snowFlake.nextId()

            // capture offset before each attempt
            binlogOffsetRightBeforeFail = oneSlot.binlog.currentReplOffset()

            try {
                oneSlot.put(key, bucketIndex, cv)
            } catch (SegmentOverflowException e) {
                exceptionThrown = true
                failedKey = key
                println "SegmentOverflowException caught for key=${key}, binlog offset before this write=${binlogOffsetRightBeforeFail}"
                break
            }
        }

        then:
        exceptionThrown
        failedKey != null

        // the failed write is NOT in the WAL delay maps (not recovered)
        !wal.delayToKeyBucketValues.containsKey(failedKey)
        !wal.delayToKeyBucketShortValues.containsKey(failedKey)

        // binlog was NOT appended for the failed write — offset should be the same as
        // right before the failing put (since the failing put throws before appending)
        def binlogOffsetAfter = oneSlot.binlog.currentReplOffset()
        binlogOffsetAfter == binlogOffsetRightBeforeFail

        cleanup:
        ConfForSlot.global.confChunk.segmentNumberPerFd = originalSegmentNumberPerFd
        ConfForSlot.global.confChunk.fdPerChunk = originalFdPerChunk
        ConfForSlot.global.confWal.valueSizeTrigger = originalValueSizeTrigger
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test merge marker survives failed persist and consumed after success'() {
        given:
        def originalSegmentNumberPerFd = ConfForSlot.global.confChunk.segmentNumberPerFd
        def originalFdPerChunk = ConfForSlot.global.confChunk.fdPerChunk
        def originalValueSizeTrigger = ConfForSlot.global.confWal.valueSizeTrigger

        // small segment count so writes fill segments deterministically
        ConfForSlot.global.confChunk.segmentNumberPerFd = 8
        ConfForSlot.global.confChunk.fdPerChunk = (byte) 1
        // low trigger so each batch triggers doPersist → chunk.persist → HAS_DATA + auto-markers
        ConfForSlot.global.confWal.valueSizeTrigger = 10

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def metaChunkSegmentFlagSeq = oneSlot.metaChunkSegmentFlagSeq

        and:
        def walGroupIndex = 0
        def wal = oneSlot.getWalByGroupIndex(walGroupIndex)
        def bucketIndex = 0
        def random = new Random()

        // write non-short data — each batch of 10 triggers persist, creating HAS_DATA segments
        def targetKeyList = Mock.prepareTargetBucketIndexKeyList(50, bucketIndex)
        for (int i = 0; i < 50; i++) {
            def key = targetKeyList[i]
            def cv = new CompressedValue()
            cv.keyHash = KeyHash.hash(key.bytes)
            cv.compressedData = new byte[100]
            random.nextBytes(cv.compressedData)
            cv.seq = oneSlot.snowFlake.nextId()
            oneSlot.put(key, bucketIndex, cv)
        }

        // ensure merge loop is enabled
        metaChunkSegmentFlagSeq.isOverHalfSegmentNumberForFirstReuseLoop = true

        // clear all auto-markers created by chunk.persist
        while (true) {
            def result = metaChunkSegmentFlagSeq.findThoseNeedToMerge(walGroupIndex)
            if (result[0] == -1) break
            metaChunkSegmentFlagSeq.commitMergedRangeWithMarkerIdx(walGroupIndex, result[0], result[1], result[2])
        }
        assert metaChunkSegmentFlagSeq.countMarkersForWalGroup(walGroupIndex) == 0

        // find first HAS_DATA segment for our manual marker
        def maxSegIndex = oneSlot.chunk.maxSegmentIndex
        int firstDataSeg = -1
        for (int i = 0; i <= maxSegIndex; i++) {
            def flag = metaChunkSegmentFlagSeq.getSegmentMergeFlag(i)
            if (flag.flagByte() == Chunk.SEGMENT_FLAG_HAS_DATA && flag.walGroupIndex() == walGroupIndex) {
                firstDataSeg = i
                break
            }
        }
        assert firstDataSeg >= 0: "Expected HAS_DATA segment, found none"

        // add ONE manual marker for a single segment
        metaChunkSegmentFlagSeq.markPersistedSegmentIndexToTargetWalGroup(walGroupIndex, firstDataSeg, (short) 1)
        assert metaChunkSegmentFlagSeq.countMarkersForWalGroup(walGroupIndex) == 1

        when: 'call putValueToWal with no reusable segments — should fail'
        for (def bitSet : metaChunkSegmentFlagSeq.segmentCanReuseBitSet) {
            bitSet.clear()
        }

        def cv1 = new CompressedValue()
        cv1.keyHash = KeyHash.hash('marker-fail-key'.bytes)
        cv1.compressedData = new byte[200]
        random.nextBytes(cv1.compressedData)
        cv1.seq = oneSlot.snowFlake.nextId()
        wal.delayToKeyBucketValues.put('marker-fail-key',
                new Wal.V(cv1.seq, bucketIndex, cv1.getKeyHash(), cv1.getExpireAt(),
                        cv1.getDictSeqOrSpType(), 'marker-fail-key', cv1.encode(), false))

        boolean persistThrew = false
        try {
            oneSlot.putValueToWal(false, wal)
        } catch (SegmentOverflowException e) {
            persistThrew = true
        }

        then: 'persist threw and our manual marker still exists'
        persistThrew
        metaChunkSegmentFlagSeq.countMarkersForWalGroup(walGroupIndex) >= 1

        when: 'restore reuse bitsets so putValueToWal can succeed'
        for (int i = 0; i <= maxSegIndex; i++) {
            metaChunkSegmentFlagSeq.segmentCanReuseBitSet[0].set(i)
        }

        boolean successPersist = false
        try {
            oneSlot.putValueToWal(false, wal)
            successPersist = true
        } catch (SegmentOverflowException e) {
            println "Second putValueToWal still failed: ${e.message}"
        }

        then: 'persist succeeded'
        successPersist

        and: 'our manual marker was consumed by commitMergedRange (new auto-marker may exist but total <= 1)'
        // chunk.persist auto-creates a new marker for the new segments, so total might be 1
        // the key invariant: our original marker was consumed, and at most 1 new auto-marker exists
        def finalMarkerCount = metaChunkSegmentFlagSeq.countMarkersForWalGroup(walGroupIndex)
        finalMarkerCount <= 1

        cleanup:
        ConfForSlot.global.confChunk.segmentNumberPerFd = originalSegmentNumberPerFd
        ConfForSlot.global.confChunk.fdPerChunk = originalFdPerChunk
        ConfForSlot.global.confWal.valueSizeTrigger = originalValueSizeTrigger
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test overwrite big string when WAL buffer full schedules old file for deletion'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'kerry-test-big-string-overwrite-buffer-full-key'

        and: 'write first big string value'
        def cv1 = new CompressedValue()
        cv1.seq = oneSlot.snowFlake.nextId()
        cv1.keyHash = 333333333L
        cv1.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv1)
        def firstUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)
        assert firstUuid != null

        and: 'manipulate WAL state so next short-value put triggers buffer-full early return'
        def wal = oneSlot.getWalByBucketIndex(bucketIndex)
        wal.writePositionShortValue = Wal.ONE_GROUP_BUFFER_SIZE - 10
        wal.isOnRewrite = false

        when: 'overwrite with second big string — triggers needPersist=true (buffer full)'
        def cv2 = new CompressedValue()
        cv2.seq = oneSlot.snowFlake.nextId()
        cv2.keyHash = 333333333L
        cv2.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv2)

        then: 'old big-string file should be scheduled for deletion'
        !oneSlot.delayToDeleteBigStringFileIds.isEmpty()
        oneSlot.delayToDeleteBigStringFileIds.any { it.uuid() == firstUuid }

        and: 'new UUID should be in the WAL map'
        def currentUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)
        currentUuid != null
        currentUuid != firstUuid

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test put big string with WAL buffer full and doPersist throw enqueues new uuid for cleanup'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'kerry-test-big-string-orphan-on-dopersist-throw'

        and: 'write first big string value so the overwrite snapshot is not null'
        def cv1 = new CompressedValue()
        cv1.seq = oneSlot.snowFlake.nextId()
        cv1.keyHash = 666666666L
        cv1.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv1)
        def firstUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)
        assert firstUuid != null
        // drain pending delete so we can count delta for the new uuid
        oneSlot.delayToDeleteBigStringFileIds.clear()

        and: 'manipulate WAL state so next short-value put triggers buffer-full early return'
        def wal = oneSlot.getWalByBucketIndex(bucketIndex)
        wal.writePositionShortValue = Wal.ONE_GROUP_BUFFER_SIZE - 10
        wal.isOnRewrite = false

        and: 'force doPersist to throw — simulates BucketFullException or other bucket persist failure'
        oneSlot.doPersistForceThrowForTest = true

        when: 'put a second big string value; needPutV != null path; doPersist throws'
        def cv2 = new CompressedValue()
        cv2.seq = oneSlot.snowFlake.nextId()
        cv2.keyHash = 666666666L
        cv2.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cv2)

        then: 'RuntimeException propagates out of put'
        def thrown = thrown(RuntimeException)
        thrown.message.contains('doPersist forced throw for test')

        and: 'the new big-string file uuid is enqueued for deletion (size delta == +1)'
        oneSlot.delayToDeleteBigStringFileIds.size() == 1
        def newIdEntry = oneSlot.delayToDeleteBigStringFileIds.peekFirst()
        newIdEntry.uuid() != firstUuid
        newIdEntry.bucketIndex() == bucketIndex
        newIdEntry.keyHash() == 666666666L
        newIdEntry.key() == key

        and: 'the orphan big-string file is on disk and exists at the expected path'
        new File(oneSlot.bigStringFiles.bigStringDir, bucketIndex + '/' + newIdEntry.uuid() + '_' + 666666666L).exists()

        and: 'the new uuid was NOT promoted into the BigStringFiles UUID map (WAL did not accept v)'
        oneSlot.bigStringFiles.bigStringUuidByKey.get(key) == firstUuid

        cleanup:
        oneSlot.doPersistForceThrowForTest = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test interval delete re-enqueues big string id when delete returns false'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def keyHash = 777777777L

        and: 'write a big string file on disk so delete is even attempted'
        def orphanUuid = 8001L
        oneSlot.bigStringFiles.writeBigStringBytes(orphanUuid, bucketIndex, keyHash, 'orphan-bytes'.bytes)
        assert new File(oneSlot.bigStringFiles.bigStringDir, bucketIndex + '/' + orphanUuid + '_' + keyHash).exists()

        and: 'enqueue the id for deletion and force the underlying delete to return false'
        oneSlot.delayToDeleteBigStringFileIds.clear()
        def queuedId = new BigStringFiles.IdWithKey(orphanUuid, bucketIndex, keyHash, 'kerry-test-big-string-stuck-on-delete-fail')
        oneSlot.delayToDeleteBigStringFileIds.add(queuedId)
        oneSlot.bigStringFiles.deleteForceReturnFalseForTest = true

        when: 'one interval tick processes the queue'
        oneSlot.intervalDeleteOverwriteBigStringFiles(bucketIndex)

        then: 'the original queued id with its key is preserved (re-enqueued at the head) — fix must not drop it'
        oneSlot.delayToDeleteBigStringFileIds.size() >= 1
        def first = oneSlot.delayToDeleteBigStringFileIds.peekFirst()
        first.uuid() == orphanUuid
        first.bucketIndex() == bucketIndex
        first.keyHash() == keyHash
        first.key() == 'kerry-test-big-string-stuck-on-delete-fail'

        and: 'underlying file is still on disk (delete was forced to return false)'
        new File(oneSlot.bigStringFiles.bigStringDir, bucketIndex + '/' + orphanUuid + '_' + keyHash).exists()

        cleanup:
        oneSlot.bigStringFiles.deleteForceReturnFalseForTest = false
        // re-attempt delete for real so the file does not leak into the next test
        oneSlot.bigStringFiles.deleteBigStringFileIfExist(orphanUuid, bucketIndex, keyHash)
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test get reads TIGHT segment correctly after isSegmentUseCompression flipped to false'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and: 'enable segment compression'
        ConfForSlot.global.confChunk.isSegmentUseCompression = true

        when: 'put a value large enough to go to segment storage (not short value)'
        def key = 'tight-segment-config-flip-key'
        def sKey = BaseCommand.slot(key, slotNumber)
        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.expireAt = CompressedValue.NO_EXPIRE

        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).isOnRewrite = false
        100.times {
            cv.seq = oneSlot.snowFlake.nextId()
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()

        then: 'value persisted as PVM (segment), get reads it'
        def result1 = oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        result1 != null

        when: 'verify it is a PVM entry'
        def keyLoader = oneSlot.keyLoader
        def vx = keyLoader.getValueXByKey(sKey.bucketIndex(), key, sKey.keyHash())
        then:
        vx != null
        PersistValueMeta.isPvm(vx.valueBytes())

        when: 'flip config: compression off, but on-disk segment is TIGHT'
        ConfForSlot.global.confChunk.isSegmentUseCompression = false

        and: 'clear LRU so get must re-read from segment'
        def walGroupIndex = Wal.calcWalGroupIndex(sKey.bucketIndex())
        oneSlot.clearKvInLRU(walGroupIndex)

        and: 'get the key — should use data marker, not config flag, to detect TIGHT'
        def result2 = oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        result2 != null
        result2.cv() != null
        result2.cv().seq > 0
        result2.cv().compressedData.length == CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100

        cleanup:
        ConfForSlot.global.confChunk.isSegmentUseCompression = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test get throws PersistValueMetaCorruptedException when PVM bytes are corrupted in key bucket'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def key = 'corrupt-pvm-test-key'
        def sKey = BaseCommand.slot(key, slotNumber)

        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.expireAt = CompressedValue.NO_EXPIRE
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).isOnRewrite = false
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()

        and:
        def keyLoader = oneSlot.keyLoader
        def vx = keyLoader.getValueXByKey(sKey.bucketIndex(), key, sKey.keyHash())
        assert vx != null
        assert PersistValueMeta.isPvm(vx.valueBytes())

        and: 'corrupt subBlockIndex to -1 in the stored PVM bytes'
        def corruptedBytes = vx.valueBytes()
        corruptedBytes[3] = (byte) -1
        keyLoader.putValueByKey(sKey.bucketIndex(), key, sKey.keyHash(), vx.expireAt(), vx.seq(), corruptedBytes)

        and: 'clear LRU so get must read from the key bucket'
        oneSlot.clearKvInLRU(Wal.calcWalGroupIndex(sKey.bucketIndex()))

        when:
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('subBlockIndex')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test get throws PersistValueMetaCorruptedException when segmentIndex exceeds max'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def key = 'corrupt-segment-index-key'
        def sKey = BaseCommand.slot(key, slotNumber)

        def cv = new CompressedValue()
        cv.keyHash = sKey.keyHash()
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN * 100]
        cv.expireAt = CompressedValue.NO_EXPIRE
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).isOnRewrite = false
        100.times {
            oneSlot.put(key, sKey.bucketIndex(), cv)
        }
        oneSlot.getWalByBucketIndex(sKey.bucketIndex()).clear()

        and:
        def keyLoader = oneSlot.keyLoader
        def vx = keyLoader.getValueXByKey(sKey.bucketIndex(), key, sKey.keyHash())
        assert vx != null
        assert PersistValueMeta.isPvm(vx.valueBytes())

        and: 'corrupt segmentIndex to Integer.MAX_VALUE (0x7FFFFFFF) in the stored PVM bytes'
        def corruptedBytes = vx.valueBytes()
        corruptedBytes[4] = (byte) 0x7F
        corruptedBytes[5] = (byte) 0xFF
        corruptedBytes[6] = (byte) 0xFF
        corruptedBytes[7] = (byte) 0xFF
        keyLoader.putValueByKey(sKey.bucketIndex(), key, sKey.keyHash(), vx.expireAt(), vx.seq(), corruptedBytes)

        and: 'clear LRU so get must read from the key bucket'
        oneSlot.clearKvInLRU(Wal.calcWalGroupIndex(sKey.bucketIndex()))

        when:
        oneSlot.get(key, sKey.bucketIndex(), sKey.keyHash())
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('segmentIndex')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test tombstoning unpersisted big string enqueues file for deletion'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def key = 'kerry-test-big-string-del-before-persist'
        def s = BaseCommand.slot(key, slotNumber)
        def keyHash = s.keyHash()

        when: 'put a big-string value that stays in WAL (not yet persisted to key bucket)'
        def cvBig = new CompressedValue()
        cvBig.seq = oneSlot.snowFlake.nextId()
        cvBig.keyHash = keyHash
        cvBig.compressedData = new byte[oneSlot.chunk.chunkSegmentLength]
        oneSlot.put(key, bucketIndex, cvBig)
        def bigStringUuid = oneSlot.bigStringFiles.bigStringUuidByKey.get(key)

        then: 'big-string uuid was recorded in WAL'
        bigStringUuid != null

        when: 'delete the key before the first persist'
        oneSlot.delayToDeleteBigStringFileIds.clear()
        oneSlot.removeDelay(key, bucketIndex, keyHash)

        then: 'the big-string file uuid should be enqueued for deletion'
        !oneSlot.delayToDeleteBigStringFileIds.isEmpty()
        oneSlot.delayToDeleteBigStringFileIds.any { it.uuid() == bigStringUuid }

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test doTask does not propagate exception from wal interval delete expired big string files'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bucketIndex = 0
        def keyHash = 888888888L
        def key = 'kerry-test-do-task-wal-expired-bs'

        and: 'write a big-string file so the delete path is reached'
        def bigStringUuid = 9001L
        oneSlot.bigStringFiles.writeBigStringBytes(bigStringUuid, bucketIndex, keyHash, 'do-task-wal-bytes'.bytes)

        and: 'inject an expired big-string entry into the first WAL short values'
        def cv = new CompressedValue()
        cv.seq = oneSlot.snowFlake.nextId()
        cv.keyHash = keyHash
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(bigStringUuid, CompressedValue.NULL_DICT_SEQ)
        def expiredV = new Wal.V(cv.seq, bucketIndex, keyHash,
                System.currentTimeMillis() - 60_000L,
                CompressedValue.SP_TYPE_BIG_STRING,
                key, cv.encode(), false)
        def wal = oneSlot.getWalByGroupIndex(0)
        wal.delayToKeyBucketShortValues.put(key, expiredV)
        oneSlot.bigStringFiles.bigStringUuidByKey.put(key, bigStringUuid)
        oneSlot.bigStringFiles.bigStringUuidSet.add(bigStringUuid)

        and: 'force the underlying delete to return false so handleWhenCvExpiredOrDeleted throws'
        oneSlot.bigStringFiles.deleteForceReturnFalseForTest = true

        when: 'doTask is called with loopCount=0 (loopCount % 10 == 0 fires the 100ms tick, walArray[0] is the WAL group we populated)'
        oneSlot.doTask(0)

        then: 'no RuntimeException propagates out of doTask'
        noExceptionThrown()

        cleanup:
        oneSlot.bigStringFiles.deleteForceReturnFalseForTest = false
        // re-attempt delete for real so the file does not leak into the next test
        oneSlot.bigStringFiles.deleteBigStringFileIfExist(bigStringUuid, bucketIndex, keyHash)
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test doTask does not propagate exception from interval delete overwrite big string files'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and: 'force intervalDeleteOverwriteBigStringFiles to throw so the doTask catch is exercised'
        oneSlot.intervalDeleteOverwriteBigStringFilesForceThrowForTest = true

        when: 'doTask is called with loopCount=0 (fires the 10ms tick intervalDeleteOverwriteBigStringFiles)'
        oneSlot.doTask(0)

        then: 'no exception propagates from intervalDeleteOverwriteBigStringFiles'
        noExceptionThrown()

        cleanup:
        oneSlot.intervalDeleteOverwriteBigStringFilesForceThrowForTest = false
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
