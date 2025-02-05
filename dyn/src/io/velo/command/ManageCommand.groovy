package io.velo.command

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import io.activej.config.Config
import io.activej.promise.Promise
import io.activej.promise.Promises
import io.activej.promise.SettablePromise
import io.velo.*
import io.velo.persist.Chunk
import io.velo.persist.LocalPersist
import io.velo.persist.OneSlot
import io.velo.persist.Wal
import io.velo.repl.cluster.MultiShard
import io.velo.repl.incremental.XOneWalGroupPersist
import io.velo.repl.support.JedisPoolHolder
import io.velo.reply.*
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import org.apache.commons.io.FileUtils
import org.jetbrains.annotations.VisibleForTesting
import org.slf4j.LoggerFactory

import static io.velo.TrainSampleJob.MIN_TRAIN_SAMPLE_SIZE

@CompileStatic
class ManageCommand extends BaseCommand {
    ManageCommand() {
        super(null, null, null)
    }

    ManageCommand(MGroup mGroup) {
        super(mGroup.cmd, mGroup.data, mGroup.socket)
    }

    ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> r = []

        if (data.length < 2) {
            return r
        }

        def subCmd = new String(data[1]).toLowerCase()
        // manage slot 0 bucket 0 view-key-count
        if (subCmd == 'slot') {
            if (data.length < 5) {
                return r
            }

            def slotBytes = data[2]
            short slot
            try {
                slot = Short.parseShort(new String(slotBytes))
            } catch (NumberFormatException ignored) {
                return r
            }

            def subSubCmd = new String(data[3]).toLowerCase()
            if (subSubCmd == 'migrate_from') {
                // given slot is to client slot, change to inner slot
                // use can use the right event loop
                slot = MultiShard.asInnerSlotByToClientSlot(slot)
            }

            r.add(new SlotWithKeyHash(slot, 0, 0L, 0))
            return r
        }

        r
    }

    @Override
    Reply handle() {
        if (data.length < 2) {
            return ErrorReply.FORMAT
        }

        def subCmd = new String(data[1]).toLowerCase()

        // cross slots
        if (subCmd == 'debug') {
            return debug()
        }

        // cross slots
        if (subCmd == 'dyn-config') {
            return dynConfig()
        }

        // cross slots
        if (subCmd == 'dict') {
            return dict()
        }

        if (subCmd == 'index') {
            return manageIndex()
        }

        // given slot
        if (subCmd == 'slot') {
            return manageInOneSlot()
        }

        return NilReply.INSTANCE
    }

    Reply manageInOneSlot() {
        if (data.length < 4) {
            return ErrorReply.FORMAT
        }

        def slotBytes = data[2]
        short slot

        try {
            slot = Short.parseShort(new String(slotBytes))
        } catch (NumberFormatException ignored) {
            return ErrorReply.INVALID_INTEGER
        }

        def tmpSubSubCmd = new String(data[3]).toLowerCase()
        if (tmpSubSubCmd == 'migrate_from') {
            def toClientSlot = slot
            if (toClientSlot >= MultiShard.TO_CLIENT_SLOT_NUMBER) {
                return ErrorReply.INVALID_INTEGER
            }
            return migrateFrom()
        }

        if (slot >= slotNumber) {
            return ErrorReply.INVALID_INTEGER
        }

        int bucketIndex = -1

        int subSubCmdIndex = 3
        def isInspectBucket = 'bucket' == new String(data[3]).toLowerCase()
        if (isInspectBucket) {
            def bucketIndexBytes = data[4]

            try {
                bucketIndex = Integer.parseInt(new String(bucketIndexBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            subSubCmdIndex = 5

            if (data.length < 6) {
                return ErrorReply.FORMAT
            }
        }

        def oneSlot = localPersist.oneSlot(slot)

        def subSubCmd = new String(data[subSubCmdIndex]).toLowerCase()
        if (subSubCmd == 'view-metrics') {
            // http url: ?manage&slot&0&view-metrics
            def all = oneSlot.collect()

            // prometheus format
            def sb = new StringBuilder()
            all.each { k, v ->
                sb << k << '{slot="' << slot << '",} ' << v << '\n'
            }
            return new BulkReply(sb.toString().bytes)
        } else if (subSubCmd == 'view-big-key-top-k') {
            def queue = oneSlot.bigKeyTopK.queue
            def str = queue.collect {
                new String(it.keyBytes()) + ': ' + it.length()
            }.join(', ')
            return new BulkReply(str.bytes)
        } else if (subSubCmd == 'view-bucket-key-count') {
            // manage slot 0 view-bucket-key-count
            // manage slot 0 bucket 0 view-bucket-key-count
            def keyCount = bucketIndex == -1 ? oneSlot.getAllKeyCount() : oneSlot.keyLoader.getKeyCountInBucketIndex(bucketIndex)
            return new IntegerReply(keyCount)
        } else if (subSubCmd == 'view-bucket-keys') {
            // manage slot 0 bucket 0 view-bucket-keys [iterate]
            def isIterate = data.length == subSubCmdIndex + 2 && new String(data[data.length - 1]).toLowerCase() == 'iterate'

            // if not set bucket index, default 0
            if (bucketIndex == -1) {
                bucketIndex = 0
            }

            def keyBuckets = oneSlot.keyLoader.readKeyBuckets(bucketIndex)
            String str
            if (!isIterate) {
                str = keyBuckets.collect { it == null ? 'Null' : it.toString() }.join(',')
            } else {
                def sb = new StringBuilder()
                for (kb in keyBuckets) {
                    if (kb == null) {
                        continue
                    }
                    kb.iterate { keyHash, expireAt, seq, keyBytes, valueBytes ->
                        sb << new String(keyBytes) << ','
                    }
                }
                str = sb.toString()
            }

            return new BulkReply(str.bytes)
        } else if (subSubCmd == 'update-kv-lru-max-size') {
            // manage slot 0 update-kv-lru-max-size 100
            if (data.length != 5) {
                return ErrorReply.FORMAT
            }

            def lruMaxSizeBytes = data[4]

            int lruMaxSize

            try {
                lruMaxSize = Integer.parseInt(new String(lruMaxSizeBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            ConfForSlot.global.lruKeyAndCompressedValueEncoded.maxSize = lruMaxSize
            oneSlot.initLRU(true)

            return OKReply.INSTANCE
        } else if (subSubCmd == 'view-in-memory-size-estimate') {
            def sb = new StringBuilder()
            def all = oneSlot.estimate(sb)
            def replies = new Reply[2]
            replies[0] = new IntegerReply(all)
            replies[1] = new BulkReply(sb.toString().bytes)
            return new MultiBulkReply(replies)
        } else if (subSubCmd == 'output-chunk-segment-flag-to-file') {
            // manage slot 0 output-chunk-segment-flag-to-file 0 1024
            if (data.length != 6) {
                return ErrorReply.FORMAT
            }

            def beginSegmentIndexBytes = data[4]
            def segmentCountBytes = data[5]
            int beginSegmentIndex
            int segmentCount

            try {
                beginSegmentIndex = Integer.parseInt(new String(beginSegmentIndexBytes))
                segmentCount = Integer.parseInt(new String(segmentCountBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            if (beginSegmentIndex < 0) {
                return ErrorReply.SYNTAX
            }

            def maxSegmentNumber = ConfForSlot.global.confChunk.maxSegmentNumber()
            if (beginSegmentIndex >= maxSegmentNumber) {
                return new ErrorReply('begin segment index need less than ' + maxSegmentNumber)
            }

            def isIterateAll = segmentCount <= 0

            def outputDir = new File(oneSlot.slotDir, 'debug')
            FileUtils.forceMkdir(outputDir)

            def beginNT = System.nanoTime()
            if (isIterateAll) {
                final String outputFileName = 'chunk_segment_flag.txt'
                new File(outputDir, outputFileName).withWriter { writer ->
                    writer.writeLine Chunk.Flag.values().collect { it.name() + ':' + it.flagByte() }.join(',')
                    oneSlot.metaChunkSegmentFlagSeq.iterateAll { segmentIndex, flagByte, seq, walGroupIndex ->
                        writer.writeLine("$segmentIndex, $flagByte, $seq, $walGroupIndex")
                    }
                }
            } else {
                final String outputFileName = 'chunk_segment_flag_range.txt'
                new File(outputDir, outputFileName).withWriter { writer ->
                    writer.writeLine Chunk.Flag.values().collect { it.name() + ':' + it.flagByte() }.join(',')
                    oneSlot.metaChunkSegmentFlagSeq.iterateRange(beginSegmentIndex, segmentCount) { segmentIndex, flagByte, seq, walGroupIndex ->
                        writer.writeLine("$segmentIndex, $flagByte, $seq, $walGroupIndex")
                    }
                }
            }
            def costNT = System.nanoTime() - beginNT
            log.info 'Output chunk segment flag to file cost {}us', costNT / 1000

            return OKReply.INSTANCE
        } else if (subSubCmd == 'set-readonly') {
            oneSlot.readonly = true
            return new BulkReply(('slot ' + slot + ' set readonly').bytes)
        } else if (subSubCmd == 'set-not-readonly') {
            oneSlot.readonly = false
            return new BulkReply(('slot ' + slot + ' set not readonly').bytes)
        } else if (subSubCmd == 'set-can-read') {
            oneSlot.canRead = true
            return new BulkReply(('slot ' + slot + ' set can read').bytes)
        } else if (subSubCmd == 'set-not-can-read') {
            oneSlot.canRead = false
            return new BulkReply(('slot ' + slot + ' set not can read').bytes)
        } else if (subSubCmd == 'key-buckets-warm-up') {
            def n = oneSlot.warmUp()
            return new IntegerReply(n)
        } else if (subSubCmd == 'mock-data') {
            if (data.length != 7) {
                return ErrorReply.FORMAT
            }

            // eg. manage slot 0 mock-data n=1000000 k=16 d=200
            final String keyPrefix = 'key:'

            int n
            int k
            int d
            try {
                n = Integer.parseInt(new String(data[4]).split('=')[1])
                k = Integer.parseInt(new String(data[5]).split('=')[1])
                d = Integer.parseInt(new String(data[6]).split('=')[1])
            } catch (NumberFormatException ignored) {
                return ErrorReply.INVALID_INTEGER
            }

            final int redisBenchmarkKeyLength = 16
            if (k < redisBenchmarkKeyLength) {
                return new ErrorReply('k must be greater than ' + redisBenchmarkKeyLength)
            }

            int skipN
            int putN
            long costT

            def mockValue = 'x' * d
            def mockValueBytes = mockValue.bytes

            int updateKeyBucketsAndChunkSegmentsDirectlyMinKeyNumber = 100_000_000
            // if nn, in big batch
            def isNN = 'nn' == new String(data[4]).split('=')[0]
            if (isNN) {
                updateKeyBucketsAndChunkSegmentsDirectlyMinKeyNumber = n
            }

            if (n >= updateKeyBucketsAndChunkSegmentsDirectlyMinKeyNumber && !ConfForSlot.global.confChunk.isSegmentUseCompression) {
                // s list size may be = 32 * 48 / 2 = 768
                def batchDone = setBigBatchKeyValues(n, keyPrefix, mockValueBytes, k, oneSlot)
                putN = batchDone.putN
                skipN = batchDone.skipN
                costT = batchDone.costT

                def batchDone2 = setBatchKeyValues((n / 10).intValue(), keyPrefix, mockValueBytes, k, slot)
                putN += batchDone2.putN
                skipN += batchDone2.skipN
                costT += batchDone2.costT
            } else {
                def batchDone = setBatchKeyValues(n, keyPrefix, mockValueBytes, k, slot)
                putN = batchDone.putN
                skipN = batchDone.skipN
                costT = batchDone.costT
            }

            return new BulkReply(('slot ' + slot + ' mock-data, putN=' + putN + ', skipN=' + skipN + ', costT=' + costT + 'ms').bytes)
        }

        return ErrorReply.SYNTAX
    }

    @TupleConstructor
    private class BatchDone {
        int putN
        int skipN
        long costT
    }

    private BatchDone setBigBatchKeyValues(int n, String keyPrefix, byte[] mockValueBytes, int keyLength, OneSlot oneSlot) {
        TreeMap<Integer, List<SlotWithKeyHash>> sListGroupByBucketIndex = new TreeMap<>()

        int skipN = 0
        int putN = 0
        def beginT = System.currentTimeMillis()
        for (int i = 0; i < n; i++) {
            def key = keyPrefix + i.toString().padLeft(keyLength - keyPrefix.length(), '0')
            def keyBytes = key.bytes
            def s = super.slot(keyBytes)
            if (s.slot() != oneSlot.slot()) {
                skipN++
                continue
            }

            sListGroupByBucketIndex.computeIfAbsent(s.bucketIndex(), k -> []).add(s)
            putN++
        }
        def costT = System.currentTimeMillis() - beginT
        log.warn 'Manage mock-data, set big batch key values, group by bucket index, slot={}, putN={}, skipN={}, costT={}ms',
                oneSlot.slot(), putN, skipN, costT

        beginT = System.currentTimeMillis()
        def walGroupNumber = Wal.calcWalGroupNumber()
        for (walGroupIndex in 0..<walGroupNumber) {
            List<SlotWithKeyHash> sList = []

            def beginBucketIndex = walGroupIndex * ConfForSlot.global.confWal.oneChargeBucketNumber
            for (j in 0..<ConfForSlot.global.confWal.oneChargeBucketNumber) {
                def bucketIndex = beginBucketIndex + j
                def sListInBucket = sListGroupByBucketIndex.get(bucketIndex)
                if (sListInBucket != null) {
                    sList.addAll(sListInBucket)
                }
            }

            if (walGroupIndex % 1024 == 0) {
                log.info 'Manage mock-data, set big batch key values, slot={}, wal group index={}, begin bucket index={}, s list size={}',
                        oneSlot.slot(), walGroupIndex, beginBucketIndex, sList.size()
            }

            if (sList.isEmpty()) {
                log.warn 'Manage mock-data, set big batch key values, slot={}, wal group index={}, begin bucket index={}, skip',
                        oneSlot.slot(), walGroupIndex, beginBucketIndex
                continue
            }

            ArrayList<Wal.V> vList = []
            for (s in sList) {
                def seq = oneSlot.snowFlake.nextId()
                def cv = new CompressedValue()
                cv.seq = seq
                cv.keyHash = s.keyHash()
                cv.compressedData = mockValueBytes
                cv.compressedLength = mockValueBytes.length
                vList.add(new Wal.V(seq, s.bucketIndex(), s.keyHash(),
                        CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                        s.rawKey(), cv.encode(), false))
            }

            def xForBinlog = new XOneWalGroupPersist(true, false, 0)
            oneSlot.chunk.persist(walGroupIndex, vList, false, xForBinlog, null)
        }
        costT += System.currentTimeMillis() - beginT
        log.warn 'Manage mock-data, set big batch key values, slot={}, costT={}ms', oneSlot.slot(), costT

        new BatchDone(putN, skipN, costT)
    }

    private BatchDone setBatchKeyValues(int n, String keyPrefix, byte[] mockValueBytes, int keyLength, int toSlot) {
        int skipN = 0
        int putN = 0
        def beginT = System.currentTimeMillis()
        for (int i = 0; i < n; i++) {
            def key = keyPrefix + i.toString().padLeft(keyLength - keyPrefix.length(), '0')
            def keyBytes = key.bytes
            def s = super.slot(keyBytes)
            if (s.slot() != toSlot) {
                skipN++
                continue
            }

            set(keyBytes, mockValueBytes, s)
            putN++
        }
        def costT = System.currentTimeMillis() - beginT
        log.warn 'Manage mock-data, set batch key values, slot={}, putN={}, skipN={}, costT={}ms', toSlot, putN, skipN, costT

        new BatchDone(putN, skipN, costT)
    }

    @VisibleForTesting
    Reply migrateFrom() {
        // manage slot 0 migrate_from localhost 7379 force
        if (data.length != 6 && data.length != 7) {
            return ErrorReply.FORMAT
        }

        def toClientSlot = Short.parseShort(new String(data[2]))
        if (MultiShard.isToClientSlotSkip(toClientSlot)) {
            return ClusterxCommand.OK
        }

        def host = new String(data[4])
        def portBytes = data[5]
        int port
        try {
            port = Integer.parseInt(new String(portBytes))
        } catch (NumberFormatException ignore) {
            return ErrorReply.INVALID_INTEGER
        }
        def force = data.length == 7 && new String(data[6]).toLowerCase() == 'force'

        def innerSlot = slotWithKeyHashListParsed.first.slot()
//        def innerSlot = MultiShard.asInnerSlotByToClientSlot(toClientSlot)

        // clusterx use the same thread as one slot 0
        localPersist.oneSlot((short) 0).asyncRun(() -> {
            def mySelfShard = LocalPersist.instance.multiShard.mySelfShard()
            mySelfShard.importMigratingSlot = toClientSlot
        }).whenComplete((r, e) -> {
            if (e != null) {
                log.error 'Manage migrate_from, set import migrating slot={}, error={}, slot={}', toClientSlot, e.message, innerSlot
            } else {
                log.warn 'Manage migrate_from, set import migrating slot={}, slot={}', toClientSlot, innerSlot
            }
        })

        def oneSlot = localPersist.oneSlot(innerSlot)

        var replPairAsSlave = oneSlot.onlyOneReplPairAsSlave
        if (replPairAsSlave) {
            if (replPairAsSlave.host == host && replPairAsSlave.port == port) {
                log.info 'Manage migrate_from, already slave of host={}, port={}, slot={}',
                        host, port, oneSlot.slot()
                return ClusterxCommand.OK
            } else {
                log.warn 'Manage migrate_from, already slave of other host={}, port={}, slot={}',
                        replPairAsSlave.host, replPairAsSlave.port, oneSlot.slot()
                if (force) {
                    log.warn 'Manage migrate_from, force remove exist repl pair as slave'
                    oneSlot.removeReplPairAsSlave()
                    oneSlot.resetAsSlave(host, port)

                    return ClusterxCommand.OK
                } else {
                    return new ErrorReply('Already slave of other host: ' + replPairAsSlave.host + ':' + replPairAsSlave.port)
                }
            }
        } else {
            oneSlot.resetAsSlave(host, port)
            return ClusterxCommand.OK
        }
    }

    private Reply trainSampleListAndReturnRatio(String keyPrefixOrSuffixGiven, List<TrainSampleJob.TrainSampleKV> sampleToTrainList) {
        def trainSampleJob = new TrainSampleJob(workerId)
        trainSampleJob.resetSampleToTrainList(sampleToTrainList)
        def trainSampleResult = trainSampleJob.train()

        // only one key prefix given, only one dict after train
        def trainSampleCacheDict = trainSampleResult.cacheDict()
        def onlyOneDict = trainSampleCacheDict.get(keyPrefixOrSuffixGiven)
        log.warn 'Train new dict result, sample value count={}, dict count={}', data.length - 4, trainSampleCacheDict.size()
        // will overwrite same key prefix dict exists
        dictMap.putDict(keyPrefixOrSuffixGiven, onlyOneDict)

//            def oldDict = dictMap.putDict(keyPrefixOrSuffixGiven, onlyOneDict)
//            if (oldDict != null) {
//                // keep old dict in persist, because may be used by other worker
//                // when start server, early dict will be overwritten by new dict with same key prefix, need not persist again?
//                dictMap.putDict(keyPrefixOrSuffixGiven + '_' + new Random().nextInt(10000), oldDict)
//            }

        // show compress ratio use dict just trained
        long totalBytes = 0
        long totalCompressedBytes = 0
        for (sample in sampleToTrainList) {
            totalBytes += sample.valueBytes().length
            def compressedBytes = onlyOneDict.compressByteArray(sample.valueBytes())
            totalCompressedBytes += compressedBytes.length
        }

        def ratio = totalCompressedBytes / totalBytes
        return new BulkReply(ratio.toString().bytes)
    }

    Reply manageIndex() {
        if (data.length < 3) {
            return ErrorReply.FORMAT
        }

        def subSubCmd = new String(data[2]).toLowerCase()
        if (subSubCmd == 'reload-key-analysis-task') {
            // manage index reload-key-analysis-task notBusyBeginTime=00:00:00.0 notBusyEndTime=23:59:59.0
            Map<String, String> map = [:]
            for (int i = 3; i < data.length; i++) {
                def arg = new String(data[i])
                def arr = arg.split('=')
                if (arr.length != 2) {
                    return ErrorReply.SYNTAX
                }

                map[arr[0]] = arr[1]
            }

            if (!map) {
                return new ErrorReply('No config given')
            }

            def config = Config.ofMap(map)
            def persistConfig = Config.create()
                    .with('keyAnalysis', config)

            localPersist.indexHandlerPool.keyAnalysisHandler.resetInnerTask(persistConfig)
            log.warn 'Manage index reload-key-analysis-task, items: {}', map
            return OKReply.INSTANCE
        }

        return ErrorReply.SYNTAX
    }

    Reply dict() {
        if (data.length < 3) {
            return ErrorReply.FORMAT
        }

        def subSubCmd = new String(data[2]).toLowerCase()
        if (subSubCmd == 'set-key-prefix-or-suffix-groups') {
            // manage dict set-key-prefix-or-suffix-groups keyPrefix1,keyPrefix2
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def keyPrefixOrSuffixGroup = new String(data[3])
            if (!keyPrefixOrSuffixGroup) {
                return ErrorReply.SYNTAX
            }

            var firstOneSlot = localPersist.currentThreadFirstOneSlot()
            firstOneSlot.dynConfig.update(TrainSampleJob.KEY_IN_DYN_CONFIG, keyPrefixOrSuffixGroup);

            return OKReply.INSTANCE
        }

        if (subSubCmd == 'view-dict-summary') {
            // manage dict view-dict-summary
            if (data.length != 3) {
                return ErrorReply.FORMAT
            }

            def sb = new StringBuilder()
            dictMap.cacheDictBySeqCopy.each { seq, dict ->
                sb << dict << '\n'
            }
            sb << '----------------\n'
            dictMap.cacheDictCopy.each { keyPrefix, dict ->
                sb << keyPrefix << ': ' << dict << '\n'
            }

            return new BulkReply(sb.toString().bytes)
        }

        if (subSubCmd == 'train-new-dict') {
            // manage dict train-new-dict keyPrefixOrSuffix sampleValue1 sampleValue2 ...
            if (data.length <= 4 + MIN_TRAIN_SAMPLE_SIZE) {
                return new ErrorReply('Train sample value count too small')
            }

            def keyPrefixOrSuffixGiven = new String(data[3])

            List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
            for (int i = 4; i < data.length; i++) {
                sampleToTrainList << new TrainSampleJob.TrainSampleKV(null, keyPrefixOrSuffixGiven, 0L, data[i])
            }

            return trainSampleListAndReturnRatio(keyPrefixOrSuffixGiven, sampleToTrainList)
        }

        if (subSubCmd == 'train-new-dict-by-keys-in-redis') {
            // manage dict train-new-dict-by-keys-in-redis keyPrefixOrSuffix host port sampleKey1 sampleKey2
            if (data.length <= 6 + MIN_TRAIN_SAMPLE_SIZE) {
                return new ErrorReply('Train sample value count too small')
            }

            def keyPrefixOrSuffixGiven = new String(data[3])
            def host = new String(data[4])
            def portBytes = data[5]

            int port
            try {
                port = Integer.parseInt(new String(portBytes))
            } catch (NumberFormatException e) {
                return ErrorReply.INVALID_INTEGER
            }

            def log = LoggerFactory.getLogger(ManageCommand.class)
            byte[][] valueBytesArray = new byte[data.length - 6][]
            try {
                def jedisPool = JedisPoolHolder.instance.create(host, port)
                // may be null
                JedisPoolHolder.exe(jedisPool, jedis -> {
                    def pong = jedis.ping()
                    log.info("Manage train dict, remote redis server={}:{} pong={}", host, port, pong);
                })

                JedisPoolHolder.exe(jedisPool, jedis -> {
                    for (i in 6..<data.length) {
                        def key = new String(data[i])
                        def isExists = jedis.exists(key)
                        if (isExists) {
                            def type = jedis.type(key)
                            if (type == 'string') {
                                valueBytesArray[i - 6] = jedis.get(key)?.bytes
                            } else if (type == 'list') {
                                def list = jedis.lrange(key, 0, -1)
                                def rl = new RedisList()
                                for (one in list) {
                                    rl.addLast(one.bytes)
                                }
                                valueBytesArray[i - 6] = rl.encodeButDoNotCompress()
                            } else if (type == 'hash') {
                                def hash = jedis.hgetAll(key)
                                def rhh = new RedisHH()
                                hash.each { k, v ->
                                    rhh.put(k, v.bytes)
                                }
                                valueBytesArray[i - 6] = rhh.encodeButDoNotCompress()
                            } else if (type == 'set') {
                                def set = jedis.smembers(key)
                                def rhk = new RedisHashKeys()
                                for (one in set) {
                                    rhk.add(one)
                                }
                                valueBytesArray[i - 6] = rhk.encodeButDoNotCompress()
                            } else if (type == 'zset') {
                                def zset = jedis.zrandmemberWithScores(key, 1000)
                                def rz = new RedisZSet()
                                for (one in zset) {
                                    rz.add(one.score, new String(one.element))
                                }
                                valueBytesArray[i - 6] = rz.encodeButDoNotCompress()
                            }
                        }
                    }
                    null
                })
            } catch (Exception e) {
                return new ErrorReply(e.message)
            }

            List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
            for (i in 0..<valueBytesArray.length) {
                if (valueBytesArray[i] == null) {
                    return new ErrorReply('Key not exists or type not support: ' + new String(data[i + 6]))
                }
                sampleToTrainList << new TrainSampleJob.TrainSampleKV(null, keyPrefixOrSuffixGiven, 0L, valueBytesArray[i])
            }

            return trainSampleListAndReturnRatio(keyPrefixOrSuffixGiven, sampleToTrainList)
        }

        if (subSubCmd == 'output-dict-bytes') {
            // manage dict output-dict-bytes 12345
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            def dictSeqBytes = data[3]
            int dictSeq

            try {
                dictSeq = Integer.parseInt(new String(dictSeqBytes))
            } catch (NumberFormatException ignored) {
                return ErrorReply.SYNTAX
            }

            def dict = dictMap.getDictBySeq(dictSeq)
            if (dict == null) {
                return new ErrorReply('Dict not found, dict seq: ' + dictSeq)
            }

            def userHome = System.getProperty('user.home')
            def file = new File(new File(userHome), 'dict-seq-' + dictSeq + '.dat')
            try {
                file.bytes = dict.dictBytes
                log.info 'Output dict bytes to file={}', file.absolutePath
            } catch (IOException e) {
                return new ErrorReply(e.message)
            }

            return OKReply.INSTANCE
        }

        return ErrorReply.SYNTAX
    }

    Reply dynConfig() {
        // manage dyn-config key value
        if (data.length != 4) {
            return ErrorReply.FORMAT
        }

        def configKeyBytes = data[2]
        def configValueBytes = data[3]

        def configKey = new String(configKeyBytes)
        def configValue = new String(configValueBytes)

        ArrayList<Promise<Boolean>> promises = []
        def oneSlots = localPersist.oneSlots()
        for (oneSlot in oneSlots) {
            def p = oneSlot.asyncCall(() -> oneSlot.updateDynConfig(configKey, configValue))
            promises.add(p)
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error 'Manage dyn-config set error={}', e.message
                finalPromise.exception = e
                return
            }

            // every true
            for (int i = 0; i < promises.size(); i++) {
                def p = promises.get(i)
                if (!p.result) {
                    finalPromise.set(new ErrorReply('Slot ' + i + ' set dyn-config failed'))
                    return
                }
            }

            finalPromise.set(OKReply.INSTANCE)
        })

        return asyncReply
    }

    Reply debug() {
        if (data.length < 4) {
            return ErrorReply.FORMAT
        }

        def subSubCmd = new String(data[2]).toLowerCase()
        if (subSubCmd == 'log-switch') {
            if (data.length != 5) {
                return ErrorReply.FORMAT
            }

            // manage debug log-switch logCmd 1
            def field = new String(data[3])
            def val = new String(data[4])
            def isOn = val == '1' || val == 'true'

            switch (field) {
                case 'logCmd' -> Debug.getInstance().logCmd = isOn
                case 'logMerge' -> Debug.getInstance().logMerge = isOn
                case 'logTrainDict' -> Debug.getInstance().logTrainDict = isOn
                case 'logRestore' -> Debug.getInstance().logRestore = isOn
                case 'bulkLoad' -> Debug.getInstance().bulkLoad = isOn
                default -> {
                    log.warn 'Manage unknown debug field={}', field
                }
            }

            return OKReply.INSTANCE
        } else if (subSubCmd == 'calc-key-hash') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }

            // manage debug calc-key-hash key
            def keyBytes = data[3]
            def slotWithKeyHash = slot(keyBytes)
            return new BulkReply(slotWithKeyHash.toString().bytes)
        } else if (subSubCmd == 'key-analysis') {
            if (data.length != 4) {
                return ErrorReply.FORMAT
            }
            // todo
            // data[3] == prefix-count-top-k

            def f = localPersist.indexHandlerPool.keyAnalysisHandler.topKPrefixCounts

            SettablePromise<Reply> finalPromise = new SettablePromise<>()
            def asyncReply = new AsyncReply(finalPromise)

            f.whenComplete { r, e ->
                if (e) {
                    finalPromise.set new ErrorReply(e.message)
                    return
                }

                if (r.isEmpty()) {
                    finalPromise.set MultiBulkReply.EMPTY
                    return
                }

                def replies = new Reply[r.size()]
                r.eachWithIndex { entry, i ->
                    replies[i] = new BulkReply((entry.key + ': ' + entry.value).bytes)
                }
                finalPromise.set(new MultiBulkReply(replies))
            }
            return asyncReply
        }

        return ErrorReply.SYNTAX
    }

}