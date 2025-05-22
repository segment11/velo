package io.velo.command

import io.velo.*
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.repl.incremental.XOneWalGroupPersist
import io.velo.reply.*
import redis.clients.jedis.Jedis
import spock.lang.Specification

class ManageCommandTest extends Specification {
    def _ManageCommand = new ManageCommand()

    final short slot = 0

    def 'test parse slot'() {
        given:
        def data1 = new byte[1][]

        expect:
        _ManageCommand.parseSlots('manage', data1, 1).size() == 0

        when:
        def data5 = new byte[5][]
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        data5[3] = 'sub_cmd'.bytes
        def sList = _ManageCommand.parseSlots('manage', data5, 1)
        then:
        sList.size() == 1

        when:
        data5[2] = '8192'.bytes
        data5[3] = 'migrate_from'.bytes
        sList = _ManageCommand.parseSlots('manage', data5, 1)
        then:
        sList.size() == 1
        sList[0].slot() == 0

        when:
        data5[2] = 'a'.bytes
        sList = _ManageCommand.parseSlots('manage', data5, 1)
        then:
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        sList = _ManageCommand.parseSlots('manage', data4, 1)
        then:
        sList.size() == 0

        when:
        data4[1] = 'xxx'.bytes
        sList = _ManageCommand.parseSlots('manage', data4, 1)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def mGroup = new MGroup('manage', data1, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)

        when:
        def reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = 'debug'.bytes
        manage.data = data2
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'dyn-config'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'dict'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'index'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'slot'.bytes
        reply = manage.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        data2[1] = 'zzz'.bytes
        reply = manage.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test debug'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        when:
        data4[2] = 'calc-key-hash'.bytes
        data4[3] = 'key:0'.bytes
        def reply = manage.debug()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        def data5 = new byte[5][]
        data5[2] = 'calc-key-hash'.bytes
        manage.data = data5
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[2] = 'log-switch'.bytes
        data5[3] = 'logCmd'.bytes
        data5[4] = 'true'.bytes
        manage.data = data5
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[4] = '1'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logMerge'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logTrainDict'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'logRestore'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'bulkLoad'.bytes
        data5[4] = '0'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'xxx'.bytes
        reply = manage.debug()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[2] = 'key-analysis'.bytes
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[2] = 'log-switch'.bytes
        manage.data = data4
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        when:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.startIndexHandlerPool()
        data4[2] = 'key-analysis'.bytes
        reply = manage.debug()
        Thread.sleep(100)
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.getResult() == MultiBulkReply.EMPTY

        when:
        localPersist.indexHandlerPool.keyAnalysisHandler.innerTask.addTopKPrefixCount('test:', 0)
        reply = manage.debug()
        Thread.sleep(100)
        then:
        reply instanceof AsyncReply
        ((AsyncReply) reply).settablePromise.getResult() instanceof MultiBulkReply
        ((MultiBulkReply) ((AsyncReply) reply).settablePromise.getResult()).replies.length == 1

        when:
        data4[2] = 'xxx'.bytes
        reply = manage.debug()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.debug()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test dyn-config'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        data4[1] = 'dyn-config'.bytes
        data4[2] = 'testKey'.bytes
        data4[3] = '1'.bytes
        def reply = manage.dynConfig()
        then:
        reply instanceof AsyncReply

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.dynConfig()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test dict'() {
        given:
        def data4 = new byte[4][]

        def mGroup = new MGroup('manage', data4, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)

        def dict = new Dict()
        dict.dictBytes = 'test'.bytes
        dict.seq = 1
        dict.createdTime = System.currentTimeMillis()
        dictMap.putDict('key:', dict)

        when:
        data4[1] = 'dict'.bytes
        data4[2] = 'set-key-prefix-or-suffix-groups'.bytes
        data4[3] = 'key:,xxx:'.bytes
        def reply = manage.dict()
        then:
        reply == OKReply.INSTANCE
        TrainSampleJob.keyPrefixOrSuffixGroupList == ['key:', 'xxx:']

        when:
        data4[3] = ''.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = 'view-dict-summary'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[2] = 'output-dict-bytes'.bytes
        // use dict seq
        data4[3] = '1'.bytes
        reply = manage.dict()
        then:
        reply == OKReply.INSTANCE

        when:
        data4[2] = 'output-dict-bytes'.bytes
        // use dict seq, but not exists
        data4[3] = '2'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        def data3 = new byte[3][]
        data3[1] = 'dict'.bytes
        data3[2] = 'view-dict-summary'.bytes
        manage.data = data3
        reply = manage.dict()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        // train new dict
        def data15 = new byte[15][]
        data15[1] = 'dict'.bytes
        data15[2] = 'train-new-dict'.bytes
        data15[3] = 'key:'.bytes
        11.times {
            data15[it + 4] = ('aaaaabbbbbccccc' * 5).bytes
        }
        manage.data = data15
        reply = manage.dict()
        then:
        reply instanceof BulkReply
        Double.parseDouble(new String(((BulkReply) reply).raw)) < 1

        when:
        def data14 = new byte[14][]
        data14[1] = 'dict'.bytes
        data14[2] = 'train-new-dict'.bytes
        manage.data = data14
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        def data16 = new byte[16][]
        data16[1] = 'dict'.bytes
        data16[2] = 'train-new-dict-by-keys-in-redis'.bytes
        manage.data = data16
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        boolean doThisCase = Consts.checkConnectAvailable('localhost', 6379)
        if (doThisCase) {
            def jedis = new Jedis('localhost', 6379)
            jedis.del('a', 'b', 'c', 'd', 'e')
            jedis.set('a', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.hset('b', 'f1', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.hset('b', 'f2', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.hset('b', 'f3', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.lpush('c', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.lpush('c', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.lpush('c', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd')
            jedis.sadd('d', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd0', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd1', 'aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd2')
            Map<String, Double> scores = [:]
            scores.put('aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd0', 1.0)
            scores.put('aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd1', 2.0)
            scores.put('aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd2', 3.0)
            jedis.zadd('e', scores)
            jedis.close()
            println 'redis test data for train-new-dict-by-keys-in-redis ready'
        }
        def data17 = new byte[17][]
        data17[1] = 'dict'.bytes
        data17[2] = 'train-new-dict-by-keys-in-redis'.bytes
        data17[3] = 'xxx:'.bytes
        data17[4] = '127.0.0.1'.bytes
        data17[5] = '6379'.bytes
        data17[6] = 'a'.bytes
        data17[7] = 'b'.bytes
        data17[8] = 'c'.bytes
        data17[9] = 'd'.bytes
        data17[10] = 'e'.bytes
        data17[11] = 'a'.bytes
        data17[12] = 'a'.bytes
        data17[13] = 'a'.bytes
        data17[14] = 'a'.bytes
        data17[15] = 'a'.bytes
        data17[16] = 'a'.bytes
        manage.data = data17
        reply = doThisCase ? manage.dict() : new BulkReply('skip'.bytes)
        then:
        reply instanceof BulkReply || reply instanceof ErrorReply

        when:
        data17[5] = 'a'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data17[5] = '6379'.bytes
        data17[16] = 'xxx'.bytes
        reply = doThisCase ? manage.dict() : ErrorReply.SYNTAX
        then:
        // xxx not exists
        reply instanceof ErrorReply

        // ***
        when:
        def data8 = new byte[8][]
        data8[1] = 'dict'.bytes
        data8[2] = 'train-new-dict-by-parquet-file'.bytes
        // keyPrefixOrSuffix=t1: file=sample.parquet schema-file=schema.message n=1000 ingest-format=json
        data8[3] = 'keyPrefixOrSuffix=t1:'.bytes
        // relative to project root path
        data8[4] = 'file=src/test/groovy/sample.parquet'.bytes
        data8[5] = 'schema-file=src/test/groovy/sample.schema'.bytes
        data8[6] = 'n=1000'.bytes
        data8[7] = 'ingest-format=json'.bytes
        manage.data = data8
        reply = manage.dict()
        then:
        reply instanceof BulkReply

        when:
        data8[3] = 'keyPrefixOrSuffix=t2:'.bytes
        data8[7] = 'ingest-format=csv'.bytes
        reply = manage.dict()
        then:
        reply instanceof BulkReply

        when:
        data8[6] = 'n=xxx'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data8[6] = 'n=99'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data8[6] = 'n=1001'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data8[6] = 'n=100'.bytes
        data8[4] = 'file0=sample.parquet'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        // file not exists
        data8[4] = 'file=sample.parquet'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data8[4] = 'file=src/test/groovy/sample.parquet'.bytes
        data8[5] = 'schema-file0=sample.schema'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        // schema file not exists
        data8[5] = 'schema-file=sample.schema'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data8[3] = 'keyPrefixOrSuffix0=t1:'.bytes
        reply = manage.dict()
        then:
        reply instanceof ErrorReply

        when:
        data8[3] = 'keyPrefixOrSuffix0'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = 'train-new-dict-by-parquet-file'.bytes
        manage.data = data4
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        // ***
        when:
        data3[2] = 'output-dict-bytes'.bytes
        manage.data = data3
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data3[2] = 'set-key-prefix-or-suffix-groups'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        when:
        data3[2] = 'xxx'.bytes
        reply = manage.dict()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.dict()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
        dictMap.clearAll()
        dictMap.cleanUp()
    }

    def 'test manage index'() {
        given:
        def mGroup = new MGroup('manage', null, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.startIndexHandlerPool()

        when:
        def reply = manage.execute('manage index xxx')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = manage.execute('manage index reload-key-analysis-task')
        then:
        reply instanceof ErrorReply

        when:
        reply = manage.execute('manage index reload-key-analysis-task aaa')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = manage.execute('manage index reload-key-analysis-task notBusyBeginTime=00:00:00.0 notBusyEndTime=23:59:59.0')
        then:
        reply == OKReply.INSTANCE

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test manage in on slot'() {
        given:
        def data5 = new byte[5][]

        def mGroup = new MGroup('manage', data5, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        oneSlot.monitorBigKeyByValueLength('test'.bytes, 1024)
        data5[1] = 'slot'.bytes
        data5[2] = '0'.bytes
        data5[3] = 'view-metrics'.bytes
        def reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data5[3] = 'view-big-key-top-k'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data5[3] = 'view-bucket-keys'.bytes
        data5[4] = ''.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[4] = 'iterate'.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        // set key bucket key / values for iterate
        def shortValueList = Mock.prepareShortValueList(10, 0)
        oneSlot.keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList,
                new XOneWalGroupPersist(true, false, 0))
        data5[4] = ''.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[4] = 'iterate'.bytes
        reply = manage.manageInOneSlot()
        println new String(((BulkReply) reply).raw)
        then:
        reply instanceof BulkReply

        when:
        data5[3] = 'update-kv-lru-max-size'.bytes
        data5[4] = '10000'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == OKReply.INSTANCE

        when:
        data5[3] = 'bucket'.bytes
        data5[4] = '0'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[4] = 'a'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data7 = new byte[7][]
        data7[1] = 'slot'.bytes
        data7[2] = '0'.bytes
        data7[3] = 'bucket'.bytes
        data7[4] = '0'.bytes
        data7[5] = 'view-bucket-key-count'.bytes
        data7[6] = ''.bytes
        manage.data = data7
        reply = manage.manageInOneSlot()
        then:
        reply instanceof IntegerReply

        when:
        data7[5] = 'view-bucket-keys'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        data4[2] = '0'.bytes
        data4[3] = 'update-kv-lru-max-size'.bytes
        manage.data = data4
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data4[3] = 'view-in-memory-size-estimate'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof MultiBulkReply

        when:
        data4[3] = 'set-readonly'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data4[3] = 'set-not-readonly'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data4[3] = 'set-can-read'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data4[3] = 'set-not-can-read'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data4[3] = 'key-buckets-warm-up'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof IntegerReply

        when:
        data4[3] = 'xxx'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data4[2] = 'a'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data6 = new byte[6][]
        data6[1] = 'slot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'output-chunk-segment-flag-to-file'.bytes
        data6[4] = '0'.bytes
        data6[5] = '0'.bytes
        manage.data = data6
        reply = manage.manageInOneSlot()
        then:
        reply == OKReply.INSTANCE

        when:
        data6[5] = '1024'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == OKReply.INSTANCE

        when:
        data6[4] = ConfForSlot.global.confChunk.maxSegmentNumber().toString().bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof ErrorReply

        when:
        data6[4] = '-1'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.SYNTAX

        when:
        data6[4] = 'a'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data6[4] = '0'.bytes
        data6[5] = 'a'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[3] = 'output-chunk-segment-flag-to-file'.bytes
        manage.data = data5
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[3] = 'migrate_from'.bytes
        manage.data = data5
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        data5[2] = '16384'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data5[2] = '1'.bytes
        data5[3] = 'view-metrics'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data7_ = new byte[7][]
        data7_[1] = 'slot'.bytes
        data7_[2] = '0'.bytes
        data7_[3] = 'mock-data'.bytes
        data7_[4] = 'n=100000'.bytes
        data7_[5] = 'k=16'.bytes
        data7_[6] = 'd=16'.bytes
        manage.data = data7_
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        ConfForSlot.global.confChunk.isSegmentUseCompression = false
        data7_[4] = 'nn=100000'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof BulkReply

        when:
        data7_[5] = 'k=xx'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        data7_[5] = 'k=14'.bytes
        reply = manage.manageInOneSlot()
        then:
        reply instanceof ErrorReply

        when:
        data5[2] = '0'.bytes
        data5[3] = 'mock-data'.bytes
        manage.data = data5
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data1 = new byte[1][]
        manage.data = data1
        reply = manage.manageInOneSlot()
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test migrate from'() {
        given:
        def data6 = new byte[6][]

        def mGroup = new MGroup('manage', data6, null)
        mGroup.from(BaseCommand.mockAGroup())
        def manage = new ManageCommand(mGroup)
        manage.from(mGroup)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        def shards = localPersist.multiShard.shards

        when:
        data6[1] = 'slot'.bytes
        data6[2] = '0'.bytes
        data6[3] = 'migrate_from'.bytes
        data6[4] = 'localhost'.bytes
        data6[5] = '7379'.bytes
        manage.slotWithKeyHashListParsed = manage.parseSlots('manage', data6, manage.slotNumber)
        def reply = manage.migrateFrom()
        then:
        reply == ClusterxCommand.OK
        shards[0].importMigratingSlot == 0

        when:
        data6[5] = 'a'.bytes
        reply = manage.migrateFrom()
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        def data7 = new byte[7][]
        data7[1] = 'slot'.bytes
        data7[2] = '0'.bytes
        data7[3] = 'migrate_from'.bytes
        data7[4] = 'localhost'.bytes
        data7[5] = '7379'.bytes
        data7[6] = ''.bytes
        oneSlot.doMockWhenCreateReplPairAsSlave = true
        oneSlot.createReplPairAsSlave('localhost', 7379)
        manage.data = data7
        manage.slotWithKeyHashListParsed = manage.parseSlots('manage', data7, manage.slotNumber)
        reply = manage.migrateFrom()
        then:
        reply == ClusterxCommand.OK

        when:
        oneSlot.removeReplPairAsSlave()
        oneSlot.createReplPairAsSlave('localhost', 7380)
        reply = manage.migrateFrom()
        then:
        // already slave of other host:port
        reply instanceof ErrorReply

        when:
        data7[6] = 'force'.bytes
        reply = manage.migrateFrom()
        then:
        reply == ClusterxCommand.OK

        when:
        // skip
        data7[2] = '1'.bytes
        reply = manage.migrateFrom()
        then:
        reply == ClusterxCommand.OK

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
