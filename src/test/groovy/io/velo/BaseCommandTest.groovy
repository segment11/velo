package io.velo

import com.github.luben.zstd.Zstd
import io.activej.config.Config
import io.activej.net.socket.tcp.ITcpSocket
import io.velo.acl.AclUsers
import io.velo.acl.U
import io.velo.command.AGroup
import io.velo.decode.BigStringNoMemoryCopy
import io.velo.decode.Request
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.repl.cluster.MultiShard
import io.velo.reply.Reply
import io.velo.type.RedisList
import org.apache.commons.io.FileUtils
import redis.clients.jedis.util.JedisClusterCRC16
import spock.lang.Specification

import java.nio.ByteBuffer

class BaseCommandTest extends Specification {
    static class SubCommand extends BaseCommand {
        SubCommand(String cmd, byte[][] data, ITcpSocket socket) {
            super(cmd, data, socket)
        }

        @Override
        ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
            ArrayList<SlotWithKeyHash> list = []
            list
        }

        @Override
        Reply handle() {
            return null
        }
    }

    final short slot = 0
    final int slotNumber = 1

    def 'test static methods'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)

        def k1 = 'key1'
        def s1 = BaseCommand.slot(k1, 1)
        println s1

        def k11 = 'key11'
        def s11 = BaseCommand.slot(k11, 2)

        def k2 = 'key2{x'
        def s2 = BaseCommand.slot(k2, 1)

        def k22 = 'key2}x'
        def s22 = BaseCommand.slot(k22, 1)

        def k3 = 'key3{x}'
        def s3 = BaseCommand.slot(k3, 1)

        def k33 = 'key3{x}'
        def s33 = BaseCommand.slot(k33, 2)

        def k4 = 'key4{x}'
        def s4 = BaseCommand.slot(k4.bytes, 1)

        def k5 = 'key5{xyz}'
        def s5 = BaseCommand.slot(k5.bytes, 1)

        expect:
        s1.slot() == 0
        s1.bucketIndex() < 65536
        s1.keyHash() != 0
        s3.slot() == s4.slot()

        when:
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = 16384
        then:
        BaseCommand.slot(k11.bytes, 16384).slot() == JedisClusterCRC16.getSlot(k11.bytes)

        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        then:
        BaseCommand.getAuthU(socket) == U.INIT_DEFAULT_U

        when:
        def veloUserData = new VeloUserDataInSocket()
        socket.userData = veloUserData
        veloUserData.authUser = 'test-user'
        AclUsers.instance.initForTest()
        then:
        BaseCommand.getAuthU(socket) == null

        when:
        def aGroup = new AGroup(null, null, socket)
        then:
        aGroup.getAuthU() == null

        cleanup:
        ConfForGlobal.clusterEnabled = false
        ConfForGlobal.slotNumber = 1
    }

    def 'test key index'() {
        expect:
        BaseCommand.KeyIndexBegin1.isKeyBytes(1)
        BaseCommand.KeyIndexBegin1.isKeyBytes(2)

        BaseCommand.KeyIndexBegin1Step2.isKeyBytes(1)
        !BaseCommand.KeyIndexBegin1Step2.isKeyBytes(2)
        BaseCommand.KeyIndexBegin1Step2.isKeyBytes(3)
        !BaseCommand.KeyIndexBegin1Step2.isKeyBytes(4)

        BaseCommand.KeyIndexBegin2.isKeyBytes(2)
        BaseCommand.KeyIndexBegin2.isKeyBytes(3)
        !BaseCommand.KeyIndexBegin2.isKeyBytes(1)

        BaseCommand.KeyIndexBegin2Step2.isKeyBytes(2)
        !BaseCommand.KeyIndexBegin2Step2.isKeyBytes(3)
        BaseCommand.KeyIndexBegin2Step2.isKeyBytes(4)
        !BaseCommand.KeyIndexBegin2Step2.isKeyBytes(5)

        when:
        def data3 = new byte[3][0]
        data3[0] = 'get'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'key2'.bytes
        ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList = []
        BaseCommand.addToSlotWithKeyHashList(slotWithKeyHashList, data3, slotNumber, BaseCommand.KeyIndexBegin1)
        then:
        slotWithKeyHashList.size() == 2
    }

    def 'test init'() {
        given:
        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes
        def c = new SubCommand('get', data2, null)
        c.requestHandler = null
        c.crossRequestWorker = false
        c.slotWithKeyHashListParsed = null
        c.cmd = 'get'
        c.data = data2
        c.socket = null
        c.resetContext('get', data2, null, new Request(data2, false, false))

        expect:
        c.cmd == 'get'
        c.data == data2
        c.socket == null
        c.dataToLine() == 'get key'
        c.execute('test test') == null
        c.execute('set >key >value') == null
        c.execute('test test') { data ->
            data[0] = 'test'.bytes
        } == null

        when:
        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
        c.init(requestHandler, new Request(data2, false, false))
        // overwrite
        def aGroup = BaseCommand.mockAGroup()
        def aGroup2 = BaseCommand.mockAGroup()
        aGroup2.byPassGetSet = new InMemoryGetSet()
        c.from(aGroup)
        c.from(aGroup2)
        c.byPassGetSet = null
        c.snowFlake = null
        then:
        c.workerId == 0
        c.slotWorkers == 1
        c.slotNumber == 1
        c.slot('key3{x}').slot() == BaseCommand.slot('key3{x}', 1).slot()

        c.compressStats != null
        c.trainSampleListMaxSize == 100
        c.snowFlake == null
        c.trainSampleJob != null
        c.sampleToTrainList.size() == 0

        c.slotWithKeyHashListParsed.size() == 0
        !c.isCrossRequestWorker

        c.handle() == null
    }

    def 'test slot'() {
        given:
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = 1024

        println new BaseCommand.SlotWithKeyHash((short) 0, (short) 0, 0, 0L, 0, 'key1')
        println new BaseCommand.SlotWithKeyHash((short) 0, 0, 0L, 0, 'key1')
        println new BaseCommand.SlotWithKeyHash((short) 0, 0, 0L, 0)

        expect:
        (0..<100).every {
            def keyBytes = ('key:' + it).bytes
            def slotWithKeyHash = BaseCommand.slot(keyBytes, ConfForGlobal.slotNumber)
            slotWithKeyHash.slot() == MultiShard.asInnerSlotByToClientSlot(JedisClusterCRC16.getSlot(keyBytes))
        }

        cleanup:
        ConfForGlobal.clusterEnabled = false
        ConfForGlobal.slotNumber = 1
    }

    def 'test get'() {
        given:
        def data2 = new byte[2][0]
        data2[0] = 'get'.bytes
        data2[1] = 'key'.bytes

        def c = new SubCommand('get', data2, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, null, Config.create())
        c.init(requestHandler, new Request(data2, false, false))

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def key = 'key'
        def sKey = BaseCommand.slot(key, slotNumber)
        c.byPassGetSet = inMemoryGetSet
        then:
        c.getExpireAt(sKey) == null
        c.getCv(sKey) == null

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.keyHash = sKey.keyHash()
        inMemoryGetSet.put(slot, 'key', sKey.bucketIndex(), cv)
        then:
        c.getExpireAt(sKey) == CompressedValue.NO_EXPIRE

        when:
        c.byPassGetSet = null
        then:
        c.getExpireAt(sKey) == null
        c.getCv(sKey) == null

        when:
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        c.getCv(sKey) != null
        c.getExpireAt(sKey) == CompressedValue.NO_EXPIRE

        when:
        cv.expireAt = System.currentTimeMillis() - 1000
        oneSlot.put(key, sKey.bucketIndex(), cv)
        then:
        c.getCv(sKey) == null

        when:
        // reset no expire
        cv.expireAt = CompressedValue.NO_EXPIRE
        // begin test big string
        def bigStringKey = 'kerry-test-big-string-key'
        def sBigString = BaseCommand.slot(bigStringKey, slotNumber)
        def cvBigString = Mock.prepareCompressedValueList(1)[0]
        cvBigString.keyHash = sBigString.keyHash()
        cvBigString.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvBigString.setCompressedDataAsBigString(1234L, CompressedValue.NULL_DICT_SEQ)
        oneSlot.put(bigStringKey, sBigString.bucketIndex(), cvBigString)
        then:
        c.getCv(sBigString) == null

        when:
        def bigStringBytes = ('aaaaabbbbbccccc' * 10).bytes
        oneSlot.bigStringFiles.writeBigStringBytes(1234L, bigStringKey, sBigString.bucketIndex(), bigStringBytes)
        then:
        c.getCv(sBigString).compressedData == bigStringBytes

        when:
        def cvNumber = new CompressedValue()
        cvNumber.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        def intBytes = new byte[4]
        ByteBuffer.wrap(intBytes).putInt(1234)
        cvNumber.compressedData = intBytes
        cvNumber.keyHash = sKey.keyHash()
        def valueBytes = c.getValueBytesByCv(cvNumber)
        then:
        valueBytes == '1234'.bytes

        when:
        def cvString = new CompressedValue()
        cvString.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvString.compressedData = 'hello'.bytes
        valueBytes = c.getValueBytesByCv(cvString)
        then:
        valueBytes == 'hello'.bytes

        when:
        def longStringBytes = ('aaaaabbbbbccccc' * 10).bytes
        def cr = CompressedValue.compress(longStringBytes, Dict.SELF_ZSTD_DICT)
        def cvCompressed = new CompressedValue()
        cvCompressed.keyHash = sKey.keyHash()
        cvCompressed.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        cvCompressed.compressedData = cr.data()
        valueBytes = c.getValueBytesByCv(cvCompressed)
        then:
        valueBytes.length == longStringBytes.length

        when:
        c.byPassGetSet = inMemoryGetSet
        inMemoryGetSet.put(slot, 'key', sKey.bucketIndex(), cv)
        valueBytes = c.get(sKey)
        then:
        valueBytes.length == cv.compressedLength

        when:
        valueBytes = c.get(sKey)
        then:
        valueBytes.length == cv.compressedLength
        c.get(BaseCommand.slot('not-exist-key', slotNumber)) == null

        when:
        c.byPassGetSet = inMemoryGetSet
        def keyForTypeList = 'key-list'
        def sKeyForTypeList = BaseCommand.slot(keyForTypeList, slotNumber)
        def cvForTypeList = new CompressedValue()
        cvForTypeList.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        cvForTypeList.compressedData = new RedisList().encode()
        cvForTypeList.keyHash = sKeyForTypeList.keyHash()
        inMemoryGetSet.put(slot, keyForTypeList, sKeyForTypeList.bucketIndex(), cvForTypeList)
        boolean exception = false
        try {
            c.get(sKeyForTypeList, true)
        } catch (TypeMismatchException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cvForTypeList.dictSeqOrSpType = 0
        inMemoryGetSet.put(slot, keyForTypeList, sKeyForTypeList.bucketIndex(), cvForTypeList)
        then:
        c.get(sKeyForTypeList, true) != null
        c.get(sKeyForTypeList, true, 0) != null

        when:
        exception = false
        try {
            c.get(sKeyForTypeList, true, CompressedValue.SP_TYPE_HASH, CompressedValue.SP_TYPE_SET)
        } catch (TypeMismatchException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test set'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def data3 = new byte[3][0]
        data3[0] = 'set'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def c = new SubCommand('set', data3, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, snowFlake, Config.create())
        c.init(requestHandler, new Request(data3, false, false))

        when:
        def key = 'key'
        def sKey = BaseCommand.slot(key, slotNumber)
        c.byPassGetSet = inMemoryGetSet
        c.setNumber((short) 1, sKey)
        then:
        c.get(sKey).length == 1

        when:
        c.setNumber(Byte.valueOf((byte) 127), sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 3

        when:
        c.setNumber((short) 200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 3

        when:
        c.setNumber((short) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 4

        when:
        c.setNumber((int) 1, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 1

        when:
        c.setNumber((int) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 4

        when:
        c.setNumber((int) 65536, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 5

        when:
        c.setNumber((int) -65537, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 6

        when:
        c.setNumber((long) 1, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 1

        when:
        c.setNumber((long) -200, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 4

        when:
        c.setNumber((long) 65536, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 5

        when:
        c.setNumber((long) -65537, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 6

        when:
        c.setNumber((long) (1L + Integer.MAX_VALUE), sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 10

        when:
        c.setNumber((long) (-1L + Integer.MIN_VALUE), sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 11

        when:
        c.setNumber((double) 0.99, sKey, CompressedValue.NO_EXPIRE)
        then:
        c.get(sKey).length == 4

        when:
        boolean exception = false
        try {
            c.setNumber((float) 0.99, sKey, CompressedValue.NO_EXPIRE)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def value = 'value'
        c.set(key, value.bytes)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(key, value.bytes)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(value.bytes, sKey)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(value.bytes, sKey)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(value.bytes, sKey, CompressedValue.SP_TYPE_SHORT_STRING)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = null
        c.set(value.bytes, sKey, CompressedValue.SP_TYPE_SHORT_STRING)
        then:
        c.get(sKey).length == 5

        when:
        c.byPassGetSet = inMemoryGetSet
        def cv = new CompressedValue()
        cv.compressedData = value.bytes
        cv.keyHash = sKey.keyHash()
        cv.expireAt = CompressedValue.NO_EXPIRE
        c.setCv(cv, sKey)
        then:
        // cv seq is new after set
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().encodedLength() == cv.encodedLength()

        when:
        c.byPassGetSet = null
        c.setCv(cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().encodedLength() == cv.encodedLength()

        when:
        def longValueBytes = ('aaaaabbbbbccccc' * 20).bytes
        c.byPassGetSet = inMemoryGetSet
        cv.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        cv.compressedData = Zstd.compress(longValueBytes, 3)
        c.setCv(cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == cv.compressedLength

        when:
        c.byPassGetSet = null
        c.setCv(cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == cv.compressedLength

        when:
        c.byPassGetSet = inMemoryGetSet
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        def intBytes = new byte[4]
        ByteBuffer.wrap(intBytes).putInt(1)
        cv.compressedData = intBytes
        c.setCv(cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = null
        c.setCv(cv, sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set('1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1234

        when:
        c.byPassGetSet = null
        c.set('1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1234

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set('0.99'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 0.99

        when:
        c.byPassGetSet = null
        c.set('0.99'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 0.99

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set(intBytes, sKey, CompressedValue.SP_TYPE_NUM_INT, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        c.byPassGetSet = null
        c.set(intBytes, sKey, CompressedValue.SP_TYPE_NUM_INT, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().numberValue() == 1

        when:
        ConfForGlobal.isValueSetUseCompression = false
        c.byPassGetSet = inMemoryGetSet
        c.set(longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == longValueBytes.length

        when:
        c.byPassGetSet = null
        c.set(longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength == longValueBytes.length

        when:
        ConfForGlobal.isValueSetUseCompression = true
        c.byPassGetSet = inMemoryGetSet
        c.set(longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()).cv().compressedLength < longValueBytes.length
        c.remove(sKey)

        when:
        c.byPassGetSet = null
        c.set(longValueBytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.getCv(sKey).compressedLength < longValueBytes.length
        c.remove(sKey)

        when:
        c.byPassGetSet = inMemoryGetSet
        c.set('1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.exists(sKey)
        !c.exists(c.slot('no-exist-key', slotNumber))

        when:
        c.removeDelay(sKey)
        then:
        inMemoryGetSet.getBuf(slot, key, sKey.bucketIndex(), sKey.keyHash()) == null

        when:
        c.byPassGetSet = null
        c.set('1234'.bytes, sKey, 0, CompressedValue.NO_EXPIRE)
        then:
        c.exists(sKey)

        when:
        c.removeDelay(sKey)
        then:
        c.getCv(sKey) == null
        !c.exists(sKey)

        when:
        // big string
        c.bigStringNoMemoryCopy = new BigStringNoMemoryCopy()
        c.bigStringNoMemoryCopy.offset = 1000
        c.bigStringNoMemoryCopy.length = 1000
        def bigValueBytes = ('x' * 10000).bytes
        c.set(bigValueBytes, sKey, 0, System.currentTimeMillis() + 1000)
        then:
        oneSlot.bigStringFiles.getBigStringFileUuidList(sKey.bucketIndex()).size() > 0

        when:
        ConfForGlobal.bigStringNoCompressMinSize = 500
        c.set(bigValueBytes, sKey, 0, System.currentTimeMillis() + 1000)
        then:
        oneSlot.bigStringFiles.getBigStringFileUuidList(sKey.bucketIndex()).size() > 0

        cleanup:
        ConfForGlobal.bigStringNoCompressMinSize = 1024 * 256
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test train dict'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def data3 = new byte[3][0]
        data3[0] = 'set'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def c = new SubCommand('set', data3, null)
        def inMemoryGetSet = new InMemoryGetSet()

        def requestHandler = new RequestHandler((byte) 0, (byte) 1, (short) 1, snowFlake, Config.create())
        c.init(requestHandler, new Request(data3, false, false))

        and:
        FileUtils.forceMkdir(Consts.testDir)
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)
        if (dictMap.dictSize() != 0) {
            dictMap.clearAll()
        }

        expect:
        c.handleTrainSampleResult(null) == null

        when:
        def trainSampleResult = new TrainSampleJob.TrainSampleResult(new HashMap<String, Dict>(), new ArrayList<Long>())
        c.handleTrainSampleResult(trainSampleResult)
        then:
        dictMap.dictSize() == 0

        when:
        ConfForGlobal.isOnDynTrainDictForCompression = true
        c.byPassGetSet = inMemoryGetSet
        def longValueBytes = ('aaaaabbbbbccccc' * 10).bytes
        List<String> keyList = []
        1001.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            keyList << key
            c.set(key, longValueBytes)
        }
        then:
        dictMap.dictSize() == 1
        dictMap.getDict('key:') != null

        when:
        def firstKey = keyList[0]
        def sFirstKey = BaseCommand.slot(firstKey.bytes, slotNumber)
        // use trained dict
        c.set(longValueBytes, sFirstKey)
        def cvGet = inMemoryGetSet.getBuf(slot, firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash()).cv()
        then:
        c.getValueBytesByCv(cvGet).length == longValueBytes.length

        when:
        def trainedDict = dictMap.getDict('key:')
        dictMap.clearAll()
        boolean exception = false
        try {
            c.getValueBytesByCv(cvGet)
        } catch (DictMissingException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        Dict.resetGlobalDictBytes(trainedDict.dictBytes)
        c.set(firstKey, longValueBytes)
        cvGet = inMemoryGetSet.getBuf(slot, firstKey, sFirstKey.bucketIndex(), sFirstKey.keyHash()).cv()
        then:
        cvGet.dictSeqOrSpType == Dict.GLOBAL_ZSTD_DICT_SEQ
        c.getValueBytesByCv(cvGet).length == longValueBytes.length

        when:
        Dict.resetGlobalDictBytes(new byte[1])
        exception = false
        try {
            c.getValueBytesByCv(cvGet)
        } catch (DictMissingException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        dictMap.cleanUp()
        Consts.testDir.deleteDir()
    }

    def 'test key analysis'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.startIndexHandlerPool()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        def sKey = BaseCommand.slot('a'.bytes, (short) 1)
        def c = new SubCommand('set', null, null)
        c.from(BaseCommand.mockAGroup())

        when:
        c.set('a'.bytes, sKey)
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.dictSeqOrSpType = 1
        c.setCv(cv, sKey)
        c.remove(sKey)
        c.removeDelay(sKey)
        Thread.sleep(1000)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
    }
}
