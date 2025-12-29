package io.velo

import io.activej.bytebuf.ByteBuf
import io.netty.buffer.Unpooled
import spock.lang.Specification

import java.nio.ByteBuffer

class CompressedValueTest extends Specification {

    def 'test type'() {
        given:
        def cv = new CompressedValue()
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        cv.compressedData = new byte[0]
        println cv
        println cv.getExpireAt()
        println cv.compressedLength
        println cv.uncompressedLength

        expect:
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_BYTE)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_SHORT)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_INT)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_LONG)
        CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_NUM_DOUBLE)
        CompressedValue.isTypeDouble(CompressedValue.SP_TYPE_NUM_DOUBLE)
        !CompressedValue.isTypeDouble(CompressedValue.SP_TYPE_NUM_LONG)
        !CompressedValue.isTypeNumber(CompressedValue.SP_TYPE_SHORT_STRING)
        !CompressedValue.isTypeNumber(CompressedValue.NULL_DICT_SEQ)
        !CompressedValue.isTypeString(CompressedValue.SP_TYPE_HASH)

        CompressedValue.isTypeString(cv.dictSeqOrSpType)
        CompressedValue.isHash(CompressedValue.SP_TYPE_HH)
        CompressedValue.isHash(CompressedValue.SP_TYPE_HASH)
        !CompressedValue.isHash(CompressedValue.SP_TYPE_LIST)
        CompressedValue.isList(CompressedValue.SP_TYPE_LIST)
        !CompressedValue.isList(CompressedValue.SP_TYPE_SET)
        CompressedValue.isSet(CompressedValue.SP_TYPE_SET)
        !CompressedValue.isSet(CompressedValue.SP_TYPE_ZSET)
        CompressedValue.isZSet(CompressedValue.SP_TYPE_ZSET)
        !CompressedValue.isZSet(CompressedValue.SP_TYPE_GEO)
        CompressedValue.isGeo(CompressedValue.SP_TYPE_GEO)
        !CompressedValue.isGeo(CompressedValue.SP_TYPE_STREAM)
        CompressedValue.isStream(CompressedValue.SP_TYPE_STREAM)
        !CompressedValue.isStream(CompressedValue.SP_TYPE_BLOOM_BITMAP)
        CompressedValue.isBloomFilter(CompressedValue.SP_TYPE_BLOOM_BITMAP)
        !CompressedValue.isBloomFilter(CompressedValue.SP_TYPE_HLL)
        CompressedValue.isHll(CompressedValue.SP_TYPE_HLL)
        !CompressedValue.isHll(CompressedValue.SP_TYPE_HH)
        CompressedValue.isDeleted(new byte[]{CompressedValue.SP_FLAG_DELETE_TMP})
        !CompressedValue.isDeleted(new byte[2])
        !CompressedValue.isDeleted(new byte[]{0})

        cv.isTypeNumber()
        !cv.isTypeDouble()
        cv.isTypeString()
        cv.isShortString()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        then:
        cv.isTypeString()
        !cv.isTypeDouble()
        !cv.isTypeNumber()
        !cv.isBigString()
        !cv.isHash()
        !cv.isList()
        !cv.isSet()
        !cv.isZSet()
        !cv.isGeo()
        !cv.isStream()
        !cv.isUseDict()

        when:
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        then:
        !cv.isTypeNumber()

        when:
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN]
        then:
        cv.isShortString()

        when:
        cv.compressedData = new byte[CompressedValue.SP_TYPE_SHORT_STRING_MIN_LEN + 1]
        then:
        !cv.isShortString()

        when:
        cv.compressedData = null
        then:
        !cv.isShortString()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        then:
        cv.isHash()
        !cv.isTypeString()
        !cv.isShortString()
        !cv.isList()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        then:
        cv.isHash()
        !cv.isCompressed()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_LIST
        then:
        cv.isList()
        !cv.isSet()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        then:
        cv.isSet()
        !cv.isZSet()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_ZSET
        then:
        cv.isZSet()
        !cv.isGeo()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_GEO
        then:
        cv.isGeo()
        !cv.isStream()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_STREAM
        then:
        cv.isStream()
        !cv.isBloomFilter()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BLOOM_BITMAP
        then:
        cv.isBloomFilter()
        !cv.isHll()

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_HLL
        then:
        cv.isHll()
        !cv.isHash()

        when:
        cv.dictSeqOrSpType = Dict.GLOBAL_ZSTD_DICT_SEQ
        then:
        cv.isCompressed()
    }

    def 'test expire'() {
        given:
        def cv = new CompressedValue()

        expect:
        cv.noExpire()
        !cv.isExpired()

        when:
        cv.expireAt = System.currentTimeMillis() - 1000
        then:
        cv.isExpired()
        !cv.noExpire()

        when:
        cv.expireAt = System.currentTimeMillis() + 1000
        then:
        !cv.isExpired()

        when:
        cv.expireAt = CompressedValue.EXPIRE_NOW
        then:
        cv.isExpired()
    }

    def 'test encode'() {
        given:
        def cv = new CompressedValue()
        cv.seq = 123L
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.keyHash = 123L
        cv.compressedData = new byte[10]

        def cv2 = new CompressedValue()
        cv2.seq = 123L
        cv2.seq = 123L
        cv2.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv2.keyHash = 123L
        cv2.compressedData = null

        when:
        def encoded = cv.encode()
        def encoded2 = cv2.encode()
        def cvDecode = CompressedValue.decode(Unpooled.wrappedBuffer(encoded), null, 0L)
        def buf = ByteBuf.wrapForWriting(new byte[cv.encodedLength()])
        def buf2 = ByteBuf.wrapForWriting(new byte[cv2.encodedLength()])
        cv.encodeTo(buf)
        cv2.encodeTo(buf2)
        then:
        CompressedValue.onlyReadSeq(encoded) == cv.seq
        CompressedValue.onlyReadSpType(encoded) == cv.dictSeqOrSpType
        cvDecode.seq == cv.seq
        cvDecode.dictSeqOrSpType == cv.dictSeqOrSpType
        cvDecode.keyHash == cv.keyHash
        cvDecode.compressedLength == cv.compressedLength
        Arrays.equals(cvDecode.compressedData, cv.compressedData)
        encoded.length == cv.encodedLength()
        buf.tail() == cv.encodedLength()
        encoded2.length == CompressedValue.VALUE_HEADER_LENGTH
        buf2.tail() == CompressedValue.VALUE_HEADER_LENGTH

        when:
        cv2.compressedData = new byte[0]
        encoded2 = cv2.encode()
        buf2.tail(0)
        cv2.encodeTo(buf2)
        then:
        encoded2.length == CompressedValue.VALUE_HEADER_LENGTH
        buf2.tail() == CompressedValue.VALUE_HEADER_LENGTH

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_BYTE
        cv.compressedData[0] = Byte.MAX_VALUE
        def encodedNumber = cv.encodeAsNumber()
        def encodedBufferNumber = ByteBuffer.wrap(encodedNumber)
        def cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L)
        then:
        CompressedValue.onlyReadSeq(encodedNumber) == cv.seq
        CompressedValue.onlyReadSpType(encodedNumber) == cv.dictSeqOrSpType
        encodedNumber.length == 10
        encodedBufferNumber.getLong(1) == cv.seq
        encodedBufferNumber.get(9) == Byte.MAX_VALUE
        cvDecodeNumber.numberValue() == Byte.MAX_VALUE

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_SHORT
        def shortBytes = new byte[2]
        ByteBuffer.wrap(shortBytes).putShort(Short.MAX_VALUE)
        cv.compressedData = shortBytes
        encodedNumber = cv.encodeAsNumber()
        cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L)
        then:
        encodedNumber.length == 11
        cvDecodeNumber.numberValue() == Short.MAX_VALUE
        CompressedValue.onlyReadSpType(encodedNumber) == cv.dictSeqOrSpType

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_INT
        def intBytes = new byte[4]
        ByteBuffer.wrap(intBytes).putInt(Integer.MAX_VALUE)
        cv.compressedData = intBytes
        encodedNumber = cv.encodeAsNumber()
        cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L)
        then:
        encodedNumber.length == 13
        cvDecodeNumber.numberValue() == Integer.MAX_VALUE
        CompressedValue.onlyReadSpType(encodedNumber) == cv.dictSeqOrSpType

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_LONG
        def longBytes = new byte[8]
        ByteBuffer.wrap(longBytes).putLong(Long.MAX_VALUE)
        cv.compressedData = longBytes
        encodedNumber = cv.encodeAsNumber()
        cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L)
        then:
        encodedNumber.length == 17
        cvDecodeNumber.numberValue() == Long.MAX_VALUE
        CompressedValue.onlyReadSpType(encodedNumber) == cv.dictSeqOrSpType

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_NUM_DOUBLE
        def doubleBytes = new byte[8]
        ByteBuffer.wrap(doubleBytes).putDouble(Double.MAX_VALUE)
        cv.compressedData = doubleBytes
        encodedNumber = cv.encodeAsNumber()
        cvDecodeNumber = CompressedValue.decode(Unpooled.wrappedBuffer(encodedNumber), null, 0L)
        then:
        encodedNumber.length == 17
        cvDecodeNumber.numberValue() == Double.MAX_VALUE
        CompressedValue.onlyReadSpType(encodedNumber) == cv.dictSeqOrSpType

        when:
        boolean exception = false
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cv.compressedData = new byte[10]
        try {
            cv.encodeAsNumber()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            cv.numberValue()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        def encodedShortString = cv.encodeAsShortString()
        def encodedBufferShortString = ByteBuffer.wrap(encodedShortString)
        then:
        // 1 + 8 seq long + 10 compressed data
        encodedShortString.length == 19
        encodedBufferShortString.getLong(1) == cv.seq
        encodedBufferShortString.slice(9, 10) == ByteBuffer.wrap(cv.compressedData)
        CompressedValue.encodeAsShortString(1L, new byte[10]).length == 19
        CompressedValue.getSeqFromNumberOrShortStringEncodedBytes(new byte[10]) == 0

        when:
        cv.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        cv.setCompressedDataAsBigString(890L, Dict.SELF_ZSTD_DICT_SEQ)
        def encodedBigStringMeta = cv.encodeAsBigStringShort(890L, Dict.SELF_ZSTD_DICT_SEQ)
        def cvDecodeBigStringMeta = CompressedValue.decode(Unpooled.wrappedBuffer(encodedBigStringMeta), null, 0L)
        def bufferBigStringMeta = ByteBuffer.wrap(cvDecodeBigStringMeta.compressedData)
        then:
        cv.isUseDict()
        cv.getBigStringMetaUuid() == 890L
        cvDecodeBigStringMeta.isBigString()
        bufferBigStringMeta.getLong() == 890L
        // dict seq
        bufferBigStringMeta.getInt() == Dict.SELF_ZSTD_DICT_SEQ
        CompressedValue.onlyReadSpType(encodedBigStringMeta) == CompressedValue.SP_TYPE_BIG_STRING
    }

    def 'test encode corner case'() {
        given:
        def keyBytes = 'abc'.bytes

        def cv = new CompressedValue()
        cv.seq = 123L
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.keyHash = KeyHash.hash(keyBytes)
        cv.compressedData = new byte[10]

        when:
        def encoded = cv.encode()
        def cvDecode = CompressedValue.decode(Unpooled.wrappedBuffer(encoded), keyBytes, cv.keyHash)
        then:
        cvDecode.seq == cv.seq

        when:
        boolean exception = false
        def keyBytesNotMatch = 'abcd'.bytes
        try {
            CompressedValue.decode(Unpooled.wrappedBuffer(encoded), keyBytesNotMatch, 0L)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cv.compressedData = null
        encoded = cv.encode()
        cvDecode = CompressedValue.decode(Unpooled.wrappedBuffer(encoded), keyBytes, cv.keyHash)
        then:
        cvDecode.seq == cv.seq

        when:
        exception = false
        try {
            CompressedValue.decode(Unpooled.wrappedBuffer(encoded), keyBytesNotMatch, 0L)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test compress'() {
        given:
        def rawBytes = ('111112222233333' * 10).bytes

        when:
        def cv1 = CompressedValue.compress(rawBytes, null)
        def cv2 = CompressedValue.compress(rawBytes, Dict.SELF_ZSTD_DICT)
        cv1.dictSeqOrSpType = Dict.SELF_ZSTD_DICT_SEQ
        then:
        cv1.compressedLength < rawBytes.length
        cv2.compressedLength < rawBytes.length
        !cv1.isIgnoreCompression(rawBytes)
        cv1.uncompressedLength == rawBytes.length

        when:
        def rawBytesDecompressed2 = cv1.decompress(null)
        then:
        rawBytes == rawBytesDecompressed2

        when:
        def rawBytesDecompressed3 = cv1.decompress(Dict.SELF_ZSTD_DICT)
        then:
        rawBytes == rawBytesDecompressed3

        when:
        def rawBytes2 = '1234'.bytes
        // will not compress
        def cv3 = CompressedValue.compress(rawBytes2, null)
        then:
        cv3.isIgnoreCompression(rawBytes2)
        !cv3.isIgnoreCompression('12345'.bytes)
        !cv3.isIgnoreCompression('1235'.bytes)

        when:
        def snowFlake = new SnowFlake(1, 1)
        def job = new TrainSampleJob((byte) 0)
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024
        TrainSampleJob.keyPrefixOrSuffixGroupList = ['key:']
        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
        11.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), rawBytes)
        }
        job.resetSampleToTrainList(sampleToTrainList)
        def result = job.train()
        def dict = result.cacheDict().get('key:')
        dict.initCtx()
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def cv4 = CompressedValue.compress(rawBytes, dict)
        then:
        cv4.compressedLength < rawBytes.length

        when:
        def rawBytesDecompressed = cv4.decompress(dict)
        then:
        rawBytes == rawBytesDecompressed

        cleanup:
        dict.closeCtx()
    }
}
