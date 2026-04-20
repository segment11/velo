package io.velo.repl.incremental

import io.velo.CompressedValue
import io.velo.KeyHash
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XBigStringsTest extends Specification {
    def 'test encode and decode'() {
        given:
        def uuid = 1L
        def key = 'test-big-string-key'
        def cv = new CompressedValue()
        cv.seq = 1234L
        cv.keyHash = KeyHash.hash(key.bytes)
        cv.expireAt = System.currentTimeMillis() + 1000
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(1234L, CompressedValue.NULL_DICT_SEQ)
        def cvEncoded = cv.encode()

        def xBigStrings = new XBigStrings(uuid, 0, cv.keyHash, key, cvEncoded)

        expect:
        xBigStrings.type() == BinlogContent.Type.big_strings

        when:
        def encoded = xBigStrings.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def xBigStrings2 = XBigStrings.decodeFrom(buffer)
        then:
        xBigStrings2.encodedLength() == encoded.length
        xBigStrings2.uuid == xBigStrings.uuid
        xBigStrings2.bucketIndex == xBigStrings.bucketIndex
        xBigStrings2.keyHash == xBigStrings.keyHash
        xBigStrings2.key == xBigStrings.key
        xBigStrings2.cvEncoded == xBigStrings.cvEncoded

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 8 + 4, (CompressedValue.KEY_MAX_LENGTH + 1).shortValue())
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.putShort(1 + 4 + 8 + 4, (short) 0)
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buffer.position(1)
        def keyBytesLen = key.bytes.length
        def cvEncodedLenOffset = 1 + 4 + 8 + 4 + 8 + 2 + keyBytesLen
        buffer.putInt(cvEncodedLenOffset, -1)
        buffer.position(1)
        try {
            xBigStrings.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        xBigStrings.apply(slot, replPair)
        then:
        replPair.toFetchBigStringIdList.size() == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test apply should work when slot is readonly during slave replay'() {
        given:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.readonly = true

        def key = 'test-big-string-readonly-replay'
        def uuid = 1L
        def cv = new CompressedValue()
        cv.seq = 1234L
        cv.keyHash = KeyHash.hash(key.bytes)
        cv.expireAt = CompressedValue.NO_EXPIRE
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(uuid, CompressedValue.NULL_DICT_SEQ)
        def xBigStrings = new XBigStrings(uuid, 0, cv.keyHash, key, cv.encode())
        def replPair = ReplPairTest.mockAsSlave()

        when:
        xBigStrings.apply(slot, replPair)

        then:
        noExceptionThrown()
        replPair.toFetchBigStringIdList.size() == 1

        cleanup:
        localPersist.fixSlotThreadId((short) 0, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
