package io.velo.repl

import io.velo.CompressedValue
import io.velo.Dict
import io.velo.KeyHash
import io.velo.persist.Mock
import io.velo.repl.incremental.*
import spock.lang.Specification

import java.nio.ByteBuffer

class BinlogContentTest extends Specification {
    def 'test type'() {
        given:
        def one = BinlogContent.Type.fromCode(BinlogContent.Type.wal.code())

        expect:
        one == BinlogContent.Type.wal

        when:
        boolean exception = false
        try {
            BinlogContent.Type.fromCode((byte) 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def v = Mock.prepareValueList(1)[0]
        def xWalV = new XWalV(v, true)
        def encoded = xWalV.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        def xWalV11 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XWalV
        then:
        xWalV11.v.encode(false) == v.encode(false)

        when:
        def uuid = 1L
        def key = 'test-big-string-key'
        def cv = new CompressedValue()
        cv.keyHash = KeyHash.hash(key.bytes)
        def cvEncoded = cv.encode()

        def xBigStrings = new XBigStrings(uuid, 0, 1L, key, cvEncoded)
        encoded = xBigStrings.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xBigStrings2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XBigStrings
        then:
        xBigStrings2.encodedLength() == encoded.length

        when:
        def keyPrefix = 'key:'
        def dictBytes = new byte[300]
        def dict = new Dict(dictBytes)
        def xDict = new XDict(keyPrefix, dict)
        encoded = xDict.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xDict2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XDict
        then:
        xDict2.encodedLength() == encoded.length

        when:
        def xFlush = new XFlush()
        encoded = xFlush.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xFlush2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XFlush
        then:
        xFlush2.encodedLength() == encoded.length

        when:
        def xAclUpdate = new XAclUpdate('user default on ~* &* +@all')
        encoded = xAclUpdate.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xAclUpdate2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XAclUpdate
        then:
        xAclUpdate2.encodedLength() == encoded.length

        when:
        def xSkipApply = new XSkipApply(100L)
        encoded = xSkipApply.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xSkipApply2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XSkipApply
        then:
        xSkipApply2.encodedLength() == encoded.length
        xSkipApply2.seq == 100L

        when:
        def xUpdateSeq = new XUpdateSeq(200L, System.currentTimeMillis())
        encoded = xUpdateSeq.encodeWithType()
        buffer = ByteBuffer.wrap(encoded)
        def xUpdateSeq2 = BinlogContent.Type.fromCode(buffer.get()).decodeFrom(buffer) as XUpdateSeq
        then:
        xUpdateSeq2.encodedLength() == encoded.length
        xUpdateSeq2.seq == 200L

        when:
        def xDynConfig = new XDynConfig()
        def xDynConfig2 = BinlogContent.Type.dyn_config.decodeFrom(ByteBuffer.wrap(new byte[0]))
        then:
        xDynConfig.encodedLength() == 0
        xDynConfig2 == null
    }
}
