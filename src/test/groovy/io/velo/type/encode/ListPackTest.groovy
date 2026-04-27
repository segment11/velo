package io.velo.type.encode

import io.netty.buffer.Unpooled
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder

class ListPackTest extends Specification {

    static byte[] encodeBackLenBytes(int len) {
        if (len <= 127) {
            return [(byte) len]
        } else if (len < 16383) {
            return [(byte) ((len >> 7) | 128), (byte) (len & 127)]
        } else if (len < 2097151) {
            return [(byte) ((len >> 14) | 128), (byte) (((len >> 7) & 127) | 128), (byte) (len & 127)]
        } else if (len < 268435455) {
            return [(byte) ((len >> 21) | 128), (byte) (((len >> 14) & 127) | 128), (byte) (((len >> 7) & 127) | 128), (byte) (len & 127)]
        } else {
            return [(byte) ((len >> 28) | 128), (byte) (((len >> 21) & 127) | 128), (byte) (((len >> 14) & 127) | 128), (byte) (((len >> 7) & 127) | 128), (byte) (len & 127)]
        }
    }

    static byte[] buildListPackWith12BitString(byte[] payloadBytes) {
        def elemHeaderSize = 2
        def elemTotalSize = elemHeaderSize + payloadBytes.length
        def backLenBytes = encodeBackLenBytes(elemTotalSize)

        def totalBytes = 4 + 2 + elemHeaderSize + payloadBytes.length + backLenBytes.length + 1
        def buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN)
        buf.putInt(totalBytes)
        buf.putShort((short) 1)
        buf.put((byte) (0xE0 | (payloadBytes.length >> 8)))
        buf.put((byte) (payloadBytes.length & 0xFF))
        buf.put(payloadBytes)
        buf.put(backLenBytes)
        buf.put((byte) 0xFF)
        return buf.array()
    }

    def 'test 12-bit string decode with length 300'() {
        given:
        def payload = 'A' * 300
        def listPackBytes = buildListPackWith12BitString(payload.getBytes())
        def nettyBuf = Unpooled.wrappedBuffer(listPackBytes)

        when:
        List<byte[]> results = []
        List<Integer> indices = []
        ListPack.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
            indices << index
        })

        then:
        results.size() == 1
        new String(results[0]) == payload
        indices[0] == 0
    }

    def 'test 12-bit string decode with length 64 (minimum for 12-bit)'() {
        given:
        def payloadBytes = new byte[64]
        Arrays.fill(payloadBytes, (byte) 'X')
        def listPackBytes = buildListPackWith12BitString(payloadBytes)
        def nettyBuf = Unpooled.wrappedBuffer(listPackBytes)

        when:
        List<byte[]> results = []
        ListPack.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        results[0].length == 64
        results[0] == payloadBytes
    }

    def 'test 12-bit string decode with length 4095 (maximum)'() {
        given:
        def payloadBytes = new byte[4095]
        Arrays.fill(payloadBytes, (byte) 'Z')
        def listPackBytes = buildListPackWith12BitString(payloadBytes)
        def nettyBuf = Unpooled.wrappedBuffer(listPackBytes)

        when:
        List<byte[]> results = []
        ListPack.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        results[0].length == 4095
        results[0] == payloadBytes
    }

    def 'test 6-bit string decode still works'() {
        given:
        def payload = 'hello'
        def payloadBytes = payload.getBytes()

        def elemHeaderSize = 1
        def elemTotalSize = elemHeaderSize + payloadBytes.length
        def backLenBytes = encodeBackLenBytes(elemTotalSize)
        def totalBytes = 4 + 2 + elemHeaderSize + payloadBytes.length + backLenBytes.length + 1

        def buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN)
        buf.putInt(totalBytes)
        buf.putShort((short) 1)
        buf.put((byte) (0x80 | payloadBytes.length))
        buf.put(payloadBytes)
        buf.put(backLenBytes)
        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        ListPack.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        new String(results[0]) == 'hello'
    }

    def 'test 12-bit string with multiple entries'() {
        given:
        def payload1 = 'B' * 100
        def payload2 = 'C' * 200
        def entries = [payload1.getBytes(), payload2.getBytes()]

        def elemDataList = []
        for (def payloadBytes : entries) {
            def elemHeaderSize = 2
            def elemTotalSize = elemHeaderSize + payloadBytes.length
            def backLenBytes = encodeBackLenBytes(elemTotalSize)
            def elemBytes = new byte[elemHeaderSize + payloadBytes.length + backLenBytes.length]
            elemBytes[0] = (byte) (0xE0 | (payloadBytes.length >> 8))
            elemBytes[1] = (byte) (payloadBytes.length & 0xFF)
            System.arraycopy(payloadBytes, 0, elemBytes, 2, payloadBytes.length)
            System.arraycopy(backLenBytes, 0, elemBytes, 2 + payloadBytes.length, backLenBytes.length)
            elemDataList << elemBytes
        }

        def totalBytes = 4 + 2
        for (def e : elemDataList) totalBytes += e.length
        totalBytes += 1

        def buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN)
        buf.putInt(totalBytes)
        buf.putShort((short) entries.size())
        for (def e : elemDataList) buf.put(e)
        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        List<Integer> indices = []
        ListPack.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
            indices << index
        })

        then:
        results.size() == 2
        new String(results[0]) == payload1
        new String(results[1]) == payload2
        indices == [0, 1]
    }
}
