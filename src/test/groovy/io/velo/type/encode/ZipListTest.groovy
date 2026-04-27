package io.velo.type.encode

import io.netty.buffer.Unpooled
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder

class ZipListTest extends Specification {

    def 'test ZIP_STR_32B decode reads big-endian length'() {
        given:
        def payloadBytes = ('Y' * 65536).getBytes()

        def ziplistSize = 10 + 5 + 1 + 4 + payloadBytes.length + 1
        def buf = ByteBuffer.allocate(ziplistSize).order(ByteOrder.LITTLE_ENDIAN)

        buf.putInt(ziplistSize)
        buf.putInt(0)
        buf.putShort((short) 1)

        buf.put((byte) 0)
        buf.put((byte) 0x80)
        buf.put((byte) ((payloadBytes.length >> 24) & 0xFF))
        buf.put((byte) ((payloadBytes.length >> 16) & 0xFF))
        buf.put((byte) ((payloadBytes.length >> 8) & 0xFF))
        buf.put((byte) (payloadBytes.length & 0xFF))
        buf.put(payloadBytes)

        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        List<Integer> indices = []
        ZipList.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
            indices << index
        })

        then:
        results.size() == 1
        results[0] == payloadBytes
        indices[0] == 0
    }

    def 'test ZIP_STR_06B decode still works'() {
        given:
        def payload = 'hello'
        def payloadBytes = payload.getBytes()

        def ziplistSize = 10 + 5 + 1 + payloadBytes.length + 1
        def buf = ByteBuffer.allocate(ziplistSize).order(ByteOrder.LITTLE_ENDIAN)

        buf.putInt(ziplistSize)
        buf.putInt(0)
        buf.putShort((short) 1)

        buf.put((byte) 0)
        buf.put((byte) (0 | payloadBytes.length))
        buf.put(payloadBytes)

        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        ZipList.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        new String(results[0]) == 'hello'
    }

    def 'test ZIP_STR_14B decode still works'() {
        given:
        def payloadBytes = ('X' * 300).getBytes()

        def ziplistSize = 10 + 5 + 1 + 1 + payloadBytes.length + 1
        def buf = ByteBuffer.allocate(ziplistSize).order(ByteOrder.LITTLE_ENDIAN)

        buf.putInt(ziplistSize)
        buf.putInt(0)
        buf.putShort((short) 1)

        buf.put((byte) 0)
        buf.put((byte) (0x40 | (payloadBytes.length >> 8)))
        buf.put((byte) (payloadBytes.length & 0xFF))
        buf.put(payloadBytes)

        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        ZipList.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        results[0] == payloadBytes
    }

    def 'test ZIP_STR_32B with smaller length above 14-bit range'() {
        given:
        def payloadBytes = ('Z' * 70000).getBytes()

        def ziplistSize = 10 + 5 + 1 + 4 + payloadBytes.length + 1
        def buf = ByteBuffer.allocate(ziplistSize).order(ByteOrder.LITTLE_ENDIAN)

        buf.putInt(ziplistSize)
        buf.putInt(0)
        buf.putShort((short) 1)

        buf.put((byte) 0)
        buf.put((byte) 0x80)
        buf.put((byte) ((payloadBytes.length >> 24) & 0xFF))
        buf.put((byte) ((payloadBytes.length >> 16) & 0xFF))
        buf.put((byte) ((payloadBytes.length >> 8) & 0xFF))
        buf.put((byte) (payloadBytes.length & 0xFF))
        buf.put(payloadBytes)

        buf.put((byte) 0xFF)

        def nettyBuf = Unpooled.wrappedBuffer(buf.array())

        when:
        List<byte[]> results = []
        ZipList.decode(nettyBuf, { valueBytes, index ->
            results << valueBytes
        })

        then:
        results.size() == 1
        results[0] == payloadBytes
    }
}
