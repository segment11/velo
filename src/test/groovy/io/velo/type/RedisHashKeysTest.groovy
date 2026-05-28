package io.velo.type

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.KeyHash
import spock.lang.Specification

import java.nio.ByteBuffer

class RedisHashKeysTest extends Specification {
    private static short slot(String key) {
        BaseCommand.slot(key.bytes, 128).slot()
    }

    def 'key generate'() {
        expect:
        RedisHashKeys.keysKey('test') == 'h_k_{test}'
        RedisHashKeys.fieldKey('test', 'name') == 'h_f_{test}.name'

        slot('test') == slot('h_k_{test}')
        slot('test') == slot('h_f_{test}.name')
    }

    def 'set'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        then:
        rhk.contains('field1')
        rhk.size() == 1

        when:
        rhk.add('field2')
        then:
        rhk.contains('field2')
        rhk.size() == 2

        when:
        rhk.remove('field1')
        then:
        !rhk.contains('field1')
        rhk.size() == 1

        rhk.set == new HashSet(['field2'])
    }

    def 'encode'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')
        def encoded = rhk.encode()
        encoded = rhk.encodeButDoNotCompress()
        def rhk2 = RedisHashKeys.decode(encoded)
        then:
        rhk2.contains('field1')
        rhk2.contains('field2')
        rhk2.size() == 2
        RedisHashKeys.getSizeWithoutDecode(encoded) == 2
    }

    def 'decode crc32 not match'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')
        def encoded = rhk.encode()
        encoded[RedisHashKeys.HEADER_LENGTH - 4] = 0
        boolean exception = false
        try {
            RedisHashKeys.decode(encoded)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'encode size 0'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        def encoded = rhk.encode()
        def rhk2 = RedisHashKeys.decode(encoded, false)
        then:
        rhk2.size() == 0

        when:
        rhk.add('field1')
        def encoded2 = rhk.encode()
        def rhk3 = RedisHashKeys.decode(encoded2, false)
        then:
        rhk3.size() == 1
    }

    def 'decode illegal length'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')
        def encoded = rhk.encode()
        def buffer = ByteBuffer.wrap(encoded)
        // New format: after header(14), first field starts with expireAt(8), then fieldLen at offset 22
        buffer.putShort(RedisHashKeys.HEADER_LENGTH + 8, (short) 0)
        boolean exception = false
        try {
            RedisHashKeys.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'iterate'() {
        given:
        def rhk = new RedisHashKeys()

        when:
        rhk.add('field1')
        rhk.add('field2')
        def encoded = rhk.encode()
        List<String> list = []
        RedisHashKeys.iterate(encoded, false) { bytes, i, expireAt ->
            list << new String(bytes)
            return false
        }
        then:
        list == ['field1', 'field2']

        when:
        list.clear()
        RedisHashKeys.iterate(encoded, true) { bytes, i, expireAt ->
            list << new String(bytes)
            if ('field1' == new String(bytes)) {
                return true
            }
            return false
        }
        then:
        list == ['field1']
    }

    def 'test compress'() {
        given:
        def rhk = new RedisHashKeys()
        def longString = 'aaaaabbbbbccccc' * 10

        when:
        RedisHH.PREFER_COMPRESS_RATIO = 0.9
        10.times {
            rhk.add(longString + it)
        }
        def encoded = rhk.encode()
        def rhk2 = RedisHashKeys.decode(encoded)
        then:
        rhk2.size() == 10
        rhk.set == rhk2.set

        when:
        // compress ratio too big, ignore
        RedisHH.PREFER_COMPRESS_RATIO = 0.1
        def rhk4 = new RedisHashKeys()
        5.times {
            rhk4.add(UUID.randomUUID().toString())
        }
        def encoded4 = rhk4.encode()
        then:
        // uuid length is 36
        // New format per field: expireAt(8) + fieldLen(2) + fieldBytes(36) = 46 bytes
        encoded4.length == RedisHashKeys.HEADER_LENGTH + 5 * (8 + 2 + 36)
    }

    def 'test encode throws when size exceeds Short.MAX_VALUE'() {
        given:
        def rhk = new RedisHashKeys()
        int sizeToTest = ((int) Short.MAX_VALUE) + 1

        when:
        sizeToTest.times { i ->
            rhk.add("field" + i)
        }

        then:
        rhk.size() == sizeToTest

        when:
        rhk.encode()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds')
    }

    def 'test decode throws on oversized positive field length'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        def encoded = rhk.encode()
        def buffer = ByteBuffer.wrap(encoded)
        // New format: after header comes expireAt(8), then fieldLen at offset 14+8=22
        buffer.putShort(RedisHashKeys.HEADER_LENGTH + 8, (short) 10000)

        when:
        RedisHashKeys.decode(encoded, false)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds remaining buffer')
    }

    def 'test iterate throws on oversized positive field length'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        def encoded = rhk.encode()
        def buffer = ByteBuffer.wrap(encoded)
        // New format: after header comes expireAt(8), then fieldLen at offset 14+8=22
        buffer.putShort(RedisHashKeys.HEADER_LENGTH + 8, (short) 10000)

        when:
        RedisHashKeys.iterate(encoded, false) { bytes, i, expireAt -> false }

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds remaining buffer')
    }

    def 'test ttl cache model'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.add('field3')

        expect:
        rhk.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE

        when:
        rhk.putCachedExpireAt('field1', 1000L)
        then:
        rhk.getCachedExpireAt('field1') == 1000L

        when:
        rhk.putCachedExpireAt('field2', 2000L)
        then:
        rhk.getCachedExpireAt('field2') == 2000L

        expect:
        rhk.isLiveByCache('field1', 500L)  // at 500ms, not yet expired (expires at 1000ms)
        rhk.isLiveByCache('field1', 1000L)  // at 1000ms, exactly at expiry, still live
        !rhk.isLiveByCache('field1', 1500L) // at 1500ms, expired
        rhk.isLiveByCache('field2', 1500L)  // at 1500ms, not yet expired (expires at 2000ms)
        !rhk.isLiveByCache('field2', 2500L) // at 2500ms, expired

        when:
        def live = rhk.liveFieldsByCache(500L)
        then:
        live.containsAll(['field1', 'field3']) // all live at 500ms

        when:
        live = rhk.liveFieldsByCache(1500L)
        then:
        live.containsAll(['field2', 'field3']) // field1 expired at 1500

        when:
        rhk.clearCachedExpireAt('field1')
        then:
        rhk.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE

        when:
        rhk.putCachedExpireAt('field1', CompressedValue.NO_EXPIRE)
        then:
        rhk.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
        rhk.getCachedExpireAt('field2') == 2000L
    }

    def 'test liveFieldsByCache no-arg returns no-expire fields'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')

        expect:
        rhk.liveFieldsByCache() == ['field1', 'field2']
    }

    def 'test decode with ttl round-trip'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.putCachedExpireAt('field1', 1000L)

        when:
        def encoded = rhk.encode()
        def decoded = RedisHashKeys.decode(encoded)

        then:
        decoded.size() == 2
        decoded.getCachedExpireAt('field1') == 1000L
        decoded.getCachedExpireAt('field2') == CompressedValue.NO_EXPIRE
    }

    def 'test new format with empty ttl section round-trip'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')

        when:
        def encoded = rhk.encode()
        def decoded = RedisHashKeys.decode(encoded)

        then:
        decoded.size() == 2
        decoded.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
        decoded.getCachedExpireAt('field2') == CompressedValue.NO_EXPIRE
    }

    def 'test new format with ttl entries round-trip'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.add('field3')
        rhk.putCachedExpireAt('field1', 1000L)
        rhk.putCachedExpireAt('field2', 2000L)
        // field3 has no TTL

        when:
        def encoded = rhk.encode()
        def decoded = RedisHashKeys.decode(encoded)

        then:
        decoded.size() == 3
        decoded.getCachedExpireAt('field1') == 1000L
        decoded.getCachedExpireAt('field2') == 2000L
        decoded.getCachedExpireAt('field3') == CompressedValue.NO_EXPIRE
        decoded.isLiveByCache('field1', 500L)
        decoded.isLiveByCache('field1', 1000L)
        !decoded.isLiveByCache('field1', 1500L)
        decoded.liveFieldsByCache(500L).containsAll(['field1', 'field2', 'field3'])
        decoded.liveFieldsByCache(1500L).containsAll(['field2', 'field3'])
        decoded.liveFieldsByCache(2500L).contains('field3')
    }

    def 'test remove clears cached ttl'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.putCachedExpireAt('field1', 1000L)

        expect:
        rhk.getCachedExpireAt('field1') == 1000L

        when:
        rhk.remove('field1')

        then:
        rhk.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
        rhk.size() == 0
    }

    def 'test crc catches corrupted ttl metadata'() {
        given:
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.putCachedExpireAt('field1', 1000L)
        def encoded = rhk.encode()

        when:
        // Corrupt a byte in the TTL section
        encoded[encoded.length - 5] = (byte) (encoded[encoded.length - 5] ^ 0xFF)
        boolean exception = false
        try {
            RedisHashKeys.decode(encoded)
        } catch (IllegalStateException e) {
            exception = true
        }

        then:
        exception
    }
}
