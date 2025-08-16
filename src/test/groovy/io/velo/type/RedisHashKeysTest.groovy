package io.velo.type

import io.velo.BaseCommand
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
        buffer.putShort(RedisHashKeys.HEADER_LENGTH, (short) 0)
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
        RedisHashKeys.iterate(encoded, false) { bytes, i ->
            list << new String(bytes)
            return false
        }
        then:
        list == ['field1', 'field2']

        when:
        list.clear()
        RedisHashKeys.iterate(encoded, true) { bytes, i ->
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
        encoded4.length == RedisHashKeys.HEADER_LENGTH + 5 * (2 + 36)
    }
}
