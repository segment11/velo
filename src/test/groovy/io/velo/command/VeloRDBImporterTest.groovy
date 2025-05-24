package io.velo.command

import io.netty.buffer.Unpooled
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

class VeloRDBImporterTest extends Specification {

    def 'test restore'() {
        given:
        def v = new VeloRDBImporter()
        def callback = new RDBCallback() {
            @Override
            void onInteger(Integer value) {
                println "Integer: $value"
            }

            @Override
            void onString(byte[] valueBytes) {
                println "String: ${new String(valueBytes)}"
            }

            @Override
            void onList(RedisList rl) {
                println "List: $rl"
            }

            @Override
            void onSet(RedisHashKeys rhk) {
                println "Set: $rhk"
            }

            @Override
            void onZSet(RedisZSet rz) {
                println "ZSet: $rz"
            }

            @Override
            void onHash(RedisHH rhh) {
                println "Hash: $rhh"
            }
        }

        when:
        def aBytes = new byte[]{
                (byte) 0x00, (byte) 0xC2, (byte) 0xA0, (byte) 0x86,
                (byte) 0x01, (byte) 0x00, (byte) 0x0B, (byte) 0x00,
                (byte) 0x98, (byte) 0xFB, (byte) 0xBE, '#',
                (byte) 0x92, (byte) 0xB0, 'O', (byte) 0xFD
        }
        v.restore(Unpooled.wrappedBuffer(aBytes), callback)
        then:
        1 == 1

        when:
        def bBytes = new byte[]{
                (byte) 0x00, (byte) 0xC1, ',',
                (byte) 0x01, (byte) 0x0B, (byte) 0x00,
                (byte) 0xFC, (byte) 0x9D, (byte) 0xDC,
                (byte) 0x81, (byte) 0x98, (byte) 0x1D,
                (byte) 0xB5, (byte) 0x99
        }
        v.restore(Unpooled.wrappedBuffer(bBytes), callback)
        then:
        1 == 1

        when:
        def cBytes = new byte[]{
                (byte) 0x00, (byte) 0xC2, (byte) 0xA0, (byte) 0x86,
                (byte) 0x01, (byte) 0x00, (byte) 0x0B, (byte) 0x00,
                (byte) 0x98, (byte) 0xFB, (byte) 0xBE, '#',
                (byte) 0x92, (byte) 0xB0, 'O', (byte) 0xFD
        }
        v.restore(Unpooled.wrappedBuffer(cBytes), callback)
        then:
        1 == 1

        when:
        def dBytes = new byte[]{
                (byte) 0x00,
                '\n',
                '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
                (byte) 0x0B, (byte) 0x00,
                (byte) 0xBB, (byte) 0xB5, (byte) 0x7F, (byte) 0xEA,
                '"',
                (byte) 0xCC, (byte) 0x06,
                '['
        }
        v.restore(Unpooled.wrappedBuffer(dBytes), callback)
        then:
        1 == 1

        when:
        def lzfBytes = new byte[]{
                0, -61, 29, 64, -112, 10, 49, 50, 51, 52, 53, 54, 55, 56, 57, 48, 49, -32,
                40, 9, 6, 97, 98, 99, 100, 101, 102, 103, -32, 66, 6, 1, 102, 103, 11, 0, 92,
                -50, -102, -30, -48, 48, -109, 59
        }
        v.restore(Unpooled.wrappedBuffer(lzfBytes), callback)
        then:
        1 == 1
    }
}
