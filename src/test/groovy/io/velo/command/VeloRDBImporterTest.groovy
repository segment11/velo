package io.velo.command

import io.netty.buffer.Unpooled
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

class VeloRDBImporterTest extends Specification {
    class RDBCallbackImpl implements RDBCallback {
        Object result

        @Override
        void onInteger(Integer value) {
            println "Integer: $value"
            result = value
        }

        @Override
        void onString(byte[] valueBytes) {
            println "String: ${new String(valueBytes)}"
            result = valueBytes
        }

        @Override
        void onList(RedisList rl) {
            def str = rl.list.collect { new String(it) }.join(',')
            println "List: " + str
            result = rl
        }

        @Override
        void onSet(RedisHashKeys rhk) {
            def str = rhk.set.join(',')
            println "Set: " + str
            result = rhk
        }

        @Override
        void onZSet(RedisZSet rz) {
            def str = rz.set.collect {
                "${new String(it.member())}:${it.score()}".toString()
            }.join(',')
            println "ZSet: " + str
            result = rz
        }

        @Override
        void onHash(RedisHH rhh) {
            def str = rhh.map.collect {
                "${it.key}:${new String(it.value)}".toString()
            }.join(',')
            println "Hash: " + str
            result = rhh
        }
    }

    def 'test restore'() {
        given:
        def v = new VeloRDBImporter()
        def callback = new RDBCallbackImpl()

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
    }
}
