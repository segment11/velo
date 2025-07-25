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

        when:
        def quickListBytes = new byte[]{
                18, 1, 2, 13, 13, 0, 0, 0, 3, 0, 3, 1, 2, 1, 1, 1, -1, 11, 0, -41, 54, 8, 47, -114, -101, -61, 115
        }
        v.restore(Unpooled.wrappedBuffer(quickListBytes), callback)
        then:
        callback.result instanceof RedisList
        (callback.result as RedisList).size() == 3

        when:
        def quickListBytes2 = new byte[]{
                18, 1, 2, -61, 27, 58, 7, 58, 0, 0, 0, 3, 0, -86, 120, -32, 10, 0, 0, 121, -32, 12, 0, 8, 43, 123,
                1, -125, 97, 98, 99, 4, -1, 11, 0, -97, -87, -115, -24, 84, 76, 89, -41
        }
        v.restore(Unpooled.wrappedBuffer(quickListBytes2), callback)
        then:
        callback.result instanceof RedisList
        (callback.result as RedisList).size() == 3

        when:
        def intSetBytes = new byte[]{
                11, 16, 2, 0, 0, 0, 4, 0, 0, 0, 1, 0, 2, 0, 3, 0, 57, 48, 11, 0, 35, 37, -93, -95, 90, -94, 52, -1
        }
        v.restore(Unpooled.wrappedBuffer(intSetBytes), callback)
        then:
        callback.result instanceof RedisHashKeys
        (callback.result as RedisHashKeys).set.size() == 4

        when:
        def listPackSetBytes = new byte[]{
                20, 14, 14, 0, 0, 0, 2, 0, -125, 97, 98, 99, 4, 123, 1, -1, 11, 0, -88, 69, 96, -99, 43, -117, 76,
                105
        }
        v.restore(Unpooled.wrappedBuffer(listPackSetBytes), callback)
        then:
        callback.result instanceof RedisHashKeys
        (callback.result as RedisHashKeys).set.size() == 2

        when:
        def listPackHashBytes = new byte[]{
                16, 22, 22, 0, 0, 0, 4, 0, -125, 97, 98, 99, 4, 123, 1, -125, 120, 121, 122, 4, -63, -56, 2, -1, 11,
                0, -45, -126, 97, -43, 63, 50, 75, -103
        }
        v.restore(Unpooled.wrappedBuffer(listPackHashBytes), callback)
        then:
        callback.result instanceof RedisHH
        (callback.result as RedisHH).map.size() == 2

        when:
        def listPackZSetBytes = new byte[]{
                17, 21, 21, 0, 0, 0, 4, 0, -125, 97, 97, 97, 4, 1, 1, -125, 98, 98, 98, 4, 2, 1, -1, 11, 0, 68, 51,
                25, 105, 59, 101, 71, -54
        }
        v.restore(Unpooled.wrappedBuffer(listPackZSetBytes), callback)
        then:
        callback.result instanceof RedisZSet
        (callback.result as RedisZSet).set.size() == 2
    }

    def 'test dump string'() {
        given:
        def v = new VeloRDBImporter()
        def callback = new RDBCallbackImpl()

        when:
        def bytesByte = VeloRDBImporter.dumpString('12'.bytes)
        v.restore(Unpooled.wrappedBuffer(bytesByte), callback)
        then:
        callback.result as byte[] == '12'.bytes

        when:
        def bytesShort = VeloRDBImporter.dumpString('123'.bytes)
        v.restore(Unpooled.wrappedBuffer(bytesShort), callback)
        then:
        callback.result as byte[] == '123'.bytes

        when:
        def bytesInt = VeloRDBImporter.dumpString('1234'.bytes)
        v.restore(Unpooled.wrappedBuffer(bytesInt), callback)
        then:
        callback.result as byte[] == '1234'.bytes

        when:
        def bytesLong = VeloRDBImporter.dumpString('1234567890'.bytes)
        v.restore(Unpooled.wrappedBuffer(bytesLong), callback)
        then:
        callback.result as byte[] == '1234567890'.bytes

        when:
        def bytes = VeloRDBImporter.dumpString('hello world'.bytes)
        v.restore(Unpooled.wrappedBuffer(bytes), callback)
        then:
        callback.result as byte[] == 'hello world'.bytes
    }

    def 'test dump types'() {
        given:
        def v = new VeloRDBImporter()
        def callback = new RDBCallbackImpl()

        when:
        // set
        def rhk = new RedisHashKeys()
        rhk.add('member0')
        rhk.add('member1')
        def bytesSet = VeloRDBImporter.dumpSet(rhk)
        v.restore(Unpooled.wrappedBuffer(bytesSet), callback)
        then:
        callback.result instanceof RedisHashKeys
        (callback.result as RedisHashKeys).set.size() == 2

        when:
        // hash
        def rhh = new RedisHH()
        rhh.put('field0', 'value0'.bytes)
        rhh.put('field1', 'value1'.bytes)
        def bytesHash = VeloRDBImporter.dumpHash(rhh)
        v.restore(Unpooled.wrappedBuffer(bytesHash), callback)
        then:
        callback.result instanceof RedisHH
        (callback.result as RedisHH).map.size() == 2

        when:
        // list
        def rl = new RedisList()
        rl.addLast('value0'.bytes)
        rl.addLast('value1'.bytes)
        def bytesList = VeloRDBImporter.dumpList(rl)
        v.restore(Unpooled.wrappedBuffer(bytesList), callback)
        then:
        callback.result instanceof RedisList
        (callback.result as RedisList).size() == 2

        when:
        // zset
        def rz = new RedisZSet()
        rz.add(0.1, 'member0')
        rz.add(0.2, 'member1')
        def bytesZSet = VeloRDBImporter.dumpZSet(rz)
        v.restore(Unpooled.wrappedBuffer(bytesZSet), callback)
        then:
        callback.result instanceof RedisZSet
        (callback.result as RedisZSet).set.size() == 2
    }
}
