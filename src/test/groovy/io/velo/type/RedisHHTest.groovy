package io.velo.type

import io.velo.*
import io.velo.persist.Consts
import spock.lang.Specification

import java.nio.ByteBuffer

class RedisHHTest extends Specification {
    def 'hash'() {
        given:
        def rh = new RedisHH()

        when:
        def expireAt = System.currentTimeMillis() + 1000 * 10
        rh.put('name', 'zhangsan'.bytes)
        rh.put('name', 'zhangsan'.bytes, expireAt)
        rh.put('age', '20'.bytes)
        then:
        rh.get('name') == 'zhangsan'.bytes
        rh.get('age') == '20'.bytes
        rh.getExpireAt('name') == expireAt
        rh.size() == 2
        rh.remove('name')
        !rh.remove('name')
        rh.size() == 1

        when:
        rh.putExpireAt('name', expireAt + 1000 * 10)
        then:
        rh.getExpireAt('name') == expireAt + 1000 * 10

        when:
        rh.putAll(['name2': 'lisi'.bytes, 'age2': '30'.bytes])
        then:
        rh.get('name2') == 'lisi'.bytes
        rh.get('age2') == '30'.bytes
        rh.size() == 3
    }

    def 'encode'() {
        given:
        def rh = new RedisHH()

        when:
        rh.put('name', 'zhangsan'.bytes, System.currentTimeMillis() + 1000 * 2)
        rh.put('age', '20'.bytes)
        def encoded = rh.encode()
        encoded = rh.encodeButDoNotCompress()
        def rh2 = RedisHH.decode(encoded)
        then:
        rh2.get('name') == 'zhangsan'.bytes
        rh2.get('age') == '20'.bytes
        rh2.size() == 2
        rh2.map.containsKey('name')
        rh2.map.containsKey('age')

        when:
        int countOnlyFindName = 0
        RedisHH.iterate(encoded, true) { key, value, expireAt ->
            countOnlyFindName++
            return key == 'name'
        }
        then:
        countOnlyFindName == 1

        when:
        // wait field name expire
        Thread.sleep(1000 * 2 + 100)
        rh2 = RedisHH.decode(encoded)
        then:
        rh2.get('name') == null
        rh2.get('age') == '20'.bytes
        rh2.size() == 1

        when:
        int countIfFindName = 0
        rh2.iterate { key, value, expireAt ->
            if (key == 'name') {
                countIfFindName++
                return true
            }
            return false
        }
        then:
        countIfFindName == 0

        when:
        int countIfFindAge = 0
        rh2.iterate { key, value, expireAt ->
            if (key == 'age') {
                countIfFindAge++
                return true
            }
            return false
        }
        then:
        countIfFindAge == 1
    }

    def 'decode crc32 not match'() {
        given:
        def rh = new RedisHH()

        when:
        rh.put('name', 'zhangsan'.bytes)
        rh.put('age', '20'.bytes)
        def encoded = rh.encode()
        encoded[RedisHH.HEADER_LENGTH - 4] = 0
        boolean exception = false
        try {
            RedisHH.decode(encoded)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'encode size 0'() {
        given:
        def rh = new RedisHH()

        when:
        def encoded = rh.encode()
        def rh2 = RedisHH.decode(encoded, false)
        then:
        rh2.size() == 0
    }

    def 'key or value length too long'() {
        given:
        def rh = new RedisHH()

        when:
        def key = 'a' * 1024
        def value = 'b'
        boolean exception = false
        try {
            rh.put(key, value.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        key = 'a'
        value = 'b' * (1024 * 1024 * 2)
        boolean exception2 = false
        try {
            rh.put(key, value.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2
    }

    def 'decode key or value length error'() {
        given:
        def rh = new RedisHH()

        when:
        def key = 'a'
        def value = 'b'
        rh.put(key, value.bytes)
        def encoded = rh.encode()
        def buffer = ByteBuffer.wrap(encoded)
        // first key length
        buffer.putShort(RedisHH.HEADER_LENGTH + 8, (short) (CompressedValue.KEY_MAX_LENGTH + 1))
        boolean exception = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(RedisHH.HEADER_LENGTH + 8, (short) -1)
        exception = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(RedisHH.HEADER_LENGTH + 8, (short) 1)
        buffer.putInt(RedisHH.HEADER_LENGTH + 8 + 2 + 1, CompressedValue.VALUE_MAX_LENGTH + 1)
        boolean exception2 = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2

        when:
        buffer.putInt(RedisHH.HEADER_LENGTH + 8 + 2 + 1, -1)
        exception2 = false
        try {
            RedisHH.decode(encoded, false)
        } catch (IllegalStateException e) {
            println e.message
            exception2 = true
        }
        then:
        exception2
    }

    def 'test compress'() {
        given:
        def rh = new RedisHH()
        def longStringBytes = ('aaaaabbbbbccccc' * 10).bytes

        when:
        RedisHH.PREFER_COMPRESS_RATIO = 0.9
        10.times {
            rh.put('field' + it, longStringBytes)
        }
        def encoded = rh.encode()
        def rh2 = RedisHH.decode(encoded)
        then:
        rh2.size() == 10
        (0..<10).every {
            rh2.get('field' + it) == longStringBytes
        }

        when:
        def job = new TrainSampleJob((byte) 0)
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024

        def snowFlake = new SnowFlake(0, 0)

        TrainSampleJob.keyPrefixOrSuffixGroupList = ['key:']
        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
        11.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), longStringBytes)
        }

        job.resetSampleToTrainList(sampleToTrainList)
        def result = job.train()
        def dictTrained = result.cacheDict().get('key:')

        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)
        dictMap.putDict('key:', dictTrained)

        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def encoded2 = rh.encode(dictTrained)
        def rh3 = RedisHH.decode(encoded2, false)
        then:
        rh3.size() == 10
        (0..<10).every {
            rh3.get('field' + it) == longStringBytes
        }

        when:
        boolean exception = false
        dictMap.clearAll()
        try {
            RedisHH.decode(encoded2, false)
        } catch (DictMissingException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        // compress ratio too big, ignore
        RedisHH.PREFER_COMPRESS_RATIO = 0.1
        def rh4 = new RedisHH()
        5.times {
            rh4.put('field' + it, UUID.randomUUID().toString().bytes)
        }
        def encoded4 = rh4.encode()
        then:
        // uuid length is 36
        encoded4.length == RedisHH.HEADER_LENGTH + 5 * (8 + 2 + 6 + 4 + 36)

        cleanup:
        dictMap.cleanUp()
        Consts.testDir.deleteDir()
    }

    def 'test charset encode'() {
        given:
        def rh = new RedisHH()
        def keyBytes = '你好'.bytes
        def bytes = '你好我也好'.bytes
        rh.put(new String(keyBytes), bytes)

        when:
        def encoded = rh.encode()
        def rh2 = RedisHH.decode(encoded)
        then:
        rh2.size() == 1
        rh2.get(new String(keyBytes)) == bytes
    }
}
