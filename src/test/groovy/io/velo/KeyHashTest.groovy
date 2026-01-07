package io.velo


import spock.lang.Specification

class KeyHashTest extends Specification {
    def 'test hash'() {
        given:
        def keyList = (0..<10).collect {
            UUID.randomUUID().toString()
        }

        when:
        Set<Integer> hash32Set = []
        Set<Long> hash64Set = []
        keyList.each {
            hash32Set.add(KeyHash.hash32(it.bytes))
            hash64Set.add(KeyHash.hash(it.bytes))
        }
        then:
        hash32Set.size() == keyList.size()
        hash64Set.size() == keyList.size()

        when:
        def h0 = KeyHash.hashOffset(keyList[0].bytes, 0, keyList[0].length() - 10)
        def h00 = KeyHash.hash32Offset(keyList[0].bytes, 0, keyList[0].length() - 10)
        then:
        h0 != h00
    }

    def 'test split index'() {
        expect:
        KeyHash.splitIndex(0L, (byte) 1, 0) == 0
        KeyHash.splitIndex(1L, (byte) 3, 0) < 3
        KeyHash.splitIndex(10L, (byte) 3, 0) == 0
        KeyHash.splitIndex(11L, (byte) 3, 0) == 1
        KeyHash.splitIndex(12L, (byte) 3, 0) == 2

        KeyHash.splitIndex(13L, (byte) 3, 0) < 3
    }

    def 'test bucket index'() {
        given:
        def keyList = (0..<10).collect {
            UUID.randomUUID().toString()
        }

        for (key in keyList) {
            println 'key: ' + key + ', bucket index: ' + KeyHash.bucketIndex(KeyHash.hash(key.bytes))
        }

        expect:
        1 == 1

        when:
        def key = 'x'
        def keyHash = KeyHash.hash(key.bytes)
        then:
        keyHash != 0

        when:
        key = 'xxxx'
        keyHash = KeyHash.hash(key.bytes)
        then:
        keyHash != 0

        when:
        key = 'xhxx'
        keyHash = KeyHash.hash(key.bytes)
        then:
        keyHash != 0

        when:
        def inBucket0KeyList = (0..<10).collect {
            'xh!0_key:' + it.toString().padLeft(12, '0')
        }
        then:
        inBucket0KeyList.collect {
            KeyHash.bucketIndex(KeyHash.hash(it.bytes))
        }.unique().size() == 1
    }
}
