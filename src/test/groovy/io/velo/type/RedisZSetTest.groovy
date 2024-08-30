package io.velo.type


import spock.lang.Specification

class RedisZSetTest extends Specification {
    def 'zset'() {
        given:
        def rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')
        def memberA = rz.get('a')
        memberA.score(1)
        then:
        !rz.isEmpty()
        rz.size() == 3
        rz.getMemberMap().size() == 3
        rz.contains('a')
        memberA.initRank == 0
        !rz.remove('d')
        !rz.add(0.9, 'a', false, false)
        rz.add(1.8, 'b', true, true)
        rz.between(1, true, 2, true).collect { it.member() }.join(',') == 'a,b'
        rz.betweenByMember('a', true, 'b', true).collect { it.value.score() }.join(',') == '1.0,1.8'
        rz.betweenByMember('-', true, '+', true).collect { it.value.score() }.join(',') == '1.0,1.8,3.0'
        rz.between(Double.NEGATIVE_INFINITY, false, Double.POSITIVE_INFINITY, false).collect { it.member() }.join(',') == 'a,b,c'
        rz.between(0.9, true, 1.8, true).collect { it.member() }.join(',') == 'a,b'
        rz.between(1.0, false, 3.0, false).collect { it.member() }.join(',') == 'b'

        when:
        def rz2 = new RedisZSet()
        then:
        rz2.betweenByMember('-', true, '+', true).isEmpty()

        when:
        def isDeletedA = rz.remove('a')
        then:
        isDeletedA
        rz.size() == 2
        !rz.contains('a')

        when:
        rz.add(4, 'd')
        rz.add(5, 'e')
        rz.print()
        println rz.toString()
        then:
        rz.size() == 4
        rz.getSet().size() == 4
        rz.contains('d')
        rz.contains('e')
        rz.pollFirst().member() == 'b'
        rz.pollLast().member() == 'e'
        rz.pollLast().member() == 'd'
        rz.pollLast().member() == 'c'
        rz.pollFirst() == null
        rz.pollLast() == null

        when:
        rz.clear()
        then:
        rz.size() == 0
    }

    def 'encode'() {
        given:
        def rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')
        def encoded = rz.encode()
        encoded = rz.encodeButDoNotCompress()
        def rz2 = RedisZSet.decode(encoded, false)
        def rz3 = RedisZSet.decode(encoded)
        then:
        rz2.size() == 3
        rz3.size() == 3
        RedisZSet.getSizeWithoutDecode(encoded) == 3
        rz2.contains('a')
        rz2.contains('b')
        rz2.contains('c')
        rz2.get('a').score() == 1
        rz2.get('b').score() == 2
        rz2.get('c').score() == 3
    }

    def 'decode crc32 not match'() {
        given:
        def rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(3, 'c')
        def encoded = rz.encode()
        encoded[RedisZSet.HEADER_LENGTH - 4] = Byte.MAX_VALUE
        boolean exception = false
        try {
            RedisZSet.decode(encoded, true)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'max score member'() {
        given:
        def rz = new RedisZSet()

        when:
        rz.add(1, 'a')
        rz.add(2, 'b')
        rz.add(2, 'zz')
        rz.add(3, 'c')
        rz.add(3, 'zz')
        then:
        rz.pollLast().member() == 'zz'
    }

    def 'encode size 0'() {
        given:
        def rz = new RedisZSet()

        when:
        def encoded = rz.encode()
        def rz2 = RedisZSet.decode(encoded, false)
        then:
        rz2.size() == 0
    }

    def 'test compress'() {
        given:
        def rz = new RedisZSet()
        def longString = 'aaaaabbbbbccccc' * 10

        when:
        RedisHH.PREFER_COMPRESS_RATIO = 0.9
        10.times {
            rz.add(it, longString + it)
        }
        def encoded = rz.encode()
        def rz2 = RedisZSet.decode(encoded)
        then:
        rz2.size() == 10
        rz.set == rz2.set

        when:
        // compress ratio too big, ignore
        RedisHH.PREFER_COMPRESS_RATIO = 0.1
        def rz4 = new RedisZSet()
        5.times {
            rz4.add(it, UUID.randomUUID().toString())
        }
        def encoded4 = rz4.encode()
        then:
        // uuid length is 36
        encoded4.length == RedisZSet.HEADER_LENGTH + 5 * (2 + 8 + 36)
    }
}
