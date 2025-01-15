package io.velo.type

import spock.lang.Specification

class RedisGeoTest extends Specification {
    def 'test all'() {
        given:
        def rg = new RedisGeo()

        expect:
        rg.size() == 0
        rg.isEmpty()
        !rg.contains('a')
        !rg.remove('b')

        when:
        rg.add('a', 13, 38)
        rg.add('b', 15, 37)
        then:
        RedisGeo.distance(rg.get('a'), rg.get('b')) > 0
        rg.get('a') != null
        rg.remove('b')

        when:
        rg.add('b', 15, 37)
        def encoded = rg.encode()
        def rg2 = RedisGeo.decode(encoded)
        def rg3 = RedisGeo.decode(encoded, false)
        then:
        rg2.size() == 2
        rg3.size() == 2

        when:
        encoded[RedisGeo.HEADER_LENGTH - 4] = 0
        boolean exception = false
        try {
            RedisGeo.decode(encoded)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test hash'() {
        given:
        def hash = RedisGeo.hash(new RedisGeo.P(15.087269, 37.502669))
        println new String(hash)

        expect:
        RedisGeo.geohashEncode(0, 0, 0, 0, -181, 0, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 0, 0, 0, 181, 0, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 0, 0, 0, 0, -90, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 0, 0, 0, 0, 90, (byte) 26) == 0

        RedisGeo.geohashEncode(11, 0, 0, 0, 10, 0, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 9, 0, 0, 10, 0, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 0, 11, 0, 0, 10, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 0, 0, 9, 0, 10, (byte) 26) == 0
    }
}
