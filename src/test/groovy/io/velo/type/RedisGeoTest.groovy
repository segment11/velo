package io.velo.type

import spock.lang.Specification

class RedisGeoTest extends Specification {
    def 'test member and encode'() {
        given:
        def rg = new RedisGeo()

        expect:
        rg.size() == 0
        rg.isEmpty()
        rg.map.isEmpty()
        !rg.contains('a')
        !rg.remove('b')

        when:
        rg.add('a', 13, 38)
        rg.add('b', 15, 37)
        then:
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

    def 'test distance'() {
        given:
        def p0 = new RedisGeo.P(13.361389, 38.115556)
        def p1 = new RedisGeo.P(15.087269, 37.502669)
        def d = RedisGeo.distance(p0, p1)
        println d

        expect:
        d > 0
        RedisGeo.isWithinBox(p0, p0.lon(), p0.lat(), 100, 100)
        RedisGeo.isWithinBox(p0, p1.lon(), p1.lat(), d * 2 + 100, d * 2 + 100)
    }

    def 'test unit'() {
        expect:
        RedisGeo.Unit.M.toMeters(1.0) == 1.0
        RedisGeo.Unit.UNKNOWN.toMeters(1.0) == 1.0
        RedisGeo.Unit.KM.toMeters(1.0) == 1000.0
        RedisGeo.Unit.MI.toMeters(1.0) == 1609.34
        RedisGeo.Unit.FT.toMeters(1.0) == 0.3048

        RedisGeo.Unit.fromString('m') == RedisGeo.Unit.M
        RedisGeo.Unit.fromString('km') == RedisGeo.Unit.KM
        RedisGeo.Unit.fromString('mi') == RedisGeo.Unit.MI
        RedisGeo.Unit.fromString('ft') == RedisGeo.Unit.FT
        RedisGeo.Unit.fromString('xx') == RedisGeo.Unit.UNKNOWN
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
