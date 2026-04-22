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
        given:
        def M = RedisGeo.Unit.M
        def KM = RedisGeo.Unit.KM
        def MI = RedisGeo.Unit.MI
        def FT = RedisGeo.Unit.FT

        expect:
        M.toMeters(1.0d) == 1.0d
        KM.toMeters(1.0d) == 1000.0d
        MI.toMeters(1.0d) == 1609.34d
        FT.toMeters(1.0d) == 0.3048d

        KM.fromMeters(1000.0d) == 1.0d
        MI.fromMeters(1609.34d) == 1.0d
        FT.fromMeters(0.3048d) == 1.0d
        M.fromMeters(1.0d) == 1.0d

        // round-trip: fromMeters(toMeters(x)) == x
        KM.fromMeters(KM.toMeters(1.0d)) == 1.0d
        MI.fromMeters(MI.toMeters(1.0d)) == 1.0d
        FT.fromMeters(FT.toMeters(1.0d)) == 1.0d

        RedisGeo.Unit.fromString('m') == M
        RedisGeo.Unit.fromString('km') == KM
        RedisGeo.Unit.fromString('mi') == MI
        RedisGeo.Unit.fromString('ft') == FT
        RedisGeo.Unit.fromString('xx') == RedisGeo.Unit.UNKNOWN
    }

    def 'test hash'() {
        given:
        def p = new RedisGeo.P(15.087269, 37.502669)
        def hash = RedisGeo.hash(p)
        def hashAsStore = RedisGeo.hashAsStore(p)
        println 'geo hash: ' + new String(hash)
        println 'geo hash as score: ' + hashAsStore

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

    def 'test geohashEncode handles degenerate bounding box'() {
        expect:
        RedisGeo.geohashEncode(0, 0, 0, 10, 0, 5, (byte) 26) == 0
        RedisGeo.geohashEncode(0, 10, 0, 0, 5, 0, (byte) 26) == 0
    }

    def 'test hash and hashAsStore use consistent latitude bounds'() {
        given:
        def p = new RedisGeo.P(15.087269, 37.502669)

        when:
        def hashResult = RedisGeo.hash(p)
        def hashAsStoreResult = RedisGeo.hashAsStore(p)

        then:
        hashResult.length == 11
        hashAsStoreResult != 0
    }

    def 'test hash uses Redis-compatible latitude bounds'() {
        given:
        def pBeyondRedis = new RedisGeo.P(15.087269, 86.0)

        when:
        def hashAsStoreBeyond = RedisGeo.hashAsStore(pBeyondRedis)

        then:
        hashAsStoreBeyond == 0

        when:
        def hashBeyond = RedisGeo.hash(pBeyondRedis)

        then:
        new String(hashBeyond) == "00000000000"
    }
}
