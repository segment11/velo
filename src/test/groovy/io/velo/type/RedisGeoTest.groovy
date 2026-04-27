package io.velo.type

import spock.lang.Specification

class RedisGeoTest extends Specification {
    private static String toGeohashString(long l) {
        def geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz".bytes
        def bytes = new byte[11]
        for (int i = 0; i < 11; i++) {
            int idx
            if (i == 10) {
                idx = 0
            } else {
                idx = (int) ((l >> (52 - ((i + 1) * 5))) & 0x1f)
            }
            bytes[i] = geoalphabet[idx]
        }
        return new String(bytes)
    }

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

    def 'test hash uses standard geohash latitude normalization'() {
        given:
        def p = new RedisGeo.P(15.087269, 37.502669)
        def expected = toGeohashString(RedisGeo.geohashEncode(-180, 180, -90, 90, p.lon, p.lat, (byte) 26))

        when:
        def hashResult = new String(RedisGeo.hash(p))

        then:
        hashResult == expected
    }

    def 'test hashAsStore keeps internal Redis geo bounds'() {
        given:
        def p = new RedisGeo.P(15.087269, 37.502669)
        def storeHash = RedisGeo.hashAsStore(p)
        def standardHash = RedisGeo.geohashEncode(-180, 180, -90, 90, p.lon, p.lat, (byte) 26)

        expect:
        storeHash != 0
        standardHash != 0
        storeHash != standardHash
    }

    def 'test add throws on invalid coordinates'() {
        given:
        def rg = new RedisGeo()

        when:
        rg.add('test', 200, 37.502669)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('longitude')

        when:
        rg.add('test', -200, 37.502669)

        then:
        e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('longitude')

        when:
        rg.add('test', 15, 100)

        then:
        e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('latitude')

        when:
        rg.add('test', 15, -100)

        then:
        e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('latitude')
    }

    def 'test add accepts valid coordinates'() {
        given:
        def rg = new RedisGeo()

        when:
        rg.add('valid1', 180, 85.05112877980659)
        rg.add('valid2', -180, -85.05112877980659)
        rg.add('valid3', 0, 0)

        then:
        noExceptionThrown()
        rg.size() == 3
    }

    def 'test encode throws when entry count exceeds Short.MAX_VALUE'() {
        given:
        def rg = new RedisGeo()
        def mapField = RedisGeo.class.getDeclaredField('map')
        mapField.setAccessible(true)
        def map = (Map) mapField.get(rg)
        (Short.MAX_VALUE + 1).times { i ->
            map.put('m_' + i, new RedisGeo.P(0, 0))
        }

        when:
        rg.encode()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds Short.MAX_VALUE')
    }

    def 'test encode throws when member length exceeds Short.MAX_VALUE'() {
        given:
        def rg = new RedisGeo()
        def bigMember = new String(new char[Short.MAX_VALUE + 1])
        rg.add(bigMember, 0, 0)

        when:
        rg.encode()

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds Short.MAX_VALUE')
    }

    def 'test decode throws on oversized member length'() {
        given:
        def rg = new RedisGeo()
        rg.add('member1', 13, 38)
        def encoded = rg.encode()
        def buffer = java.nio.ByteBuffer.wrap(encoded)
        buffer.putShort(RedisGeo.HEADER_LENGTH, (short) 10000)

        when:
        RedisGeo.decode(encoded, false)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('exceeds remaining buffer')
    }

    def 'test decode throws on invalid coordinates in payload'() {
        given:
        def rg = new RedisGeo()
        rg.add('member1', 13, 38)
        def encoded = rg.encode()
        def buffer = java.nio.ByteBuffer.wrap(encoded)
        def memberLenOffset = RedisGeo.HEADER_LENGTH
        def memberLen = buffer.getShort(memberLenOffset)
        def lonOffset = memberLenOffset + 2 + memberLen
        buffer.putDouble(lonOffset, 999.0)

        when:
        RedisGeo.decode(encoded, false)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('longitude')
    }

    def 'test decode throws on invalid latitude in payload'() {
        given:
        def rg = new RedisGeo()
        rg.add('member1', 13, 38)
        def encoded = rg.encode()
        def buffer = java.nio.ByteBuffer.wrap(encoded)
        def memberLenOffset = RedisGeo.HEADER_LENGTH
        def memberLen = buffer.getShort(memberLenOffset)
        def latOffset = memberLenOffset + 2 + memberLen + 8
        buffer.putDouble(latOffset, 999.0)

        when:
        RedisGeo.decode(encoded, false)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.toLowerCase().contains('latitude')
    }
}
