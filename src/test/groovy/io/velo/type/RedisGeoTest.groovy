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
        rg.add('a', 1.0d, 2.0d)
        rg.add('b', 1.0d, 2.0d)
        then:
        rg.get('a') != null
        rg.remove('b')

        when:
        rg.add('b', 1.0d, 2.0d)
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
}
