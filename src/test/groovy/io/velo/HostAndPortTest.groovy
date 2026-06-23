package io.velo

import spock.lang.Specification

class HostAndPortTest extends Specification {
    def 'test equals and hashCode and HashMap behavior'() {
        given:
        def h0 = new HostAndPort('localhost', 7379)
        def h00 = new HostAndPort('localhost', 7379)
        def h1 = new HostAndPort('localhost', 7380)

        expect:
        h0 == h00
        h0 != h1
        h0.toString() == 'localhost:7379'

        when:
        HashMap<HostAndPort, String> map = new HashMap<HostAndPort, String>()
        map.put(h0, 'h0')
        map.put(h1, 'h1')
        then:
        map.size() == 2
        map.containsKey(h00)

        when:
        map.put(h00, 'h00')
        then:
        map.size() == 2
        map.get(h00) == 'h00'
    }

    def 'test parse'() {
        expect:
        HostAndPort.parse(null) == null
        HostAndPort.parse('localhost:7379') == new HostAndPort('localhost', 7379)
        HostAndPort.parse('127.0.0.1:6379').host == '127.0.0.1'
        HostAndPort.parse('127.0.0.1:6379').port == 6379

        when:
        HostAndPort.parse(':6379')
        then:
        thrown(IllegalArgumentException)

        when:
        HostAndPort.parse('localhost:0')
        then:
        thrown(IllegalArgumentException)

        when:
        HostAndPort.parse('localhost:65536')
        then:
        thrown(IllegalArgumentException)

        when:
        HostAndPort.parse('localhost:abc')
        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('Invalid host:port format')
    }
}
