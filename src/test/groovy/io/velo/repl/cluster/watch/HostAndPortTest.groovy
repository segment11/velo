package io.velo.repl.cluster.watch

import spock.lang.Specification

class HostAndPortTest extends Specification {
    def 'test all'() {
        given:
        def h0 = new HostAndPort('localhost', 7379)
        def h00 = new HostAndPort('localhost', 7379)
        def h1 = new HostAndPort('localhost', 7380)

        expect:
        h0 == h00
        h0 != h1

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
}
