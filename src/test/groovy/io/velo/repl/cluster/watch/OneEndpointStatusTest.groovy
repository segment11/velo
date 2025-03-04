package io.velo.repl.cluster.watch

import spock.lang.Specification

class OneEndpointStatusTest extends Specification {
    def 'test all'() {
        given:
        def status = new OneEndpointStatus()
        println status

        expect:
        status.isPingOk()

        when:
        status.addStatus(OneEndpointStatus.Status.PING_FAIL)
        println status
        then:
        status.isPingOk()

        when:
        10.times {
            status.addStatus(OneEndpointStatus.Status.PING_FAIL)
        }
        println status
        then:
        !status.isPingOk()
    }
}
