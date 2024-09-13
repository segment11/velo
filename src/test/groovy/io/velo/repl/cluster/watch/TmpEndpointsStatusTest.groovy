package io.velo.repl.cluster.watch

import spock.lang.Specification

class TmpEndpointsStatusTest extends Specification {
    def 'test all'() {
        given:
        def tmp = new TmpEndpointsStatus()
        tmp.slaveListenAddress = 'localhost:27380'
        tmp.oneEndpointStatusMapByClusterName = [:]

        expect:
        tmp.slaveListenAddress == 'localhost:27380'
        tmp.oneEndpointStatusMapByClusterName.size() == 0
    }
}
