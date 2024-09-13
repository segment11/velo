package io.velo.repl.cluster.watch

import spock.lang.Specification

class OneClusterMetaTest extends Specification {
    def 'test all'(){
        given:
        def oneClusterMeta = new OneClusterMeta()
        def hostAndPort = new HostAndPort('localhost', 7379)
        oneClusterMeta.masterHostAndPortList = [hostAndPort]

        expect:
        oneClusterMeta.masterHostAndPortList.size() == 1
        oneClusterMeta.masterHostAndPortList[0] == hostAndPort
    }
}
