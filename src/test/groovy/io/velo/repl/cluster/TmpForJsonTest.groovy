package io.velo.repl.cluster

import spock.lang.Specification

class TmpForJsonTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        def x = new TmpForJson()
        x.list = []
        x.nodes = []

        expect:
        x.list.size() == 0
        x.nodes.size() == 0
    }
}
