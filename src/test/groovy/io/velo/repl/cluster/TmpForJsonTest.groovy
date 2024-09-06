package io.velo.repl.cluster

import spock.lang.Specification

class TmpForJsonTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        def x = new TmpForJson()
        x.shards = []

        expect:
        x.shards.size() == 0
    }
}
