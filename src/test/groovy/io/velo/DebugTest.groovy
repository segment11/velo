package io.velo

import spock.lang.Specification

class DebugTest extends Specification {
    def 'test all'() {
        given:
        def d = Debug.instance

        expect:
        !d.logCmd
        !d.logTrainDict
        !d.logRestore
        !d.bulkLoad
    }
}
