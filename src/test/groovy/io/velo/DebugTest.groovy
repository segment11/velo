package io.velo


import spock.lang.Specification

class DebugTest extends Specification {
    def 'test all'() {
        given:
        def d = Debug.instance
        d.logMerge = false

        expect:
        !d.logMerge
        !d.logTrainDict
        !d.logRestore
        !d.bulkLoad
    }
}
